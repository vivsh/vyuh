use std::io::{self, BufRead, IsTerminal, Write};

use thiserror::Error;

use crate::db::engine::{Answer, Clarification, Decision, OptionAction, clarification_message};

/// Failures while a terminal host gathers Gaman migration decisions.
#[derive(Debug, Error)]
pub(crate) enum MigrationPromptError {
    /// Standard input closed before every required decision was entered.
    #[error("migration clarification input ended before a choice was provided")]
    InputClosed,

    /// Terminal input or output could not be read or written.
    #[error("migration clarification input failed: {0}")]
    Io(#[from] io::Error),

    /// The task responsible for terminal input could not complete.
    #[error("migration clarification prompt could not complete: {0}")]
    Join(#[from] tokio::task::JoinError),
}

/// Reports whether a command may read clarification input from this process.
pub(crate) fn can_prompt(non_interactive: bool) -> bool {
    !non_interactive && io::stdin().is_terminal()
}

/// Collects Gaman decisions without blocking Vyuh's asynchronous command executor.
pub(crate) async fn collect_decisions(
    clarifications: Vec<Clarification>,
) -> Result<Vec<Decision>, MigrationPromptError> {
    tokio::task::spawn_blocking(move || {
        let stdin = io::stdin();
        let stdout = io::stdout();
        prompt_all(&clarifications, &mut stdin.lock(), &mut stdout.lock())
    })
    .await?
}

/// Formats Mool's canonical prompt for commands that cannot request terminal input.
pub(crate) fn render_clarification(clarification: &Clarification) -> String {
    let message = clarification_message(clarification);
    let mut output = message.description;
    for (index, option) in message.options.iter().enumerate() {
        output.push_str(&format!("\n    {}. {}", index + 1, option.label));
    }
    output
}

fn prompt_all(
    clarifications: &[Clarification],
    input: &mut impl BufRead,
    output: &mut impl Write,
) -> Result<Vec<Decision>, MigrationPromptError> {
    let mut decisions = Vec::with_capacity(clarifications.len());
    for clarification in clarifications {
        decisions.push(prompt_one(clarification, input, output)?);
    }
    Ok(decisions)
}

fn prompt_one(
    clarification: &Clarification,
    input: &mut impl BufRead,
    output: &mut impl Write,
) -> Result<Decision, MigrationPromptError> {
    let message = clarification_message(clarification);
    writeln!(output, "\n{}", message.description)?;
    for (index, option) in message.options.iter().enumerate() {
        writeln!(output, "  {}. {}", index + 1, option.label)?;
    }
    let answer = read_answer(&message.options, input, output)?;
    Ok(Decision {
        clarification_id: clarification.id.clone(),
        answer,
    })
}

fn read_answer(
    options: &[crate::db::engine::ClarificationOption],
    input: &mut impl BufRead,
    output: &mut impl Write,
) -> Result<Answer, MigrationPromptError> {
    loop {
        write!(output, "Choose an option: ")?;
        output.flush()?;
        let Some(choice) = read_line(input)? else {
            return Err(MigrationPromptError::InputClosed);
        };
        let Some(option) = selected_option(options, choice.trim()) else {
            writeln!(output, "Choose a number between 1 and {}.", options.len())?;
            continue;
        };
        return read_option(option, input, output);
    }
}

fn selected_option<'a>(
    options: &'a [crate::db::engine::ClarificationOption],
    choice: &str,
) -> Option<&'a crate::db::engine::ClarificationOption> {
    let index = choice.parse::<usize>().ok()?.checked_sub(1)?;
    options.get(index)
}

fn read_option(
    option: &crate::db::engine::ClarificationOption,
    input: &mut impl BufRead,
    output: &mut impl Write,
) -> Result<Answer, MigrationPromptError> {
    match &option.action {
        OptionAction::Fixed(answer) => Ok(answer.clone()),
        OptionAction::RequiresInput {
            prompt,
            make_answer,
        } => {
            write!(output, "  {prompt} ")?;
            output.flush()?;
            let Some(value) = read_line(input)? else {
                return Err(MigrationPromptError::InputClosed);
            };
            Ok(make_answer(value.trim().to_string()))
        }
    }
}

fn read_line(input: &mut impl BufRead) -> Result<Option<String>, io::Error> {
    let mut line = String::new();
    let bytes = input.read_line(&mut line)?;
    Ok((bytes != 0).then_some(line))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::db::engine::{ClarificationKind, Severity};

    fn rename_column() -> Clarification {
        Clarification {
            id: "rename_column:users:email".to_string(),
            severity: Severity::Suggestion,
            kind: ClarificationKind::RenameColumn {
                table: "users".to_string(),
                old: "email".to_string(),
                candidates: vec!["email_address".to_string()],
            },
        }
    }

    /// Verifies the terminal host converts a selected Gaman option into its fixed answer.
    #[test]
    fn prompt_selects_fixed_answer() {
        let mut input = Cursor::new("1\n");
        let mut output = Vec::new();
        let result = prompt_one(&rename_column(), &mut input, &mut output);

        assert!(matches!(
            result,
            Ok(Decision {
                answer: Answer::RenameTo(ref value),
                ..
            }) if value == "email_address"
        ));
        let text = String::from_utf8(output);
        assert!(matches!(text, Ok(value) if value.contains("email_address")));
    }

    /// Verifies typed Gaman options retain the supplied value in the generated decision.
    #[test]
    fn prompt_reads_typed_answer() {
        let clarification = Clarification {
            id: "not_null:users:age".to_string(),
            severity: Severity::Warning,
            kind: ClarificationKind::NotNullAdd {
                table: "users".to_string(),
                column: "age".to_string(),
                col_type: "integer".to_string(),
            },
        };
        let mut input = Cursor::new("1\n0\n");
        let mut output = Vec::new();
        let result = prompt_one(&clarification, &mut input, &mut output);

        assert!(matches!(
            result,
            Ok(Decision {
                answer: Answer::NotNullDefault(ref value),
                ..
            }) if value == "0"
        ));
    }

    /// Verifies closed standard input fails without manufacturing a migration decision.
    #[test]
    fn prompt_rejects_closed_input() {
        let mut input = Cursor::new("");
        let mut output = Vec::new();
        let result = prompt_one(&rename_column(), &mut input, &mut output);

        assert!(matches!(result, Err(MigrationPromptError::InputClosed)));
    }
}
