use indexmap::IndexMap;
use serde::Serialize;

use crate::{Site, errors::ErrorSource};

use super::args::CommandArgType;
use super::error::CommandError;
use super::types::{Command, CommandContext};

#[derive(Debug, Clone, Serialize)]
pub struct CommandInfo {
    pub name: String,
    pub summary: Option<String>,
    pub args: Vec<CommandArgInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CommandArgInfo {
    pub name: String,
    pub type_name: &'static str,
    pub required: bool,
    pub description: Option<String>,
    pub hints: Vec<String>,
}

/// Registry of named CLI commands.
#[derive(Clone)]
pub struct CommandRegistry {
    banner: Option<String>,
    commands: IndexMap<String, Command>,
}

impl CommandRegistry {
    pub fn new() -> Self {
        Self {
            banner: None,
            commands: IndexMap::new(),
        }
    }

    #[allow(dead_code)]
    pub fn with_banner(mut self, banner: String) -> Self {
        self.banner = Some(banner);
        self
    }

    pub fn merge(&mut self, other: CommandRegistry) -> Result<(), CommandError> {
        for (name, cmd) in other.commands {
            if self.commands.contains_key(&name) {
                return Err(CommandError::AlreadyExists(name));
            }
            self.commands.insert(name, cmd);
        }
        Ok(())
    }

    pub(crate) fn register(&mut self, command: Command) -> Result<(), CommandError> {
        let name = command
            .options
            .name
            .as_deref()
            .unwrap_or_else(|| command.handler.inspect().name.as_str())
            .to_string();
        if name == "help" || self.commands.contains_key(&name) {
            return Err(CommandError::AlreadyExists(name));
        }
        self.commands.insert(name, command);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn iter_commands(&self) -> impl Iterator<Item = &Command> {
        self.commands.values()
    }

    pub(crate) fn operations(&self) -> impl Iterator<Item = crate::Operation> + '_ {
        self.commands.values().map(Command::operation)
    }

    pub fn generate_help(&self, command_name: &str) -> Result<String, CommandError> {
        if command_name == "help" {
            return Ok(help_command_help());
        }
        let command = self
            .commands
            .get(command_name)
            .ok_or_else(|| CommandError::NotFound(command_name.to_string()))?;

        let mut help = format!("Usage: {} [OPTIONS]\n", command_name);
        if let Some(description) = command_summary(command) {
            help.push_str(&format!("\n{}\n", description));
        }
        if command.args.is_empty() {
            help.push_str("\nNo arguments.\n");
            return Ok(help);
        }
        help.push_str("\nOptions:\n");
        let mut args = command.args.iter().collect::<Vec<_>>();
        args.sort_by(|a, b| a.flag_name().cmp(b.flag_name()));
        let width = args
            .iter()
            .map(|arg| option_usage(arg).len())
            .max()
            .unwrap_or(0);
        for arg in args {
            let required_str = if arg.required { " (required)" } else { "" };
            let desc_str = arg
                .description
                .as_deref()
                .map(|d| format!(" - {}", d))
                .unwrap_or_default();
            let hints_str = if arg.hints.is_empty() {
                String::new()
            } else {
                format!(" [{}]", arg.hints.join("; "))
            };
            let usage = option_usage(arg);
            let line = format!(
                "  {usage:<width$}{}{}{}\n",
                required_str,
                hints_str,
                desc_str,
                width = width
            );
            help.push_str(&line);
        }
        Ok(help)
    }

    pub(crate) fn infos(&self) -> Vec<CommandInfo> {
        let mut commands = self.commands.iter().collect::<Vec<_>>();
        commands.sort_by(|(left, _), (right, _)| left.cmp(right));
        commands
            .into_iter()
            .map(|(name, command)| CommandInfo {
                name: name.clone(),
                summary: command_summary(command),
                args: command
                    .args
                    .iter()
                    .map(|arg| CommandArgInfo {
                        name: arg.name.clone(),
                        type_name: arg.arg_type.type_name(),
                        required: arg.required,
                        description: arg.description.clone(),
                        hints: arg.hints.clone(),
                    })
                    .collect(),
            })
            .collect()
    }

    pub(crate) fn execute_help(&self) -> String {
        let mut help = String::new();
        if let Some(banner) = &self.banner {
            help.push_str(banner);
            help.push_str("\n\n");
        }
        help.push_str("Available commands:\n\n");
        let mut entries = self
            .commands
            .iter()
            .map(|(name, cmd)| {
                (
                    name.as_str(),
                    command_summary(cmd).unwrap_or_else(|| "No description available".to_string()),
                )
            })
            .collect::<Vec<_>>();
        entries.push((
            "help",
            "Show available commands or help for one command.".to_string(),
        ));
        entries.sort_by(|(left, _), (right, _)| left.cmp(right));
        let width = entries
            .iter()
            .map(|(name, _)| name.len())
            .max()
            .unwrap_or(0);
        for (name, summary) in entries {
            help.push_str(&format!("  {:<width$} {}\n", name, summary, width = width));
        }
        help.push_str("\nUse '<command> --help' for more information on a specific command.\n");
        help
    }

    pub(crate) fn early_output(
        &self,
        command_name: &str,
        args: &[&str],
    ) -> Option<Result<String, CommandError>> {
        if command_name == "help" {
            return Some(self.help_output(args));
        }
        if has_help_flag(args) {
            return Some(self.generate_help(command_name));
        }
        if !self.commands.contains_key(command_name) {
            return Some(Err(CommandError::UnknownCommand(command_name.to_string())));
        }
        None
    }

    pub async fn execute(
        &self,
        command_name: &str,
        args: &[&str],
        site: Site,
    ) -> Result<(), CommandError> {
        if command_name == "help" {
            println!("{}", self.help_output(args)?);
            return Ok(());
        }
        if has_help_flag(args) {
            println!("{}", self.generate_help(command_name)?);
            return Ok(());
        }
        let command = self
            .commands
            .get(command_name)
            .ok_or_else(|| CommandError::UnknownCommand(command_name.to_string()))?;
        let payload = (command.parser)(command_name, args, &command.args)?;
        let ctx = CommandContext::new(site, payload, command.operation().id);
        command
            .handler
            .call(ctx)
            .await
            .map(|_| ())
            .map_err(|err| match err.source.as_deref() {
                Some(ErrorSource::Validation(report)) => CommandError::Validation(report.clone()),
                _ => CommandError::Handler(err),
            })
    }
}

pub(crate) fn builtin_registry() -> Result<CommandRegistry, CommandError> {
    super::core::core_registry()
}

fn command_summary(command: &Command) -> Option<String> {
    command.options.description.clone().or_else(|| {
        command
            .handler
            .inspect()
            .description
            .as_ref()
            .and_then(|d| d.lines().next().map(|s| s.to_string()))
    })
}

fn option_usage(arg: &super::args::CommandArg) -> String {
    if matches!(arg.arg_type, CommandArgType::Boolean) {
        format!("--{} / --no-{}", arg.flag_name(), arg.flag_name())
    } else {
        let value = if matches!(arg.arg_type, CommandArgType::Array(_)) {
            format!("<{}>...", arg.arg_type.type_name())
        } else {
            format!("<{}>", arg.arg_type.type_name())
        };
        format!("--{} {}", arg.flag_name(), value)
    }
}

fn has_help_flag(args: &[&str]) -> bool {
    args.iter().any(|arg| *arg == "--help" || *arg == "-h")
}

fn help_command_help() -> String {
    "Usage: help [COMMAND]\n\nShow available commands or help for one command.\n".to_string()
}

impl CommandRegistry {
    fn help_output(&self, args: &[&str]) -> Result<String, CommandError> {
        match args {
            [] => Ok(self.execute_help()),
            [command] => self.generate_help(command),
            [_, extra, ..] => Err(CommandError::UnexpectedArgument {
                command: "help".to_string(),
                argument: (*extra).to_string(),
            }),
        }
    }
}
