use crate::{
    Data, Error, Site, SiteConf, Valid, Validate,
    errors::{ErrorCommandContext, ErrorConf},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::args::{parse_args, parse_schema_to_args};
use super::error::CommandError;
use super::registry::CommandRegistry;
use super::types::{CommandConf, command};

async fn make_site() -> Site {
    let conf = SiteConf::default().log_init(false);
    let bundle = crate::bundles::bundle([]);
    Site::build(conf, bundle).await.unwrap()
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
struct TestArgs {
    name: String,
    age: i32,
    #[serde(default)]
    verbose: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
struct ParserArgs {
    name: String,
    age: i32,
    #[serde(default)]
    verbose: bool,
    #[serde(default)]
    dry_run: bool,
    tags: Vec<String>,
}

fn command_arg_defs<T: JsonSchema>() -> Vec<super::args::CommandArg> {
    let mut settings = schemars::generate::SchemaSettings::default();
    settings.inline_subschemas = true;
    let mut generator = schemars::SchemaGenerator::new(settings);
    let schema = generator.subschema_for::<T>();
    parse_schema_to_args(&schema).unwrap()
}

fn test_command() -> super::Command {
    async fn handler(_args: Data<TestArgs>) -> Result<(), Error> {
        Ok(())
    }

    command::<TestArgs, _, _>(handler, CommandConf::new("test")).unwrap()
}

#[tokio::test]
async fn test_execute_command() {
    async fn handler(args: Data<TestArgs>) -> Result<(), Error> {
        assert_eq!(args.name, "Alice");
        assert_eq!(args.age, 30);
        assert!(args.verbose);
        Ok(())
    }

    let mut registry = CommandRegistry::new();
    let cmd = command::<TestArgs, _, _>(handler, CommandConf::new("test")).unwrap();
    registry.register(cmd).unwrap();

    let site = make_site().await;
    let result = registry
        .execute(
            "test",
            &["--name", "Alice", "--age", "30", "--verbose"],
            site,
        )
        .await;
    assert!(result.is_ok());
}

/// Verifies a command handler receives the ID retained by its operation metadata.
#[tokio::test]
async fn command_extracts_operation_id() -> Result<(), String> {
    let seen = std::sync::Arc::new(parking_lot::Mutex::new(None));
    let captured = std::sync::Arc::clone(&seen);
    let handler = move |id: crate::OperationId, _args: Data<TestArgs>| {
        let captured = std::sync::Arc::clone(&captured);
        async move {
            *captured.lock() = Some(id);
            Ok::<(), Error>(())
        }
    };
    let command = command::<TestArgs, _, _>(handler, CommandConf::new("operation"))
        .map_err(|error| error.to_string())?;
    let expected = command.operation().id;
    let mut registry = CommandRegistry::new();
    registry
        .register(command)
        .map_err(|error| error.to_string())?;
    registry
        .execute(
            "operation",
            &["--name", "A", "--age", "1"],
            make_site().await,
        )
        .await
        .map_err(|error| error.to_string())?;
    assert_eq!(*seen.lock(), Some(expected));
    Ok(())
}

#[test]
fn test_parse_arrays_booleans_and_scalars() {
    let args = command_arg_defs::<ParserArgs>();
    let parsed = parse_args::<ParserArgs>(
        "parse",
        &[
            "--name",
            "Alice",
            "--age",
            "30",
            "--tags",
            "api",
            "web",
            "--verbose",
            "false",
            "--tags",
            "admin",
        ],
        &args,
    )
    .unwrap();

    assert_eq!(
        parsed,
        ParserArgs {
            name: "Alice".to_string(),
            age: 30,
            verbose: false,
            dry_run: false,
            tags: vec!["api".to_string(), "web".to_string(), "admin".to_string()],
        }
    );
}

#[test]
fn test_parse_equals_negative_and_kebab_aliases() {
    let args = command_arg_defs::<ParserArgs>();
    let parsed = parse_args::<ParserArgs>(
        "parse",
        &[
            "--name=Alice",
            "--age=-1",
            "--tags=api",
            "--tags",
            "web",
            "--dry-run",
        ],
        &args,
    )
    .unwrap();

    assert_eq!(
        parsed,
        ParserArgs {
            name: "Alice".to_string(),
            age: -1,
            verbose: false,
            dry_run: true,
            tags: vec!["api".to_string(), "web".to_string()],
        }
    );

    let parsed = parse_args::<ParserArgs>(
        "parse",
        &[
            "--name",
            "Alice",
            "--age",
            "30",
            "--tags",
            "api",
            "--dry_run",
        ],
        &args,
    )
    .unwrap();
    assert!(parsed.dry_run);
}

#[test]
fn test_parse_no_bool_flag() {
    let args = command_arg_defs::<ParserArgs>();
    let parsed = parse_args::<ParserArgs>(
        "parse",
        &[
            "--name",
            "Alice",
            "--age",
            "30",
            "--tags",
            "api",
            "--no-verbose",
        ],
        &args,
    )
    .unwrap();

    assert!(!parsed.verbose);
}

#[test]
fn test_duplicate_scalar_and_bool_flags_error() {
    let args = command_arg_defs::<ParserArgs>();

    let err = parse_args::<ParserArgs>(
        "parse",
        &[
            "--name", "Alice", "--name", "Ada", "--age", "30", "--tags", "api",
        ],
        &args,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        CommandError::DuplicateFlag { command, flag }
            if command == "parse" && flag == "name"
    ));

    let err = parse_args::<ParserArgs>(
        "parse",
        &[
            "--name",
            "Alice",
            "--age",
            "30",
            "--tags",
            "api",
            "--verbose",
            "--no-verbose",
        ],
        &args,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        CommandError::DuplicateFlag { command, flag }
            if command == "parse" && flag == "verbose"
    ));
}

#[test]
fn test_parse_errors_are_specific() {
    let args = command_arg_defs::<ParserArgs>();

    let err = parse_args::<ParserArgs>("parse", &["--unknown"], &args).unwrap_err();
    assert!(matches!(
        err,
        CommandError::UnknownFlag {
            command,
            flag
        } if command == "parse" && flag == "unknown"
    ));

    let err = parse_args::<ParserArgs>("parse", &["--name", "Alice", "--tags", "api"], &args)
        .unwrap_err();
    assert!(err.to_string().contains("Missing required argument: --age"));

    let err = parse_args::<ParserArgs>(
        "parse",
        &["--name", "Alice", "--age", "not-int", "--tags", "api"],
        &args,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        CommandError::ParseError {
            flag,
            expected_type,
            ..
        } if flag == "age" && expected_type == "integer"
    ));
}

#[tokio::test]
async fn test_duplicate_and_reserved_command_names_fail() {
    let mut registry = CommandRegistry::new();
    registry.register(test_command()).unwrap();

    let duplicate = registry.register(test_command()).unwrap_err();
    assert!(matches!(duplicate, CommandError::AlreadyExists(name) if name == "test"));

    async fn help_handler(_args: Data<TestArgs>) -> Result<(), Error> {
        Ok(())
    }

    let reserved = command::<TestArgs, _, _>(help_handler, CommandConf::new("help")).unwrap();
    let err = CommandRegistry::new().register(reserved).unwrap_err();
    assert!(matches!(err, CommandError::AlreadyExists(name) if name == "help"));
}

#[tokio::test]
async fn test_unknown_command_mentions_help() {
    let registry = CommandRegistry::new();
    let site = make_site().await;
    let err = registry.execute("missing", &[], site).await.unwrap_err();
    assert!(matches!(err, CommandError::UnknownCommand(ref name) if name == "missing"));
    assert!(err.to_string().contains("help"));
}

#[test]
fn test_help_is_sorted_and_uses_descriptions() {
    async fn alpha(_args: Data<TestArgs>) -> Result<(), Error> {
        Ok(())
    }

    async fn beta(_args: Data<TestArgs>) -> Result<(), Error> {
        Ok(())
    }

    let mut registry = CommandRegistry::new();
    registry
        .register(
            command::<TestArgs, _, _>(beta, CommandConf::new("beta").description("Beta command."))
                .unwrap(),
        )
        .unwrap();
    registry
        .register(
            command::<TestArgs, _, _>(
                alpha,
                CommandConf::new("alpha").description("Alpha command."),
            )
            .unwrap(),
        )
        .unwrap();

    let help = registry.execute_help();
    assert!(help.find("alpha").unwrap() < help.find("beta").unwrap());
    assert!(help.contains("Alpha command."));
    assert!(help.contains("Beta command."));

    let command_help = registry.generate_help("alpha").unwrap();
    assert!(command_help.find("--age").unwrap() < command_help.find("--name").unwrap());
    assert!(command_help.contains("Alpha command."));
}

#[test]
fn test_help_lists_pseudo_help_and_command_help_forms() {
    async fn greet(_args: Data<TestArgs>) -> Result<(), Error> {
        Ok(())
    }

    let mut registry = CommandRegistry::new();
    registry
        .register(
            command::<TestArgs, _, _>(
                greet,
                CommandConf::new("greet").description("Print a greeting."),
            )
            .unwrap(),
        )
        .unwrap();

    let help = registry.execute_help();
    assert!(help.contains("help"));
    assert!(help.contains("greet"));

    let direct = registry.generate_help("greet").unwrap();
    let via_help = registry.early_output("help", &["greet"]).unwrap().unwrap();
    let via_flag = registry.early_output("greet", &["-h"]).unwrap().unwrap();
    assert_eq!(direct, via_help);
    assert_eq!(direct, via_flag);
}

#[test]
fn test_no_arg_command_help_says_no_arguments() {
    let registry = super::registry::builtin_registry().unwrap();
    let help = registry.generate_help("serve").unwrap();
    assert!(help.contains("No arguments."));
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
struct ValidatedArgs {
    #[validate(email)]
    email: String,
    #[validate(min_length = 3)]
    name: String,
}

#[tokio::test]
async fn test_valid_data_validates_command_args() {
    async fn handler(Valid(Data(args)): Valid<Data<ValidatedArgs>>) -> Result<(), Error> {
        assert_eq!(args.email, "person@example.com");
        Ok(())
    }

    let mut registry = CommandRegistry::new();
    registry
        .register(command::<ValidatedArgs, _, _>(handler, CommandConf::new("create-user")).unwrap())
        .unwrap();

    let site = make_site().await;
    registry
        .execute(
            "create-user",
            &["--email", "person@example.com", "--name", "Ada"],
            site.clone(),
        )
        .await
        .unwrap();

    let err = registry
        .execute(
            "create-user",
            &["--email", "not-email", "--name", "Al"],
            site,
        )
        .await
        .unwrap_err();

    let CommandError::Validation(_) = err else {
        panic!("expected validation error, got {err:?}");
    };
    let rendered = ErrorConf::default().render_command(
        ErrorCommandContext {
            command: "create-user".to_string(),
            args: vec![],
        },
        err.to_view(),
    );
    assert!(rendered.contains("Validation failed for command 'create-user'"));
    assert!(rendered.contains("--email"));
    assert!(rendered.contains("--name"));

    let help = registry.generate_help("create-user").unwrap();
    assert!(help.contains("min length: 3"));
}

#[test]
fn test_custom_command_error_renderer_receives_error_view() {
    let err = CommandError::MissingRequired {
        flag: "name".to_string(),
    };
    let rendered = ErrorConf::default()
        .command(|ctx, view| {
            format!(
                "{}:{}:{}",
                ctx.command,
                view.code,
                view.message.contains("--name")
            )
        })
        .render_command(
            ErrorCommandContext {
                command: "greet".to_string(),
                args: vec![],
            },
            err.to_view(),
        );

    assert_eq!(rendered, "greet:command_parse_error:true");
}
