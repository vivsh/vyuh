use std::any::TypeId;

use crate::{
    Error, Site,
    callables::{self, Callable},
};

use super::args::{self, CommandArg};
use super::error::CommandError;

// ── public config ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize)]
pub struct CommandConf {
    pub name: Option<String>,
    pub description: Option<String>,
}

impl CommandConf {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            description: None,
        }
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

// ── handler alias ─────────────────────────────────────────────────────────────

pub type CommandHandlerIn = Callable<CommandContext, Error>;

// ── context ───────────────────────────────────────────────────────────────────

pub struct CommandContext {
    pub(super) site: Site,
    pub(super) payload: callables::DataBox,
    pub(super) operation_id: crate::OperationId,
}

impl CommandContext {
    pub(super) fn new(
        site: Site,
        payload: callables::DataBox,
        operation_id: crate::OperationId,
    ) -> Self {
        Self {
            site,
            payload,
            operation_id,
        }
    }
}

impl callables::IntoDataBox for CommandContext {
    fn into_data_box(self) -> callables::DataBox {
        self.payload
    }
}

impl callables::HasSite for CommandContext {
    fn site(&self) -> &Site {
        &self.site
    }
}

impl callables::FromContextParts<CommandContext> for crate::OperationId {
    fn from_context_parts(context: &CommandContext) -> Result<Self, callables::CallError> {
        Ok(context.operation_id)
    }
}

// ── command ───────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct Command {
    pub(crate) handler: CommandHandlerIn,
    pub(crate) options: CommandConf,
    pub(crate) args: Vec<CommandArg>,
    pub(crate) parser: fn(&str, &[&str], &[CommandArg]) -> Result<callables::DataBox, CommandError>,
    operation: callables::Operation,
}

impl Command {
    pub(crate) fn operation(&self) -> callables::Operation {
        self.operation.clone().with_conf(&self.options)
    }
}

// ── factory ───────────────────────────────────────────────────────────────────

pub(crate) fn command<T, H, Args>(handler: H, options: CommandConf) -> Result<Command, CommandError>
where
    T: callables::DataValue,
    H: callables::Specable<Args, Output = Result<(), Error>> + Send + Sync + 'static,
    Args: callables::FromContext<CommandContext>
        + callables::IntoArgSpecs
        + callables::HasData<T>
        + Send
        + 'static,
{
    let mut callable: Callable<CommandContext, Error> = Callable::new(handler);
    let schema = command_schema_from_args::<Args>()?;
    let parsed_args = args::parse_schema_to_args(&schema)?;
    let parser: fn(&str, &[&str], &[CommandArg]) -> Result<callables::DataBox, CommandError> =
        |command_name, cli, arg_defs| {
            let obj: T = args::parse_args(command_name, cli, arg_defs)?;
            Ok(callables::DataBox::new(obj))
        };
    callable.type_id = TypeId::of::<T>();
    let operation =
        callables::Operation::from_specs(callables::OperationKind::Command, callable.inspect());
    Ok(Command {
        handler: callable,
        options,
        args: parsed_args,
        parser,
        operation,
    })
}

fn command_schema_from_args<Args>() -> Result<schemars::Schema, CommandError>
where
    Args: callables::IntoArgSpecs,
{
    let specs = Args::into_arg_specs();
    let Some(schema) = specs.iter().rev().find_map(|spec| spec.part.body_schema()) else {
        return Err(CommandError::UnsupportedSchema(
            "command handlers must contain Data<T> or Valid<Data<T>>".into(),
        ));
    };

    let mut settings = schemars::generate::SchemaSettings::default();
    settings.inline_subschemas = true;
    let mut generator = schemars::SchemaGenerator::new(settings);
    Ok(schema.schema(&mut generator))
}
