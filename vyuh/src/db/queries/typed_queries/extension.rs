//! Typed extension hooks for database functions and expressions.

use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::db::placeholders::Dialect;

use super::super::QueryError;
use super::dialect;
use super::expr::{Expr, ExprNode};
use super::handles::{Column, Var};

/// Dialect-aware typed SQL function.
///
/// Implement this for reusable functions such as `unaccent`, JSON helpers, or
/// project-specific database functions. The renderer validates the function
/// for the active dialect before writing SQL.
pub trait DbFunction<T>: Clone + Send + Sync + 'static {
    /// Returns the SQL function name for `dialect`.
    fn name(&self, dialect: Dialect) -> Result<Cow<'static, str>, QueryError>;

    /// Validates dialect support and argument count.
    fn validate(&self, _dialect: Dialect, _arity: usize) -> Result<(), QueryError> {
        Ok(())
    }
}

/// Advanced hook for typed custom expressions.
///
/// Prefer [`DbFunction`] for ordinary function calls. Use this hook when a
/// dialect-specific expression cannot be represented as `name(arg, ...)`.
pub trait DbExpression<T>: Clone + Send + Sync + 'static {
    /// Returns child expressions used by this custom expression.
    fn args(&self) -> FunctionArgs {
        FunctionArgs::default()
    }

    /// Validates dialect support before rendering.
    fn validate(&self, _dialect: Dialect) -> Result<(), QueryError> {
        Ok(())
    }

    /// Renders this expression with pre-rendered child expression access.
    fn render(&self, ctx: &mut ExprRenderCtx<'_>) -> Result<String, QueryError>;
}

/// Rendering context passed to [`DbExpression`].
pub struct ExprRenderCtx<'a> {
    dialect: Dialect,
    args: &'a [String],
}

impl<'a> ExprRenderCtx<'a> {
    pub(super) fn new(dialect: Dialect, args: &'a [String]) -> Self {
        Self { dialect, args }
    }

    /// Returns the SQL dialect currently being rendered.
    pub fn dialect(&self) -> Dialect {
        self.dialect
    }

    /// Returns a pre-rendered child expression.
    pub fn arg(&self, index: usize) -> Result<&str, QueryError> {
        self.args
            .get(index)
            .map(String::as_str)
            .ok_or_else(|| QueryError::BindError(format!("missing expression argument {index}")))
    }
}

/// Type-erased function arguments captured by the typed AST.
#[derive(Clone, Default)]
pub struct FunctionArgs {
    pub(super) nodes: Vec<ExprNode>,
}

impl FunctionArgs {
    /// Builds expression arguments for custom database functions or expressions.
    pub fn new(args: impl IntoFunctionArgs) -> Self {
        args.into_function_args()
    }
}

/// Converts tuple arguments into typed function arguments.
pub trait IntoFunctionArgs {
    #[doc(hidden)]
    fn into_function_args(self) -> FunctionArgs;
}

/// Converts a single expression-like value into an untyped function argument.
#[doc(hidden)]
pub trait IntoAnyExpr {
    #[doc(hidden)]
    fn into_any_expr(self) -> FunctionArgs;
}

/// Creates a typed database function expression.
pub fn func<T, F, A>(function: F, args: A) -> Expr<T>
where
    T: 'static,
    F: DbFunction<T>,
    A: IntoFunctionArgs,
{
    let args = args.into_function_args();
    Expr::new(ExprNode::Function {
        function: Arc::new(FunctionAdapter::<T, F> {
            function,
            _marker: PhantomData,
        }),
        args: args.nodes,
    })
}

/// Creates a typed custom expression.
pub fn custom<T, E>(expression: E) -> Expr<T>
where
    T: 'static,
    E: DbExpression<T>,
{
    let args = expression.args();
    Expr::new(ExprNode::Custom {
        expression: Arc::new(CustomAdapter::<T, E> {
            expression,
            _marker: PhantomData,
        }),
        args: args.nodes,
    })
}

pub(super) trait FunctionSpec: Send + Sync {
    fn name(&self, dialect: Dialect) -> Result<Cow<'static, str>, QueryError>;

    fn validate(&self, dialect: Dialect, arity: usize) -> Result<(), QueryError>;
}

pub(super) trait CustomExpressionSpec: Send + Sync {
    fn validate(&self, dialect: Dialect) -> Result<(), QueryError>;

    fn render(&self, ctx: &mut ExprRenderCtx<'_>) -> Result<String, QueryError>;
}

struct FunctionAdapter<T, F> {
    function: F,
    _marker: PhantomData<fn() -> T>,
}

struct CustomAdapter<T, E> {
    expression: E,
    _marker: PhantomData<fn() -> T>,
}

impl<T, F> FunctionSpec for FunctionAdapter<T, F>
where
    T: 'static,
    F: DbFunction<T>,
{
    fn name(&self, dialect: Dialect) -> Result<Cow<'static, str>, QueryError> {
        let name = self.function.name(dialect)?;
        dialect::render_function(dialect, name)
    }

    fn validate(&self, dialect: Dialect, arity: usize) -> Result<(), QueryError> {
        self.function.validate(dialect, arity)
    }
}

impl<T, E> CustomExpressionSpec for CustomAdapter<T, E>
where
    T: 'static,
    E: DbExpression<T>,
{
    fn validate(&self, dialect: Dialect) -> Result<(), QueryError> {
        self.expression.validate(dialect)
    }

    fn render(&self, ctx: &mut ExprRenderCtx<'_>) -> Result<String, QueryError> {
        self.expression.render(ctx)
    }
}

impl IntoFunctionArgs for () {
    fn into_function_args(self) -> FunctionArgs {
        FunctionArgs::default()
    }
}

impl<T> IntoAnyExpr for Expr<T> {
    fn into_any_expr(self) -> FunctionArgs {
        FunctionArgs {
            nodes: vec![self.node],
        }
    }
}

impl<T> IntoAnyExpr for &Expr<T> {
    fn into_any_expr(self) -> FunctionArgs {
        FunctionArgs {
            nodes: vec![self.node.clone()],
        }
    }
}

impl<T> IntoAnyExpr for Column<T> {
    fn into_any_expr(self) -> FunctionArgs {
        self.into_expr().into_any_expr()
    }
}

impl<T> IntoAnyExpr for &Column<T> {
    fn into_any_expr(self) -> FunctionArgs {
        self.into_expr().into_any_expr()
    }
}

impl<T> IntoAnyExpr for Var<T> {
    fn into_any_expr(self) -> FunctionArgs {
        self.into_expr().into_any_expr()
    }
}

impl<T> IntoAnyExpr for &Var<T> {
    fn into_any_expr(self) -> FunctionArgs {
        self.into_expr().into_any_expr()
    }
}

macro_rules! impl_function_args {
    ($($name:ident),+ $(,)?) => {
        impl<$($name),+> IntoFunctionArgs for ($($name,)+)
        where
            $($name: IntoAnyExpr,)+
        {
            fn into_function_args(self) -> FunctionArgs {
                #[allow(non_snake_case)]
                let ($($name,)+) = self;
                let mut nodes = Vec::new();
                $(nodes.extend($name.into_any_expr().nodes);)+
                FunctionArgs { nodes }
            }
        }
    };
}

impl_function_args!(A);
impl_function_args!(A, B);
impl_function_args!(A, B, C);
impl_function_args!(A, B, C, D);
impl_function_args!(A, B, C, D, E);
impl_function_args!(A, B, C, D, E, F);

use super::expr::IntoExpr;
