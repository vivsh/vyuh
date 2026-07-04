//! SQL expression and predicate AST nodes and their builders.
use std::marker::PhantomData;
use std::sync::Arc;

use crate::db::argvalue::ArgValue;

use super::extension::{CustomExpressionSpec, FunctionSpec};
use super::handles::ColumnOwner;
use super::source::SourceColumnRef;

/// Typed SQL expression node.
#[derive(Clone)]
pub struct Expr<T> {
    pub(super) node: ExprNode,
    _marker: PhantomData<fn() -> T>,
}

/// Boolean SQL predicate node.
#[derive(Clone)]
pub struct Predicate {
    pub(super) node: ExprNode,
}

/// SQL ordering expression.
#[derive(Clone)]
pub struct OrderExpr {
    pub(super) expr: ExprNode,
    pub(super) desc: bool,
}

#[derive(Clone)]
pub(super) enum ExprNode {
    Column(ColumnRef),
    Value(ValueNode),
    Binary {
        left: Box<ExprNode>,
        op: &'static str,
        right: Box<ExprNode>,
    },
    Unary {
        op: &'static str,
        expr: Box<ExprNode>,
    },
    Bool {
        left: Box<ExprNode>,
        op: &'static str,
        right: Box<ExprNode>,
    },
    Function {
        function: Arc<dyn FunctionSpec>,
        args: Vec<ExprNode>,
    },
    Custom {
        expression: Arc<dyn CustomExpressionSpec>,
        args: Vec<ExprNode>,
    },
    InSource {
        left: Box<ExprNode>,
        source: SourceColumnRef,
    },
}

#[doc(hidden)]
#[derive(Clone)]
pub struct ColumnRef {
    pub(super) owner: ColumnOwner,
    pub(super) name: Arc<str>,
}

#[derive(Clone)]
pub(super) enum ValueNode {
    Val {
        name: Option<String>,
        rust_type: &'static str,
        value: ArgValue,
    },
    Var {
        name: String,
        rust_type: Option<&'static str>,
    },
}

impl<T> Expr<T> {
    pub(super) fn new(node: ExprNode) -> Self {
        Self {
            node,
            _marker: PhantomData,
        }
    }

    /// Adds two typed expressions.
    pub fn add<R>(self, rhs: R) -> Self
    where
        R: IntoExpr<T>,
    {
        Self::new(ExprNode::Binary {
            left: Box::new(self.node),
            op: "+",
            right: Box::new(rhs.into_expr().node),
        })
    }

    /// Equality predicate.
    pub fn eq<R>(self, rhs: R) -> Predicate
    where
        R: IntoExpr<T>,
    {
        self.compare("=", rhs)
    }

    /// Greater-than predicate.
    pub fn gt<R>(self, rhs: R) -> Predicate
    where
        R: IntoExpr<T>,
    {
        self.compare(">", rhs)
    }

    /// Ascending order expression.
    pub fn asc(self) -> OrderExpr {
        OrderExpr {
            expr: self.node,
            desc: false,
        }
    }

    /// Descending order expression.
    pub fn desc(self) -> OrderExpr {
        OrderExpr {
            expr: self.node,
            desc: true,
        }
    }

    fn compare<R>(self, op: &'static str, rhs: R) -> Predicate
    where
        R: IntoExpr<T>,
    {
        Predicate::new(ExprNode::Binary {
            left: Box::new(self.node),
            op,
            right: Box::new(rhs.into_expr().node),
        })
    }
}

/// Converts typed expression-like values into an AST expression.
pub trait IntoExpr<T> {
    #[doc(hidden)]
    fn into_expr(self) -> Expr<T>;
}

impl<T> IntoExpr<T> for Expr<T> {
    fn into_expr(self) -> Expr<T> {
        self
    }
}

/// Converts projected source columns into a subquery predicate source.
pub trait IntoSourceColumn<T> {
    #[doc(hidden)]
    fn into_source_column(self) -> SourceColumnRef;
}

impl Predicate {
    pub(super) fn new(node: ExprNode) -> Self {
        Self { node }
    }

    /// Negates this predicate with SQL `NOT`.
    pub fn not(self) -> Self {
        std::ops::Not::not(self)
    }

    /// Combines two predicates with `AND`.
    pub fn and(self, rhs: Predicate) -> Self {
        Self::new(ExprNode::Bool {
            left: Box::new(self.node),
            op: "AND",
            right: Box::new(rhs.node),
        })
    }

    /// Combines two predicates with `OR`.
    pub fn or(self, rhs: Predicate) -> Self {
        Self::new(ExprNode::Bool {
            left: Box::new(self.node),
            op: "OR",
            right: Box::new(rhs.node),
        })
    }
}

impl std::ops::Not for Predicate {
    type Output = Self;

    fn not(self) -> Self::Output {
        Self::new(ExprNode::Unary {
            op: "NOT",
            expr: Box::new(self.node),
        })
    }
}
