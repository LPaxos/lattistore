use crate::patch::*;

pub enum Stmt {
    Get(Key),
    Put(Key, Expr),
    Assign(Ident, Expr),
    If(BoolExpr, Vec<Stmt>, Vec<Stmt>),
}

pub enum Expr {
    Ident(Ident),
    Get(Key),
    Const(Value),
    ArithOp(Box<Expr>, ArithOp, Box<Expr>),
}

pub enum BoolExpr {
    Cmp(Expr, CmpOp, Expr),
    Conn(Box<BoolExpr>, BoolOp, Box<BoolExpr>),
}

#[derive(PartialEq, Eq, Hash)]
pub struct Ident(pub String);

#[derive(Clone, Copy)]
pub enum ArithOp {
    Add,
}

#[derive(Clone, Copy)]
pub enum CmpOp {
    Eq,
}

#[derive(Clone, Copy)]
pub enum BoolOp {
    And,
    Or,
}
