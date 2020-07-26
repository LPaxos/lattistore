use std::collections::BTreeSet;
use std::collections::HashMap;

use super::ast::*;
use crate::patch::*;

pub fn compile(stmts: Vec<Stmt>) -> (Transaction, Vec<Key>) {
    let mut rs = BTreeSet::new();
    for s in &stmts {
        keys_stmt(s, &mut rs);
    }

    let f = Box::new(move |rp: &Patch| {
        let mut st = State {
            rp,
            vars: HashMap::new(),
            wp: HashMap::new(),
        };

        for s in &stmts {
            stmt(s, &mut st);
        }

        st.wp
    });

    (f, rs.into_iter().collect())
}

struct State<'a> {
    rp: &'a Patch,
    vars: HashMap<&'a Ident, Value>,
    wp: Patch,
}

fn stmt<'a>(s: &'a Stmt, st: &mut State<'a>) {
    use Stmt::*;
    match s {
        Get(_) => (),
        Put(k, e) => {
            st.wp.insert(k.clone(), expr(e, st));
        }
        Assign(i, e) => {
            st.vars.insert(i, expr(e, st));
        }
        If(e, s1, s2) => {
            if bool_expr(e, st) {
                for s in s1 {
                    stmt(s, st);
                }
            } else {
                for s in s2 {
                    stmt(s, st);
                }
            }
        }
    };
}

fn expr(e: &Expr, st: &State) -> Value {
    use Expr::*;
    match e {
        Ident(i) => st.vars.get(&i).unwrap_or(&def_val()).clone(),
        Get(k) => {
            if let Some(v) = st.wp.get(k) {
                v.clone()
            } else {
                st.rp.get(k).unwrap_or(&def_val()).clone()
            }
        }
        Const(c) => c.clone(),
        ArithOp(e1, op, e2) => arith_op(*op, expr(e1, st), expr(e2, st)),
    }
}

fn bool_expr(e: &BoolExpr, st: &State) -> bool {
    use BoolExpr::*;
    match e {
        Cmp(e1, op, e2) => cmp_op(*op, expr(e1, st), expr(e2, st)),
        Conn(e1, op, e2) => bool_op(*op, bool_expr(e1, st), bool_expr(e2, st)),
        //Not(e) => !bool_expr(e, st)
    }
}

fn cmp_op(op: CmpOp, e1: Value, e2: Value) -> bool {
    use CmpOp::*;
    match op {
        Eq => e1 == e2,
        //Ge => e1 >= e2,
        //Gt => e1 > e2,
    }
}

fn bool_op(op: BoolOp, e1: bool, e2: bool) -> bool {
    use BoolOp::*;
    match op {
        And => e1 && e2,
        Or => e2 || e2,
    }
}

fn arith_op(op: ArithOp, e1: Value, e2: Value) -> Value {
    use ArithOp::*;
    match op {
        Add => e1 + &e2,
        //Sub => e1 - e2,
        //Mul => e1 * e2,
    }
}

fn keys_stmt(s: &Stmt, res: &mut BTreeSet<Key>) {
    use Stmt::*;
    match s {
        Get(k) => {
            res.insert(k.clone());
        }
        Put(_, e) => keys_expr(e, res),
        Assign(_, e) => keys_expr(e, res),
        If(e, s1, s2) => {
            keys_bool_expr(e, res);
            for s in s1 {
                keys_stmt(s, res);
            }
            for s in s2 {
                keys_stmt(s, res);
            }
        }
    }
}

fn keys_expr(e: &Expr, res: &mut BTreeSet<Key>) {
    use Expr::*;
    if let Get(k) = e {
        res.insert(k.clone());
    }
    match e {
        Get(k) => {
            res.insert(k.clone());
        }
        ArithOp(e1, _, e2) => {
            keys_expr(e1, res);
            keys_expr(e2, res);
        }
        Ident(_) => (),
        Const(_) => (),
    }
}

fn keys_bool_expr(e: &BoolExpr, res: &mut BTreeSet<Key>) {
    use BoolExpr::*;
    match e {
        Cmp(e1, _, e2) => {
            keys_expr(e1, res);
            keys_expr(e2, res);
        }
        Conn(e1, _, e2) => {
            keys_bool_expr(e1, res);
            keys_bool_expr(e2, res);
        }
        //Not(e) => keys_bool_expr(e, res),
    }
}
