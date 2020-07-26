mod ast;
mod compile;

use lalrpop_util::*;

lalrpop_mod!(lang, "/front/lang.rs");

use crate::patch::*;
use compile::*;

pub fn parse_and_compile<'inp>(
    prog: &'inp str,
) -> Result<(Transaction, Vec<Key>), ParseError<usize, lexer::Token<'inp>, &str>> {
    let ast = lang::ProgParser::new().parse(prog)?;
    Ok(compile(ast))
}
