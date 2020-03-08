use std::collections::HashSet;

use rustyline::Editor;
use rustyline::{hint::Hinter, Context};
use rustyline_derive::{Completer, Helper, Highlighter, Validator};

#[derive(Completer, Helper, Validator, Highlighter)]
pub struct CmdHelper {
    hints: HashSet<String>,
}

impl Hinter for CmdHelper {
    fn hint(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Option<String> {
        if pos < line.len() {
            return None;
        }
        self.hints
            .iter()
            .filter_map(|hint| {
                if pos > 0 && line.ends_with(" ") && hint.starts_with(&line[..pos]) {
                    Some(hint[pos..].to_owned())
                } else {
                    None
                }
            })
            .nth(0)
    }
}

impl CmdHelper {
    pub fn print_help(&self) {
        println!("\nmemkv help info:\n",);
        self.hints.iter().for_each(|hint| println!("{}", hint));
        println!("\n");
    }
}

fn cmd_hints() -> HashSet<String> {
    let mut set = HashSet::new();
    set.insert(String::from("help"));

    set.insert(String::from("get key"));
    set.insert(String::from("set key value"));
    set.insert(String::from(
        "set key value expire not_exists already_exists",
    ));

    set.insert(String::from("sadd key member [member ...]"));
    set.insert(String::from("srandmember key count"));
    set.insert(String::from("spop key"));
    set.insert(String::from("sismember key member"));
    set.insert(String::from("srem key member [member ...]"));
    set.insert(String::from("slen key"));
    set.insert(String::from("smembers key"));

    set.insert(String::from("hget key field"));
    set.insert(String::from("hset key field value"));
    set.insert(String::from("hmset key field value [field value ...]"));
    set.insert(String::from("hmget key field [field ...]"));
    set.insert(String::from("hkeys key"));
    set.insert(String::from("hvalues key"));
    set.insert(String::from("hexists key field"));
    set.insert(String::from("hlen key"));
    set.insert(String::from("hdel key field"));

    set.insert(String::from("del key [key ...]"));
    set.insert(String::from("exists key"));
    set.insert(String::from("size"));

    set
}

pub fn cmd_repl() -> Editor<CmdHelper> {
    let hint = CmdHelper { hints: cmd_hints() };
    let mut rl = Editor::new();
    rl.set_helper(Some(hint));
    rl
}
