use clap::Clap;
use dbcore::{DBError, Result, KVDB};
use rustyline::error::ReadlineError;

mod cmd;
use cmd::CmdHelper;

/// 内存 key-value 数据库 kvbd 使用说明, 使用 --help 现实详细帮助信息
#[derive(Clap)]
#[clap(version = "0.1.0", author = "guangfuhe")]
pub struct BootstrapOpts {
    /// 配置kvdb 中 key 的数量， 默认是256
    #[clap(short = "s", long = "key_size", default_value = "256")]
    keys: usize,

    /// 输出信息的详细程度，可多次使用
    #[clap(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: i32,
}

fn print_result<T>(res: Result<T>)
where
    T: std::fmt::Debug,
{
    match res {
        Ok(s) => {
            println!("{:?}", s);
        }
        Err(e) => {
            println!("{:?}", e);
        }
    }
}

fn print_option_result<T>(res: Result<Option<T>>)
where
    T: std::fmt::Debug,
{
    match res {
        Ok(Some(s)) => {
            println!("{:?}", s);
        }
        Ok(None) => {
            println!("(empty or not found)");
        }
        Err(e) => {
            println!("{:?}", e);
        }
    }
}

fn parse_bool(s: &str) -> Result<bool> {
    if s == "true" {
        Ok(true)
    } else if s == "false" {
        Ok(false)
    } else {
        Err(DBError::WrongValueType)
    }
}

fn process(db: &mut KVDB, input: &String) {
    print!("memkv: ");
    // let unknow_operation = "unknown operation!";
    let words: Vec<&str> = input.trim().split_whitespace().collect();
    match words.len() {
        0 => {}
        1 => match words[0] {
            "size" => {
                println!("{}", db.size());
            }
            _ => {
                println!("unknown command or missing params!");
            }
        },
        2 => match words[0] {
            "get" => {
                print_option_result(db.get(&String::from(words[1])));
            }
            "spop" => {
                print_option_result(db.spop(&String::from(words[1])));
            }
            "slen" => {
                print_option_result(db.slen(&String::from(words[1])));
            }
            "smembers" => {
                print_option_result(db.smembers(&String::from(words[1])));
            }
            "hkeys" => {
                print_option_result(db.hkeys(&String::from(words[1])));
            }
            "hvalues" => {
                print_option_result(db.hvalues(&String::from(words[1])));
            }
            "hlen" => {
                print_option_result(db.hlen(&String::from(words[1])));
            }
            "exists" => {
                println!("{}", db.exists(&String::from(words[1])));
            }
            _ => {
                println!("unknown command or missing params");
            }
        },
        3 => {
            let key = String::from(words[1]);
            let arg = String::from(words[2]);

            match words[0] {
                "set" => {
                    print_result(db.sets(&key, arg));
                }
                "srandmember" => match usize::from_str_radix(words[2], 10) {
                    Ok(num) => {
                        print_result(db.srandmember(&key, num));
                    }
                    Err(_) => {
                        println!("{} is not a number", arg);
                    }
                },
                "sismember" => {
                    print_result(db.sismember(&key, &arg));
                }
                "hget" => {
                    print_result(db.hget(&key, &arg));
                }
                "hexists" => {
                    print_result(db.hexists(&key, &arg));
                }
                "hdel" => {
                    print_result(db.hdel(&key, &arg));
                }
                _ => {
                    println!("unknow command!");
                }
            }
        }
        _ => {
            let key = String::from(words[1]);

            match words[0] {
                "set" => {
                    if words.len() > 3 && words.len() <= 6 {
                        let value = String::from(words[2]);
                        let not_exists = parse_bool(words[3]);
                        let already_exists = parse_bool(words[4]);
                        let mut expire = None;
                        if words.len() == 6 {
                            if let Ok(v) = u64::from_str_radix(words[5], 10) {
                                expire = Some(v);
                            }
                        }
                        if not_exists.is_ok() && already_exists.is_ok() {
                            print_result(db.set(
                                &key,
                                value,
                                not_exists.unwrap(),
                                already_exists.unwrap(),
                                expire,
                            ));
                            return;
                        }
                    }
                    println!("input error, please check with `help` command!");
                }
                "sadd" => {
                    if words.len() > 2 {
                        let members: Vec<String> = words[2..]
                            .to_vec()
                            .iter()
                            .map(|s| String::from(*s))
                            .collect();
                        print_result(db.sadd(&key, members));
                    } else {
                        println!("input error, please check with `help` command!");
                    }
                }
                "srem" => {
                    if words.len() > 2 {
                        let members: Vec<String> = words[2..]
                            .to_vec()
                            .iter()
                            .map(|s| String::from(*s))
                            .collect();
                        print_result(db.srem(&key, members));
                    } else {
                        println!("input error, please check with `help` command!");
                    }
                }
                "hset" => {
                    if words.len() == 4 {
                        let field = String::from(words[2]);
                        let value = String::from(words[3]);
                        print_result(db.hset(&key, field, value));
                    } else {
                        println!("input error, please check with `help` command!");
                    }
                }
                "hmset" => {
                    if words.len() > 2 && words.len() % 2 == 0 {
                        let mut pairs: Vec<(String, String)> = Vec::new();
                        for i in (2..words.len()).filter(|n| n % 2 == 0) {
                            pairs.push((String::from(words[i]), String::from(words[i + 1])));
                        }
                        print_result(db.hmset(&key, pairs));
                    } else {
                        println!("input error, please check with `help` command!");
                    }
                }
                "hmget" => {
                    if words.len() > 2 {
                        let fields: Vec<String> = words[2..]
                            .to_vec()
                            .iter()
                            .map(|s| String::from(*s))
                            .collect();
                        print_result(db.hmget(&key, &fields));
                    } else {
                        println!("input error, please check with `help` command!");
                    }
                }
                "del" => {
                    if words.len() > 1 {
                        let keys: Vec<String> = words[1..]
                            .to_vec()
                            .into_iter()
                            .map(|s| String::from(s))
                            .collect();
                        println!("{}", db.del(keys));
                    }
                }
                _ => {
                    println!("unknow command or missing params", );
                }
            }
        }
    }
}

fn main() {
    let bootstrap_opts: BootstrapOpts = BootstrapOpts::parse();
    println!("#    # #    # #    #           ");
    println!("##  ## #   #  #    #           Welcome to use memkv!");
    println!("# ## # ####   #    #           ");
    println!("#    # #  #   #    #           configs:");
    println!(
        "#    # #   #   #  #               * keys_size = {}",
        bootstrap_opts.keys
    );
    println!(
        "#    # #    #   ##                * verbose   = {}",
        bootstrap_opts.verbose
    );
    println!("\n\n\nfor more help information, please input \"help\"\n");

    let mut db: KVDB = KVDB::new(Some(bootstrap_opts.keys));
    let mut rl = cmd::cmd_repl();

    loop {
        match rl.readline("> "){
            Ok(input)=> {
                rl.add_history_entry(input.clone());
                match input.as_str() {
                    "help" => {
                        let helper: &CmdHelper = rl.helper().unwrap();
                        helper.print_help();
                    }
                    _ => {
                        process(&mut db, &input);
                    }
                }
            },
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break
            },
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break
            }
        }
    }
}
