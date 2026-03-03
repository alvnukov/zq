mod cli;
mod service;

use std::process;

fn main() {
    match service::run() {
        Ok(code) => process::exit(code),
        Err(service::Error::Query(msg)) => {
            eprintln!("{msg}");
            let is_compile_error =
                msg.starts_with("jq: unsupported query:") || msg.contains("jq: 1 compile error");
            process::exit(if is_compile_error { 3 } else { 5 });
        }
        Err(service::Error::Io(err)) => {
            eprintln!("jq: error: {err}");
            process::exit(2);
        }
    };
}
