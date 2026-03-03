mod cli;
mod service;

use std::process;

fn main() {
    match service::run() {
        Ok(code) => process::exit(code),
        Err(err) => {
            eprintln!("zq failed: {err}");
            process::exit(1);
        }
    };
}
