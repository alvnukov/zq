mod cli;
mod service;

use std::process;

fn main() {
    if let Err(err) = service::run() {
        eprintln!("zq failed: {err}");
        process::exit(1);
    }
}
