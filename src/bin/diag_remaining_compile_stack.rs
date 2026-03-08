use std::env;

struct FixtureCase {
    query: &'static str,
    input: &'static str,
    outputs: &'static [&'static str],
}

static CASES: &[FixtureCase] = include!("../fixtures_jq_remaining_compile.inc");

fn main() {
    let mut args = env::args().skip(1);
    let Some(index_arg) = args.next() else {
        eprintln!("usage: diag_remaining_compile_stack <case-index>");
        eprintln!("cases: {}", CASES.len());
        std::process::exit(1);
    };

    let index: usize = index_arg.parse().expect("case index must be usize");
    let case = CASES.get(index).expect("case index out of range");

    eprintln!("case {index}: query={}", case.query);
    let query = case.query.to_string();
    let input = case.input.to_string();
    let expected_len = case.outputs.len();

    let worker = std::thread::Builder::new()
        .name("diag-remaining-compile-worker".to_string())
        .stack_size(2 * 1024 * 1024)
        .spawn(move || zq::run_native_query_stream_jsonish(&query, &input, &[]))
        .expect("spawn worker thread");

    let exit = match worker.join() {
        Ok(Ok(actual)) => {
            if actual.len() != expected_len {
                eprintln!(
                    "mismatch: got {} outputs, expected {}",
                    actual.len(),
                    expected_len
                );
                4
            } else {
                0
            }
        }
        Ok(Err(err)) => {
            eprintln!("query failed: {err:?}");
            2
        }
        Err(_) => {
            eprintln!("worker thread panicked");
            3
        }
    };

    std::process::exit(exit);
}
