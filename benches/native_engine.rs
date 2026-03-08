use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::Value as JsonValue;
use std::env;
use std::sync::OnceLock;
use zq::{try_run_jq_native_stream_with_paths_options, EngineRunOptions, NativeStreamStatus};

#[derive(Clone, Copy)]
struct BenchCase {
    name: &'static str,
    query: &'static str,
}

const CASES: &[BenchCase] = &[
    BenchCase {
        name: "id",
        query: ".id",
    },
    BenchCase {
        name: "mod",
        query: ".value % 7",
    },
    BenchCase {
        name: "gsub",
        query: r#".text | gsub("[aeiou]";"")"#,
    },
    BenchCase {
        name: "pick",
        query: "{id,group,value}",
    },
    BenchCase {
        name: "add",
        query: ".a + .b",
    },
    BenchCase {
        name: "length",
        query: ".tags | length",
    },
    BenchCase {
        name: "filter_gt",
        query: "select(.id > 2) | .id",
    },
    BenchCase {
        name: "filter_pick",
        query: "select(.id > 2) | {id,group,value}",
    },
];

fn bench_rows() -> usize {
    env::var("BENCH_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(200_000)
}

fn dataset() -> &'static Vec<JsonValue> {
    static DATA: OnceLock<Vec<JsonValue>> = OnceLock::new();
    DATA.get_or_init(|| {
        let rows = bench_rows();
        eprintln!("[native-bench] generating dataset rows={rows}");
        (0..rows)
            .map(|i| {
                let id = i as i64;
                serde_json::json!({
                    "id": id,
                    "active": i % 2 == 0,
                    "group": (i % 32) as i64,
                    "value": ((i * 37) % 1000) as i64,
                    "a": (i % 97) as i64,
                    "b": ((i * 3) % 89) as i64,
                    "text": format!("item-{i:06}-alpha-beta"),
                    "tags": [(i % 7) as i64, ((i + 1) % 7) as i64, ((i + 2) % 7) as i64]
                })
            })
            .collect()
    })
}

fn run_native_count_with_mode(
    mode: &str,
    query: &str,
    inputs: &[JsonValue],
) -> Result<usize, String> {
    env::set_var("ZQ_NATIVE_PAR", mode);
    let mut count = 0usize;
    let status = try_run_jq_native_stream_with_paths_options(
        query,
        inputs,
        EngineRunOptions { null_input: false },
        |_v| {
            count += 1;
            Ok(())
        },
    )
    .map_err(|e| e.to_string())?;
    match status {
        NativeStreamStatus::Executed => Ok(count),
        NativeStreamStatus::Unsupported => {
            Err(format!("query is unsupported by native engine: {query}"))
        }
    }
}

fn bench_native_engine(c: &mut Criterion) {
    let inputs = dataset();
    let mut group = c.benchmark_group("native_engine_seq_vs_par");
    group.throughput(Throughput::Elements(inputs.len() as u64));

    for (idx, case) in CASES.iter().enumerate() {
        eprintln!(
            "[native-bench {}/{}] verify parity for case={}",
            idx + 1,
            CASES.len(),
            case.name
        );
        let seq_count = run_native_count_with_mode("0", case.query, inputs).expect("seq count");
        let par_count = run_native_count_with_mode("1", case.query, inputs).expect("par count");
        assert_eq!(
            seq_count, par_count,
            "seq/par count mismatch for case={}",
            case.name
        );

        group.bench_with_input(
            BenchmarkId::new("seq", case.name),
            &case.query,
            |b, query| {
                b.iter(|| {
                    let n = run_native_count_with_mode("0", query, inputs).expect("seq bench run");
                    black_box(n)
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("par", case.name),
            &case.query,
            |b, query| {
                b.iter(|| {
                    let n = run_native_count_with_mode("1", query, inputs).expect("par bench run");
                    black_box(n)
                })
            },
        );
    }

    env::remove_var("ZQ_NATIVE_PAR");
    group.finish();
}

criterion_group!(native_engine_benches, bench_native_engine);
criterion_main!(native_engine_benches);
