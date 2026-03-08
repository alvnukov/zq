use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use serde_yaml::Value as YamlValue;
use std::env;
use std::fmt::Write as _;
use std::sync::OnceLock;
use zq::NativeValue;

struct BenchData {
    json_stream: String,
    yaml_docs: String,
    rows: usize,
}

fn bench_rows() -> usize {
    env::var("BENCH_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(60_000)
}

fn dataset() -> &'static BenchData {
    static DATA: OnceLock<BenchData> = OnceLock::new();
    DATA.get_or_init(|| {
        let rows = bench_rows();
        eprintln!("[parsing-bench] generating dataset rows={rows}");
        let mut json_stream = String::with_capacity(rows * 140);
        let mut yaml_docs = String::with_capacity(rows * 200);

        for i in 0..rows {
            let id = i as i64;
            let active = if i % 2 == 0 { "true" } else { "false" };
            let group = (i % 32) as i64;
            let value = ((i * 37) % 1000) as i64;
            let a = (i % 97) as i64;
            let b = ((i * 3) % 89) as i64;
            let t0 = (i % 7) as i64;
            let t1 = ((i + 1) % 7) as i64;
            let t2 = ((i + 2) % 7) as i64;

            let _ = writeln!(
                json_stream,
                "{{\"id\":{id},\"active\":{active},\"group\":{group},\"value\":{value},\"a\":{a},\"b\":{b},\"text\":\"item-{id:06}-alpha-beta\",\"tags\":[{t0},{t1},{t2}]}}"
            );

            if i > 0 {
                yaml_docs.push_str("---\n");
            }
            let _ = writeln!(
                yaml_docs,
                "id: {id}\nactive: {active}\ngroup: {group}\nvalue: {value}\na: {a}\nb: {b}\ntext: \"item-{id:06}-alpha-beta\"\ntags:\n  - {t0}\n  - {t1}\n  - {t2}"
            );
        }

        BenchData {
            json_stream,
            yaml_docs,
            rows,
        }
    })
}

fn json_checksum(values: &[JsonValue]) -> i64 {
    values
        .iter()
        .filter_map(|v| v.get("id").and_then(JsonValue::as_i64))
        .sum::<i64>()
}

fn yaml_checksum(values: &[YamlValue]) -> i64 {
    values
        .iter()
        .filter_map(|v| match v {
            YamlValue::Mapping(map) => map
                .get(YamlValue::String("id".to_string()))
                .and_then(YamlValue::as_i64),
            _ => None,
        })
        .sum::<i64>()
}

fn native_checksum(values: &[NativeValue]) -> i64 {
    values.iter().filter_map(native_object_id).sum::<i64>()
}

fn native_object_id(value: &NativeValue) -> Option<i64> {
    let NativeValue::Object(fields) = value else {
        return None;
    };
    let id_value = fields.get("id")?;
    let NativeValue::Number(number) = id_value else {
        return None;
    };
    if let Some(v) = number.as_i64() {
        return Some(v);
    }
    number.as_u64().and_then(|v| i64::try_from(v).ok())
}

fn parse_json_stream_serde(input: &str) -> Result<(usize, i64), serde_json::Error> {
    let mut values = Vec::new();
    for next in serde_json::Deserializer::from_str(input).into_iter::<JsonValue>() {
        values.push(next?);
    }
    Ok((values.len(), json_checksum(&values)))
}

fn parse_yaml_docs_serde(input: &str) -> Result<(usize, i64), serde_yaml::Error> {
    let docs = serde_yaml::Deserializer::from_str(input)
        .map(YamlValue::deserialize)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((docs.len(), yaml_checksum(&docs)))
}

fn parse_yaml_docs_serde_to_json(input: &str) -> Result<(usize, i64), String> {
    let docs = serde_yaml::Deserializer::from_str(input)
        .map(YamlValue::deserialize)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let values = docs
        .into_iter()
        .map(|v| serde_json::to_value(v).map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((values.len(), json_checksum(&values)))
}

fn parse_zq_auto(input: &str) -> Result<(usize, i64), String> {
    let parsed = zq::parse_native_input_values_auto(input).map_err(|e| e.to_string())?;
    Ok((parsed.values.len(), json_checksum(&parsed.values)))
}

fn parse_zq_auto_native(input: &str) -> Result<(usize, i64), String> {
    let parsed = zq::parse_native_input_values_auto_native(input).map_err(|e| e.to_string())?;
    Ok((parsed.values.len(), native_checksum(&parsed.values)))
}

fn parse_zq_prefer_json(input: &str) -> Result<(usize, i64), String> {
    let values = zq::parse_native_input_docs_prefer_json(input).map_err(|e| e.to_string())?;
    Ok((values.len(), json_checksum(&values)))
}

fn parse_zq_prefer_yaml(input: &str) -> Result<(usize, i64), String> {
    let values = zq::parse_native_input_docs_prefer_yaml(input).map_err(|e| e.to_string())?;
    Ok((values.len(), json_checksum(&values)))
}

fn parse_zq_json_only(input: &str) -> Result<(usize, i64), String> {
    let values = zq::parse_native_json_values_only(input).map_err(|e| e.to_string())?;
    Ok((values.len(), json_checksum(&values)))
}

fn bench_parsing_layers(c: &mut Criterion) {
    let data = dataset();
    let mut group = c.benchmark_group("parsing_layers");
    group.throughput(Throughput::Elements(data.rows as u64));

    let (n_json, chk_json) = parse_json_stream_serde(&data.json_stream).expect("serde json parse");
    assert_eq!(n_json, data.rows);
    let (n_yaml, chk_yaml) = parse_yaml_docs_serde(&data.yaml_docs).expect("serde yaml parse");
    assert_eq!(n_yaml, data.rows);
    let (n_auto_json, chk_auto_json) = parse_zq_auto(&data.json_stream).expect("zq auto json");
    assert_eq!(n_auto_json, data.rows);
    assert_eq!(chk_auto_json, chk_json);
    let (n_auto_json_native, chk_auto_json_native) =
        parse_zq_auto_native(&data.json_stream).expect("zq auto json native");
    assert_eq!(n_auto_json_native, data.rows);
    assert_eq!(chk_auto_json_native, chk_json);
    let (n_pref_json, chk_pref_json) =
        parse_zq_prefer_json(&data.json_stream).expect("zq prefer json");
    assert_eq!(n_pref_json, data.rows);
    assert_eq!(chk_pref_json, chk_json);
    let (n_auto_yaml, chk_auto_yaml) = parse_zq_auto(&data.yaml_docs).expect("zq auto yaml");
    assert_eq!(n_auto_yaml, data.rows);
    assert_eq!(chk_auto_yaml, chk_yaml);
    let (n_auto_yaml_native, chk_auto_yaml_native) =
        parse_zq_auto_native(&data.yaml_docs).expect("zq auto yaml native");
    assert_eq!(n_auto_yaml_native, data.rows);
    assert_eq!(chk_auto_yaml_native, chk_yaml);
    let (n_pref_yaml, chk_pref_yaml) =
        parse_zq_prefer_yaml(&data.yaml_docs).expect("zq prefer yaml");
    assert_eq!(n_pref_yaml, data.rows);
    assert_eq!(chk_pref_yaml, chk_yaml);
    let (n_yaml_to_json, chk_yaml_to_json) =
        parse_yaml_docs_serde_to_json(&data.yaml_docs).expect("serde yaml->json");
    assert_eq!(n_yaml_to_json, data.rows);
    assert_eq!(chk_yaml_to_json, chk_yaml);
    let (n_json_only, chk_json_only) =
        parse_zq_json_only(&data.json_stream).expect("zq json-only parse");
    assert_eq!(n_json_only, data.rows);
    assert_eq!(chk_json_only, chk_json);

    eprintln!("[parsing-bench] parity check complete rows={}", data.rows);

    group.bench_with_input(
        BenchmarkId::new("serde_json_stream", data.rows),
        &data.json_stream,
        |b, input| {
            b.iter(|| {
                let result = parse_json_stream_serde(input).expect("serde json bench");
                black_box(result)
            })
        },
    );
    group.bench_with_input(
        BenchmarkId::new("zq_json_only", data.rows),
        &data.json_stream,
        |b, input| {
            b.iter(|| {
                let result = parse_zq_json_only(input).expect("zq json-only bench");
                black_box(result)
            })
        },
    );
    group.bench_with_input(
        BenchmarkId::new("zq_auto_detect_json", data.rows),
        &data.json_stream,
        |b, input| {
            b.iter(|| {
                let result = parse_zq_auto(input).expect("zq auto json bench");
                black_box(result)
            })
        },
    );
    group.bench_with_input(
        BenchmarkId::new("zq_auto_detect_json_native", data.rows),
        &data.json_stream,
        |b, input| {
            b.iter(|| {
                let result = parse_zq_auto_native(input).expect("zq auto json native bench");
                black_box(result)
            })
        },
    );
    group.bench_with_input(
        BenchmarkId::new("zq_prefer_json", data.rows),
        &data.json_stream,
        |b, input| {
            b.iter(|| {
                let result = parse_zq_prefer_json(input).expect("zq prefer json bench");
                black_box(result)
            })
        },
    );
    group.bench_with_input(
        BenchmarkId::new("serde_yaml_docs", data.rows),
        &data.yaml_docs,
        |b, input| {
            b.iter(|| {
                let result = parse_yaml_docs_serde(input).expect("serde yaml bench");
                black_box(result)
            })
        },
    );
    group.bench_with_input(
        BenchmarkId::new("serde_yaml_to_json", data.rows),
        &data.yaml_docs,
        |b, input| {
            b.iter(|| {
                let result = parse_yaml_docs_serde_to_json(input).expect("serde yaml->json bench");
                black_box(result)
            })
        },
    );
    group.bench_with_input(
        BenchmarkId::new("zq_prefer_yaml", data.rows),
        &data.yaml_docs,
        |b, input| {
            b.iter(|| {
                let result = parse_zq_prefer_yaml(input).expect("zq prefer yaml bench");
                black_box(result)
            })
        },
    );
    group.bench_with_input(
        BenchmarkId::new("zq_auto_detect_yaml", data.rows),
        &data.yaml_docs,
        |b, input| {
            b.iter(|| {
                let result = parse_zq_auto(input).expect("zq auto yaml bench");
                black_box(result)
            })
        },
    );
    group.bench_with_input(
        BenchmarkId::new("zq_auto_detect_yaml_native", data.rows),
        &data.yaml_docs,
        |b, input| {
            b.iter(|| {
                let result = parse_zq_auto_native(input).expect("zq auto yaml native bench");
                black_box(result)
            })
        },
    );

    group.finish();
}

criterion_group!(parsing_benches, bench_parsing_layers);
criterion_main!(parsing_benches);
