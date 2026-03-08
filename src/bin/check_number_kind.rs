fn main() {
    for raw in ["15", "3.0", "4", "1", "100", "1e-1", "0.1"] {
        let value: serde_json::Value = serde_json::from_str(raw).expect("parse");
        let number = value.as_number().expect("number");
        println!(
            "{raw:>5} => to_string={:<8} is_i64={} is_u64={} is_f64={}",
            number,
            number.is_i64(),
            number.is_u64(),
            number.is_f64()
        );
    }
}
