pub(super) fn strip_serde_line_col_suffix(msg: &str) -> &str {
    let marker = " at line ";
    let Some(idx) = msg.rfind(marker) else {
        return msg;
    };
    let suffix = &msg[idx + marker.len()..];
    let Some((line, col_part)) = suffix.split_once(" column ") else {
        return msg;
    };
    if line.trim().parse::<usize>().is_ok() && col_part.trim().parse::<usize>().is_ok() {
        &msg[..idx]
    } else {
        msg
    }
}

pub(super) fn unfinished_abandoned_at_eof_message(input: &str) -> String {
    let mut err_pos: Option<(usize, usize)> = None;
    for next in serde_json::Deserializer::from_str(input).into_iter::<zq::NativeValue>() {
        if let Err(e) = next {
            err_pos = Some((e.line(), e.column()));
            break;
        }
    }

    let (line, col) = if let Some((line, col)) = err_pos {
        (line, col)
    } else {
        index_to_line_col(input, input.len(), true)
    };
    format!("Unfinished abandoned text at EOF at line {line}, column {col}")
}

pub(super) fn index_to_line_col(s: &str, idx: usize, eof: bool) -> (usize, usize) {
    let mut line = 1usize;
    let mut col0 = 0usize;
    for (byte_idx, ch) in s.char_indices() {
        if byte_idx >= idx {
            break;
        }
        if ch == '\n' {
            line += 1;
            col0 = 0;
        } else {
            col0 += 1;
        }
    }
    let col = if eof { col0 } else { col0 + 1 };
    (line, col)
}

pub(super) fn raw_input_lines(input: &str) -> Vec<String> {
    input
        .split_terminator('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line).to_string())
        .collect()
}
