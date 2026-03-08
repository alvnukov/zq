use std::iter::Peekable;
use std::str::CharIndices;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Token {
    Dot,
    Field(String),
    Rec,
    Pipe,
    SetPipe,
    Comma,
    Semi,
    Plus,
    SetPlus,
    Minus,
    SetMinus,
    Star,
    SetMult,
    Slash,
    SetDiv,
    DefinedOr,
    SetDefinedOr,
    Percent,
    SetMod,
    Assign,
    EqEq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    LParen,
    RParen,
    LBrace,
    RBrace,
    Colon,
    LBracket,
    RBracket,
    Question,
    And,
    Or,
    If,
    Then,
    Else,
    Elif,
    EndKw,
    AsKw,
    DefKw,
    ModuleKw,
    ImportKw,
    IncludeKw,
    ReduceKw,
    ForeachKw,
    LabelKw,
    BreakKw,
    Try,
    Catch,
    Format(String),
    QQStringStart,
    QQStringText(String),
    QQInterpStart,
    QQInterpEnd,
    QQStringEnd,
    Loc(usize),
    Binding(String),
    Ident(String),
    Int(i64),
    Num(String),
    Str(String),
    End,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LexError {
    pub(crate) message: String,
    pub(crate) position: usize,
}

pub(crate) fn lex(input: &str) -> Result<Vec<Token>, LexError> {
    lex_with_start_line(input, 1)
}

fn lex_with_start_line(input: &str, start_line: usize) -> Result<Vec<Token>, LexError> {
    let mut out = Vec::new();
    let mut chars = input.char_indices().peekable();
    let mut line: usize = start_line;

    while let Some((idx, ch)) = chars.peek().copied() {
        if ch.is_ascii_whitespace() {
            chars.next();
            if ch == '\n' {
                line += 1;
            } else if ch == '\r' {
                if chars.next_if(|(_, next)| *next == '\n').is_some() {
                    // treat CRLF as a single newline
                }
                line += 1;
            }
            continue;
        }
        match ch {
            '#' => {
                // jq lexer.l IN_COMMENT:
                // comments run until newline, with backslash-newline continuation.
                chars.next();
                while let Some((_, c)) = chars.next() {
                    if c == '\\' {
                        if chars.next_if(|(_, next)| *next == '\n').is_some() {
                            line += 1;
                            continue;
                        }
                        if chars.next_if(|(_, next)| *next == '\r').is_some() {
                            let _ = chars.next_if(|(_, next)| *next == '\n');
                            line += 1;
                            continue;
                        }
                    }
                    if c == '\n' {
                        line += 1;
                        break;
                    }
                    if c == '\r' {
                        let _ = chars.next_if(|(_, next)| *next == '\n');
                        line += 1;
                        break;
                    }
                }
            }
            '.' => {
                chars.next();
                if chars.next_if(|(_, c)| *c == '.').is_some() {
                    out.push(Token::Rec);
                } else if matches!(chars.peek().copied(), Some((_, next)) if next.is_ascii_digit())
                {
                    let mut num_text = String::from(".");
                    let digits = consume_digits(&mut chars, &mut num_text);
                    if digits == 0 {
                        return Err(LexError {
                            message: "invalid number literal `.`".to_string(),
                            position: idx,
                        });
                    }
                    consume_exponent(&mut chars, &mut num_text, idx)?;
                    out.push(Token::Num(num_text));
                } else if matches!(
                    chars.peek().copied(),
                    Some((_, next)) if next.is_ascii_alphabetic() || next == '_'
                ) {
                    let mut name = String::new();
                    while let Some((_, next)) = chars.peek().copied() {
                        if next.is_ascii_alphanumeric() || next == '_' {
                            name.push(next);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    out.push(Token::Field(name));
                } else {
                    out.push(Token::Dot);
                }
            }
            '|' => {
                chars.next();
                if chars.next_if(|(_, c)| *c == '=').is_some() {
                    out.push(Token::SetPipe);
                } else {
                    out.push(Token::Pipe);
                }
            }
            ',' => {
                chars.next();
                out.push(Token::Comma);
            }
            ';' => {
                chars.next();
                out.push(Token::Semi);
            }
            '+' => {
                chars.next();
                if chars.next_if(|(_, c)| *c == '=').is_some() {
                    out.push(Token::SetPlus);
                } else {
                    out.push(Token::Plus);
                }
            }
            '-' => {
                chars.next();
                if chars.next_if(|(_, c)| *c == '=').is_some() {
                    out.push(Token::SetMinus);
                } else {
                    out.push(Token::Minus);
                }
            }
            '*' => {
                chars.next();
                if chars.next_if(|(_, c)| *c == '=').is_some() {
                    out.push(Token::SetMult);
                } else {
                    out.push(Token::Star);
                }
            }
            '/' => {
                chars.next();
                if chars.next_if(|(_, c)| *c == '/').is_some() {
                    if chars.next_if(|(_, c)| *c == '=').is_some() {
                        out.push(Token::SetDefinedOr);
                    } else {
                        out.push(Token::DefinedOr);
                    }
                } else if chars.next_if(|(_, c)| *c == '=').is_some() {
                    out.push(Token::SetDiv);
                } else {
                    out.push(Token::Slash);
                }
            }
            '%' => {
                chars.next();
                if chars.next_if(|(_, c)| *c == '=').is_some() {
                    out.push(Token::SetMod);
                } else {
                    out.push(Token::Percent);
                }
            }
            '=' => {
                chars.next();
                if chars.next_if(|(_, c)| *c == '=').is_some() {
                    out.push(Token::EqEq);
                } else {
                    out.push(Token::Assign);
                }
            }
            '!' => {
                chars.next();
                if chars.next_if(|(_, c)| *c == '=').is_some() {
                    out.push(Token::NotEq);
                } else {
                    return Err(LexError {
                        message: "unexpected `!`; expected `!=`".to_string(),
                        position: idx,
                    });
                }
            }
            '<' => {
                chars.next();
                if chars.next_if(|(_, c)| *c == '=').is_some() {
                    out.push(Token::Lte);
                } else {
                    out.push(Token::Lt);
                }
            }
            '>' => {
                chars.next();
                if chars.next_if(|(_, c)| *c == '=').is_some() {
                    out.push(Token::Gte);
                } else {
                    out.push(Token::Gt);
                }
            }
            '(' => {
                chars.next();
                out.push(Token::LParen);
            }
            ')' => {
                chars.next();
                out.push(Token::RParen);
            }
            '{' => {
                chars.next();
                out.push(Token::LBrace);
            }
            '}' => {
                chars.next();
                out.push(Token::RBrace);
            }
            ':' => {
                chars.next();
                out.push(Token::Colon);
            }
            '[' => {
                chars.next();
                out.push(Token::LBracket);
            }
            ']' => {
                chars.next();
                out.push(Token::RBracket);
            }
            '?' => {
                chars.next();
                out.push(Token::Question);
            }
            '@' => {
                chars.next();
                let Some((_, first)) = chars.peek().copied() else {
                    return Err(LexError {
                        message: "unexpected `@`; expected format name".to_string(),
                        position: idx,
                    });
                };
                if !(first == '_' || first.is_ascii_alphanumeric()) {
                    return Err(LexError {
                        message: "unexpected `@`; expected format name".to_string(),
                        position: idx,
                    });
                }
                let mut ident = String::new();
                while let Some((_, next)) = chars.peek().copied() {
                    if next == '_' || next.is_ascii_alphanumeric() {
                        chars.next();
                        ident.push(next);
                    } else {
                        break;
                    }
                }
                out.push(Token::Format(ident));
            }
            '$' => {
                chars.next();
                if try_consume_loc_keyword(&mut chars) {
                    out.push(Token::Loc(line));
                    continue;
                }
                let Some((_, first)) = chars.peek().copied() else {
                    return Err(LexError {
                        message: "unexpected `$`; expected variable name".to_string(),
                        position: idx,
                    });
                };
                if !(first == '_' || first.is_ascii_alphabetic()) {
                    return Err(LexError {
                        message: "unexpected `$`; expected variable name".to_string(),
                        position: idx,
                    });
                }
                let mut ident = String::new();
                while let Some((_, next)) = chars.peek().copied() {
                    if next == '_' || next.is_ascii_alphanumeric() {
                        chars.next();
                        ident.push(next);
                    } else {
                        break;
                    }
                }
                consume_namespace_suffix(&mut chars, &mut ident)?;
                out.push(Token::Binding(ident));
            }
            '"' => {
                chars.next();
                let force_qq = matches!(out.last(), Some(Token::Format(_)));
                out.extend(lex_string_tokens(&mut chars, idx, &mut line, force_qq)?);
            }
            '0'..='9' => {
                chars.next();
                let token = lex_number_from_digit(&mut chars, ch, idx)?;
                out.push(token);
            }
            '_' | 'a'..='z' | 'A'..='Z' => {
                chars.next();
                let mut ident = String::new();
                ident.push(ch);
                while let Some((_, next)) = chars.peek().copied() {
                    if next == '_' || next.is_ascii_alphanumeric() {
                        chars.next();
                        ident.push(next);
                    } else {
                        break;
                    }
                }
                consume_namespace_suffix(&mut chars, &mut ident)?;
                match ident.as_str() {
                    "and" => out.push(Token::And),
                    "or" => out.push(Token::Or),
                    "if" => out.push(Token::If),
                    "then" => out.push(Token::Then),
                    "else" => out.push(Token::Else),
                    "elif" => out.push(Token::Elif),
                    "end" => out.push(Token::EndKw),
                    "as" => out.push(Token::AsKw),
                    "def" => out.push(Token::DefKw),
                    "module" => out.push(Token::ModuleKw),
                    "import" => out.push(Token::ImportKw),
                    "include" => out.push(Token::IncludeKw),
                    "reduce" => out.push(Token::ReduceKw),
                    "foreach" => out.push(Token::ForeachKw),
                    "label" => out.push(Token::LabelKw),
                    "break" => out.push(Token::BreakKw),
                    "try" => out.push(Token::Try),
                    "catch" => out.push(Token::Catch),
                    _ => out.push(Token::Ident(ident)),
                }
            }
            _ => {
                return Err(LexError {
                    message: format!("unexpected character `{ch}`"),
                    position: idx,
                });
            }
        }
    }
    out.push(Token::End);
    Ok(out)
}

// jq-port: lexer namespace references (`import "m" as mod; mod::f`).
fn consume_namespace_suffix(
    chars: &mut Peekable<CharIndices<'_>>,
    ident: &mut String,
) -> Result<(), LexError> {
    loop {
        let mut lookahead = chars.clone();
        let Some((idx, first)) = lookahead.next() else {
            return Ok(());
        };
        if first != ':' {
            return Ok(());
        }
        let Some((_, second)) = lookahead.next() else {
            return Ok(());
        };
        if second != ':' {
            return Ok(());
        }
        let Some((_, seg_first)) = lookahead.peek().copied() else {
            return Ok(());
        };
        if !(seg_first == '_' || seg_first.is_ascii_alphabetic()) {
            return Err(LexError {
                message: "expected identifier after `::`".to_string(),
                position: idx,
            });
        }

        let _ = chars.next();
        let _ = chars.next();
        ident.push_str("::");
        while let Some((_, next)) = chars.peek().copied() {
            if next == '_' || next.is_ascii_alphanumeric() {
                let _ = chars.next();
                ident.push(next);
            } else {
                break;
            }
        }
    }
}

fn lex_number_from_digit(
    chars: &mut Peekable<CharIndices<'_>>,
    first: char,
    position: usize,
) -> Result<Token, LexError> {
    let mut num_text = String::new();
    num_text.push(first);
    consume_digits(chars, &mut num_text);

    let mut has_fraction = false;
    let mut has_exponent = false;

    if chars.next_if(|(_, c)| *c == '.').is_some() {
        has_fraction = true;
        num_text.push('.');
        consume_digits(chars, &mut num_text);
    }

    if has_exponent_start(chars) {
        has_exponent = true;
        consume_exponent(chars, &mut num_text, position)?;
    }

    if !has_fraction && !has_exponent {
        if let Ok(value) = num_text.parse::<i64>() {
            return Ok(Token::Int(value));
        }
    }

    Ok(Token::Num(num_text))
}

fn consume_digits(chars: &mut Peekable<CharIndices<'_>>, out: &mut String) -> usize {
    let mut consumed = 0usize;
    while let Some((_, next)) = chars.peek().copied() {
        if next.is_ascii_digit() {
            chars.next();
            out.push(next);
            consumed += 1;
        } else {
            break;
        }
    }
    consumed
}

fn has_exponent_start(chars: &mut Peekable<CharIndices<'_>>) -> bool {
    matches!(chars.peek().copied(), Some((_, 'e' | 'E')))
}

fn consume_exponent(
    chars: &mut Peekable<CharIndices<'_>>,
    out: &mut String,
    position: usize,
) -> Result<(), LexError> {
    if let Some((_, e)) = chars.next_if(|(_, c)| *c == 'e' || *c == 'E') {
        out.push(e);
    } else {
        return Ok(());
    }

    if let Some((_, sign)) = chars.next_if(|(_, c)| *c == '+' || *c == '-') {
        out.push(sign);
    }

    let exp_digits = consume_digits(chars, out);
    if exp_digits == 0 {
        return Err(LexError {
            message: format!("invalid number literal `{out}`"),
            position,
        });
    }
    Ok(())
}

fn lex_string_tokens(
    chars: &mut Peekable<CharIndices<'_>>,
    position: usize,
    line: &mut usize,
    force_qq: bool,
) -> Result<Vec<Token>, LexError> {
    let mut segment = String::new();
    let mut qq_tokens = Vec::new();
    let mut has_interpolation = false;

    while let Some((_, ch)) = chars.next() {
        match ch {
            '"' => {
                if has_interpolation || force_qq {
                    if !segment.is_empty() {
                        qq_tokens.push(Token::QQStringText(decode_string_segment(
                            &segment, position,
                        )?));
                    }
                    let mut out = Vec::with_capacity(qq_tokens.len() + 2);
                    out.push(Token::QQStringStart);
                    out.extend(qq_tokens);
                    out.push(Token::QQStringEnd);
                    return Ok(out);
                }
                return Ok(vec![Token::Str(decode_string_segment(&segment, position)?)]);
            }
            '\\' => {
                if chars.next_if(|(_, c)| *c == '(').is_some() {
                    has_interpolation = true;
                    if !segment.is_empty() {
                        qq_tokens.push(Token::QQStringText(decode_string_segment(
                            &segment, position,
                        )?));
                        segment.clear();
                    }
                    qq_tokens.push(Token::QQInterpStart);
                    qq_tokens.extend(consume_interpolation_tokens(chars, position, line)?);
                    qq_tokens.push(Token::QQInterpEnd);
                    continue;
                }

                let Some((_, escaped)) = chars.next() else {
                    return Err(LexError {
                        message: "unterminated string literal".to_string(),
                        position,
                    });
                };
                segment.push('\\');
                segment.push(escaped);
                if escaped == '\n' || escaped == '\r' {
                    *line += 1;
                }
            }
            '\n' => {
                *line += 1;
                segment.push(ch);
            }
            '\r' => {
                *line += 1;
                segment.push(ch);
            }
            _ => segment.push(ch),
        }
    }

    Err(LexError {
        message: "unterminated string literal".to_string(),
        position,
    })
}

fn decode_string_segment(raw: &str, position: usize) -> Result<String, LexError> {
    let quoted = format!("\"{raw}\"");
    serde_json::from_str::<String>(&quoted).map_err(|e| {
        let rendered = e.to_string();
        let lower = rendered.to_ascii_lowercase();
        let message = if lower.contains("invalid escape") {
            "invalid string literal: Invalid escape".to_string()
        } else {
            format!("invalid string literal: {rendered}")
        };
        LexError { message, position }
    })
}

fn consume_interpolation_tokens(
    chars: &mut Peekable<CharIndices<'_>>,
    position: usize,
    line: &mut usize,
) -> Result<Vec<Token>, LexError> {
    let interp_start_line = *line;
    let mut depth = 1usize;
    let mut raw = String::new();

    while let Some((_, ch)) = chars.next() {
        match ch {
            '"' => {
                raw.push('"');
                consume_interpolation_string_literal(chars, &mut raw, position, line)?;
            }
            '#' => {
                raw.push('#');
                consume_interpolation_comment(chars, &mut raw, line);
            }
            '(' => {
                depth += 1;
                raw.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    let mut tokens = lex_with_start_line(&raw, interp_start_line)?;
                    if matches!(tokens.last(), Some(Token::End)) {
                        tokens.pop();
                    }
                    return Ok(tokens);
                }
                raw.push(ch);
            }
            '\n' => {
                *line += 1;
                raw.push(ch);
            }
            '\r' => {
                *line += 1;
                raw.push(ch);
            }
            _ => raw.push(ch),
        }
    }

    Err(LexError {
        message: "unterminated string interpolation".to_string(),
        position,
    })
}

fn consume_interpolation_string_literal(
    chars: &mut Peekable<CharIndices<'_>>,
    raw: &mut String,
    position: usize,
    line: &mut usize,
) -> Result<(), LexError> {
    let mut escaped = false;
    for (_, ch) in chars.by_ref() {
        raw.push(ch);
        if ch == '\n' || ch == '\r' {
            *line += 1;
        }
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == '"' {
            return Ok(());
        }
    }
    Err(LexError {
        message: "unterminated string literal in interpolation".to_string(),
        position,
    })
}

fn consume_interpolation_comment(
    chars: &mut Peekable<CharIndices<'_>>,
    raw: &mut String,
    line: &mut usize,
) {
    while let Some((_, c)) = chars.next() {
        raw.push(c);
        if c == '\\' {
            if chars.next_if(|(_, next)| *next == '\n').is_some() {
                raw.push('\n');
                *line += 1;
                continue;
            }
            if chars.next_if(|(_, next)| *next == '\r').is_some() {
                raw.push('\r');
                if chars.next_if(|(_, next)| *next == '\n').is_some() {
                    raw.push('\n');
                }
                *line += 1;
                continue;
            }
        }
        if c == '\n' {
            *line += 1;
            return;
        }
        if c == '\r' {
            if chars.next_if(|(_, next)| *next == '\n').is_some() {
                raw.push('\n');
            }
            *line += 1;
            return;
        }
    }
}

fn try_consume_loc_keyword(chars: &mut Peekable<CharIndices<'_>>) -> bool {
    const LOC_TEXT: &str = "__loc__";
    let mut probe = chars.clone();
    for expected in LOC_TEXT.chars() {
        match probe.next() {
            Some((_, next)) if next == expected => {}
            _ => return false,
        }
    }

    if let Some((_, next)) = probe.peek().copied() {
        // Keep jq lexer longest-match behavior for BINDING:
        // "$__loc__x" must lex as a variable, not as LOC token.
        if next == '_' || next.is_ascii_alphanumeric() || next == ':' {
            return false;
        }
    }

    for _ in LOC_TEXT.chars() {
        let _ = chars.next();
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lexes_namespace_identifier_tokens_like_jq() {
        let tokens = lex(r#"import "m" as mod; mod::f"#).expect("lex");
        assert_eq!(
            tokens,
            vec![
                Token::ImportKw,
                Token::Str("m".to_string()),
                Token::AsKw,
                Token::Ident("mod".to_string()),
                Token::Semi,
                Token::Ident("mod::f".to_string()),
                Token::End,
            ]
        );
    }

    #[test]
    fn lexes_namespace_binding_tokens_like_jq() {
        let tokens = lex(r#"$mod::value"#).expect("lex");
        assert_eq!(
            tokens,
            vec![Token::Binding("mod::value".to_string()), Token::End]
        );
    }
}
