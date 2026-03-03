//! Helpers to interface with Oniguruma regex semantics used by jq.

use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use bstr::ByteSlice;
use onig::{Regex, RegexOptions, SearchOptions, Syntax};

#[derive(Copy, Clone, Default)]
pub struct Flags {
    // global search
    g: bool,
    // ignore empty matches
    n: bool,
    // case-insensitive
    i: bool,
    // multi-line mode: ^ and $ match begin/end of line
    m: bool,
    // single-line mode: allow . to match \n
    s: bool,
    // greedy
    l: bool,
    // extended mode: ignore whitespace and allow line comments (starting with `#`)
    x: bool,
}

impl Flags {
    pub fn new(flags: &str) -> Result<Self, char> {
        let mut out = Self::default();
        for flag in flags.chars() {
            match flag {
                'g' => out.g = true,
                'n' => out.n = true,
                'i' => out.i = true,
                'm' => out.m = true,
                's' => out.s = true,
                'l' => out.l = true,
                'x' => out.x = true,
                'p' => {
                    out.m = true;
                    out.s = true;
                }
                c => return Err(c),
            }
        }
        Ok(out)
    }

    pub fn global(self) -> bool {
        self.g
    }

    pub fn regex(self, re: &str) -> Result<Regex, onig::Error> {
        let mut options = RegexOptions::REGEX_OPTION_CAPTURE_GROUP;
        if self.i {
            options |= RegexOptions::REGEX_OPTION_IGNORECASE;
        }
        if self.x {
            options |= RegexOptions::REGEX_OPTION_EXTEND;
        }
        if self.m {
            options |= RegexOptions::REGEX_OPTION_MULTILINE;
        }
        if self.s {
            options |= RegexOptions::REGEX_OPTION_SINGLELINE;
        }
        if self.l {
            options |= RegexOptions::REGEX_OPTION_FIND_LONGEST;
        }
        if self.n {
            options |= RegexOptions::REGEX_OPTION_FIND_NOT_EMPTY;
        }
        Regex::with_options(re, options, Syntax::perl_ng())
    }
}

pub struct Match<B> {
    pub offset: isize,
    pub length: usize,
    pub string: B,
    pub name: Option<String>,
}

impl<'a> Match<&'a [u8]> {
    pub fn fields<T: From<isize> + From<String> + 'a>(
        &self,
        f: impl Fn(&'a [u8]) -> T,
    ) -> impl Iterator<Item = (T, T)> + '_ {
        [
            ("offset", self.offset.into()),
            ("length", (self.length as isize).into()),
            ("string", f(self.string)),
        ]
        .into_iter()
        .chain(
            self.name
                .as_ref()
                .map(|n| ("name", n.clone().into()))
                .into_iter(),
        )
        .map(|(k, v)| (k.to_string().into(), v))
    }
}

pub enum Part<B> {
    Matches(Vec<Match<B>>),
    Mismatch(B),
}

fn utf8_char_offset(s: &[u8], byte_offset: usize) -> usize {
    s[..byte_offset].chars().count()
}

fn utf8_char_len(s: &[u8], start: usize, end: usize) -> usize {
    s[start..end].chars().count()
}

fn advance_one_char_boundary(text: &str, byte_offset: usize) -> usize {
    if byte_offset >= text.len() {
        return text.len().saturating_add(1);
    }
    let step = text[byte_offset..]
        .chars()
        .next()
        .map(|c| c.len_utf8())
        .unwrap_or(1);
    byte_offset.saturating_add(step)
}

fn capture_name_by_group(re: &Regex) -> Vec<Option<String>> {
    let mut names = vec![None; re.captures_len().saturating_add(1)];
    re.foreach_name(|name, groups| {
        for g in groups {
            let idx = *g as usize;
            if idx < names.len() {
                names[idx] = Some(name.to_string());
            }
        }
        true
    });
    names
}

fn build_zero_width_match<'a>(
    s: &'a [u8],
    region: &onig::Region,
    name_by_group: &[Option<String>],
    byte_offset: usize,
) -> Vec<Match<&'a [u8]>> {
    let idx = utf8_char_offset(s, byte_offset) as isize;
    let mut out = Vec::with_capacity(region.len());
    for i in 0..region.len() {
        out.push(Match {
            offset: idx,
            length: 0,
            string: &s[byte_offset..byte_offset],
            name: if i == 0 {
                None
            } else {
                name_by_group.get(i).cloned().flatten()
            },
        });
    }
    out
}

fn build_non_zero_width_match<'a>(
    s: &'a [u8],
    region: &onig::Region,
    name_by_group: &[Option<String>],
) -> Vec<Match<&'a [u8]>> {
    let mut out = Vec::with_capacity(region.len());
    for i in 0..region.len() {
        let name = if i == 0 {
            None
        } else {
            name_by_group.get(i).cloned().flatten()
        };
        let Some((beg, end)) = region.pos(i) else {
            out.push(Match {
                offset: -1,
                length: 0,
                string: &s[0..0],
                name,
            });
            continue;
        };
        out.push(Match {
            offset: utf8_char_offset(s, beg) as isize,
            length: utf8_char_len(s, beg, end),
            string: &s[beg..end],
            name,
        });
    }
    out
}

/// Apply a regular expression to the given input value.
///
/// `sm` indicates whether to
/// 1. output strings that do *not* match the regex, and
/// 2. output the matches.
pub fn regex<'a>(s: &'a [u8], re: &'a Regex, flags: Flags, sm: (bool, bool)) -> Vec<Part<&'a [u8]>> {
    // mismatches & matches
    let (mi, ma) = sm;

    let mut out = Vec::new();
    let mut last_byte = 0usize;
    let mut start_byte = 0usize;
    let end_byte = s.len();
    let mut region = onig::Region::new();
    let name_by_group = capture_name_by_group(re);

    let Ok(text) = core::str::from_utf8(s) else {
        if mi {
            out.push(Part::Mismatch(s));
        }
        return out;
    };

    while start_byte <= end_byte {
        region.clear();
        let Some(_) = re.search_with_options(
            text,
            start_byte,
            end_byte,
            SearchOptions::SEARCH_OPTION_NONE,
            Some(&mut region),
        ) else {
            break;
        };

        let Some((beg0, end0)) = region.pos(0) else {
            break;
        };

        if mi {
            out.push(Part::Mismatch(&s[last_byte..beg0]));
            last_byte = end0;
        }

        if ma {
            if end0 == beg0 {
                out.push(Part::Matches(build_zero_width_match(
                    s,
                    &region,
                    &name_by_group,
                    beg0,
                )));
            } else {
                out.push(Part::Matches(build_non_zero_width_match(
                    s,
                    &region,
                    &name_by_group,
                )));
            }
        }

        if !flags.global() {
            break;
        }

        // Keep jq behavior for zero-width global matches: advance to avoid infinite loops.
        start_byte = if end0 == beg0 {
            advance_one_char_boundary(text, end0)
        } else {
            end0
        };
    }

    if mi {
        out.push(Part::Mismatch(&s[last_byte..]));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::{regex, Flags, Part};

    fn as_matches(parts: Vec<Part<&[u8]>>) -> Vec<Vec<(isize, usize, String)>> {
        parts
            .into_iter()
            .filter_map(|p| match p {
                Part::Matches(ms) => Some(
                    ms.into_iter()
                        .map(|m| (m.offset, m.length, String::from_utf8_lossy(m.string).to_string()))
                        .collect::<Vec<_>>(),
                ),
                Part::Mismatch(_) => None,
            })
            .collect()
    }

    #[test]
    fn onig_zero_width_capture_offsets_match_jq_17() {
        let flags = Flags::new("g").expect("flags");
        let re = flags.regex("( )*").expect("regex");
        let parts = as_matches(regex(b"abc", &re, flags, (false, true)));
        assert_eq!(parts[0][1].0, 0);
        assert_eq!(parts[0][1].1, 0);
        assert_eq!(parts[0][1].2, "");
    }

    #[test]
    fn onig_combining_mark_word_boundary_length() {
        let flags = Flags::new("").expect("flags");
        let re = flags.regex(".+?\\b").expect("regex");
        let input = "a\u{0304} two-codepoint grapheme".as_bytes();
        let parts = as_matches(regex(input, &re, flags, (false, true)));
        assert_eq!(parts[0][0].1, 2);
        assert_eq!(parts[0][0].2, "a\u{0304}");
    }

    #[test]
    fn onig_positive_lookahead_global_matches_once_like_jq() {
        let flags = Flags::new("g").expect("flags");
        let re = flags.regex("(?=u)").expect("regex");
        let parts = as_matches(regex(b"qux", &re, flags, (false, true)));
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0][0].0, 1);
    }

    #[test]
    fn onig_global_includes_terminal_empty_match() {
        let flags = Flags::new("g").expect("flags");
        let re = flags.regex("[^a-z]*(?<x>[a-z]*)").expect("regex");
        let parts = as_matches(regex(b"123foo456bar", &re, flags, (false, true)));
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[2][0], (12, 0, "".to_string()));
        assert_eq!(parts[2][1], (12, 0, "".to_string()));
    }
}
