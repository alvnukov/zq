// c-ref: C pointer/string adapters used by libyaml interop.
// moved-from: src/yamlmerge.rs::scalar_string
// moved-from: src/yamlmerge.rs::parser_error

use unsafe_libyaml::{
    yaml_event_delete, yaml_event_t, yaml_event_type_t, yaml_parser_delete, yaml_parser_initialize,
    yaml_parser_parse, yaml_parser_set_input_string, yaml_parser_t, yaml_scalar_style_t,
};

pub(crate) unsafe fn scalar_utf8_from_ptr(ptr: *const u8, len: usize) -> String {
    if ptr.is_null() || len == 0 {
        return String::new();
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    String::from_utf8_lossy(bytes).into_owned()
}

pub(crate) unsafe fn c_error_message(problem: *const std::ffi::c_char, fallback: &str) -> String {
    if problem.is_null() {
        return fallback.to_string();
    }
    unsafe { std::ffi::CStr::from_ptr(problem) }
        .to_string_lossy()
        .into_owned()
}

fn parser_error(parser: &yaml_parser_t) -> String {
    let problem = unsafe {
        c_error_message(
            parser.problem.cast::<std::ffi::c_char>(),
            "yaml parse error",
        )
    };
    format!(
        "{} at line {} column {}",
        problem,
        parser.problem_mark.line + 1,
        parser.problem_mark.column + 1
    )
}

// c-ref: libyaml parser lifecycle wrapper (initialize/set_input/parse/delete).
// moved-from: src/yamlmerge.rs::collect_merge_style_hints
pub(crate) struct Parser {
    raw: yaml_parser_t,
}

impl Parser {
    pub(crate) fn from_str(input: &str) -> Result<Self, String> {
        // FFI POD from C; zero-init matches libyaml usage pattern.
        let mut raw = unsafe { std::mem::MaybeUninit::<yaml_parser_t>::zeroed().assume_init() };
        if !unsafe { yaml_parser_initialize(&mut raw).ok } {
            return Err("yaml parser init failed".to_string());
        }
        unsafe { yaml_parser_set_input_string(&mut raw, input.as_ptr(), input.len() as u64) };
        Ok(Self { raw })
    }

    pub(crate) fn parse_event(&mut self) -> Result<Event, String> {
        // FFI POD from C; zero-init matches libyaml usage pattern.
        let mut raw = unsafe { std::mem::MaybeUninit::<yaml_event_t>::zeroed().assume_init() };
        if !unsafe { yaml_parser_parse(&mut self.raw, &mut raw).ok } {
            return Err(parser_error(&self.raw));
        }
        Ok(Event { raw })
    }
}

impl Drop for Parser {
    fn drop(&mut self) {
        unsafe { yaml_parser_delete(&mut self.raw) };
    }
}

// c-ref: libyaml event lifecycle wrapper (parse/delete).
// moved-from: src/yamlmerge.rs::collect_merge_style_hints
pub(crate) struct Event {
    raw: yaml_event_t,
}

impl Event {
    pub(crate) fn event_type(&self) -> yaml_event_type_t {
        self.raw.type_
    }

    pub(crate) fn scalar_string(&self) -> String {
        let (ptr, len) = unsafe {
            (
                self.raw.data.scalar.value.cast::<u8>(),
                self.raw.data.scalar.length as usize,
            )
        };
        unsafe { scalar_utf8_from_ptr(ptr, len) }
    }

    pub(crate) fn scalar_style(&self) -> yaml_scalar_style_t {
        unsafe { self.raw.data.scalar.style }
    }
}

impl Drop for Event {
    fn drop(&mut self) {
        unsafe { yaml_event_delete(&mut self.raw) };
    }
}
