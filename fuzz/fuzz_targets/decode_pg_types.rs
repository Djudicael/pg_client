#![no_main]

use libfuzzer_sys::fuzz_target;
use pg_types::{Format, FromSql, Type};

fn pick_type(tag: u8) -> Type {
    match tag % 8 {
        0 => Type::BOOL,
        1 => Type::INT2,
        2 => Type::INT4,
        3 => Type::INT8,
        4 => Type::FLOAT4,
        5 => Type::FLOAT8,
        6 => Type::TEXT,
        _ => Type::BYTEA,
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }

    let ty = pick_type(data[0]);
    let format = if data[1] & 1 == 0 {
        Format::Text
    } else {
        Format::Binary
    };
    let raw = Some(&data[2..]);

    let _ = bool::from_sql(&ty, raw, format);
    let _ = i16::from_sql(&ty, raw, format);
    let _ = i32::from_sql(&ty, raw, format);
    let _ = i64::from_sql(&ty, raw, format);
    let _ = f32::from_sql(&ty, raw, format);
    let _ = f64::from_sql(&ty, raw, format);
    let _ = String::from_sql(&ty, raw, format);
    let _ = Vec::<u8>::from_sql(&ty, raw, format);
    let _ = Option::<String>::from_sql(&ty, raw, format);
});
