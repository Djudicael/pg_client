#[cfg(test)]
mod tests {
    use crate::{Format, FromSql, ToSql, Type};

    #[test]
    fn test_bool_text() {
        let ty = Type::BOOL;
        assert_eq!(bool::from_sql(&ty, Some(b"t"), Format::Text).unwrap(), true);
        assert_eq!(
            bool::from_sql(&ty, Some(b"f"), Format::Text).unwrap(),
            false
        );
        assert_eq!(
            bool::from_sql(&ty, Some(b"true"), Format::Text).unwrap(),
            true
        );
        assert_eq!(
            bool::from_sql(&ty, Some(b"FALSE"), Format::Text).unwrap(),
            false
        );
    }

    #[test]
    fn test_bool_binary() {
        let ty = Type::BOOL;
        assert_eq!(
            bool::from_sql(&ty, Some(&[1]), Format::Binary).unwrap(),
            true
        );
        assert_eq!(
            bool::from_sql(&ty, Some(&[0]), Format::Binary).unwrap(),
            false
        );
    }

    #[test]
    fn test_i32_text() {
        let ty = Type::INT4;
        assert_eq!(i32::from_sql(&ty, Some(b"-42"), Format::Text).unwrap(), -42);
        assert_eq!(i32::from_sql(&ty, Some(b"0"), Format::Text).unwrap(), 0);
        assert_eq!(
            i32::from_sql(&ty, Some(b"2147483647"), Format::Text).unwrap(),
            i32::MAX
        );
    }

    #[test]
    fn test_i32_binary() {
        let ty = Type::INT4;
        assert_eq!(
            i32::from_sql(&ty, Some(&42i32.to_be_bytes()), Format::Binary).unwrap(),
            42
        );
        assert_eq!(
            i32::from_sql(&ty, Some(&(-1i32).to_be_bytes()), Format::Binary).unwrap(),
            -1
        );
    }

    #[test]
    fn test_i64_text() {
        let ty = Type::INT8;
        assert_eq!(
            i64::from_sql(&ty, Some(b"9223372036854775807"), Format::Text).unwrap(),
            i64::MAX
        );
    }

    #[test]
    fn test_i64_binary() {
        let ty = Type::INT8;
        assert_eq!(
            i64::from_sql(&ty, Some(&123456789i64.to_be_bytes()), Format::Binary).unwrap(),
            123456789
        );
    }

    #[test]
    fn test_f64_text() {
        let ty = Type::FLOAT8;
        assert!((f64::from_sql(&ty, Some(b"3.14"), Format::Text).unwrap() - 3.14).abs() < 0.001);
    }

    #[test]
    fn test_f64_binary() {
        let ty = Type::FLOAT8;
        assert_eq!(
            f64::from_sql(&ty, Some(&2.718f64.to_be_bytes()), Format::Binary).unwrap(),
            2.718
        );
    }

    #[test]
    fn test_string_text() {
        let ty = Type::TEXT;
        assert_eq!(
            String::from_sql(&ty, Some(b"hello world"), Format::Text).unwrap(),
            "hello world"
        );
    }

    #[test]
    #[cfg(feature = "uuid")]
    fn test_uuid_text() {
        use uuid::Uuid;
        let ty = Type::UUID;
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let uuid = Uuid::from_sql(&ty, Some(uuid_str.as_bytes()), Format::Text).unwrap();
        assert_eq!(uuid.to_string(), uuid_str);
    }

    #[test]
    #[cfg(feature = "uuid")]
    fn test_uuid_binary() {
        use uuid::Uuid;
        let ty = Type::UUID;
        let bytes = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let uuid = Uuid::from_sql(&ty, Some(&bytes), Format::Binary).unwrap();
        assert_eq!(uuid.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    #[cfg(feature = "uuid")]
    fn test_uuid_binary_roundtrip() {
        use uuid::Uuid;
        let ty = Type::UUID;
        let original = Uuid::parse_str("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8").unwrap();

        // Encode as binary
        let mut buf = Vec::new();
        original.to_sql(&ty, &mut buf, Format::Binary).unwrap();
        assert_eq!(buf.len(), 16);

        // Decode back
        let decoded = Uuid::from_sql(&ty, Some(&buf), Format::Binary).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    #[cfg(feature = "serde-json")]
    fn test_jsonb_binary() {
        use serde_json::json;
        let ty = Type::JSONB;
        // JSONB binary: 0x01 version header + JSON text
        let bytes = b"\x01{\"key\": 42}";
        let value = serde_json::Value::from_sql(&ty, Some(bytes), Format::Binary).unwrap();
        assert_eq!(value["key"], json!(42));
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_datetime_text() {
        use chrono::{DateTime, Utc};
        let ty = Type::TIMESTAMPTZ;
        let dt_str = "2024-01-15T10:30:00+00:00";
        let dt = DateTime::<Utc>::from_sql(&ty, Some(dt_str.as_bytes()), Format::Text).unwrap();
        assert_eq!(dt.to_rfc3339(), dt_str);
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_datetime_binary() {
        use chrono::{DateTime, NaiveDate, TimeZone, Utc};
        let ty = Type::TIMESTAMPTZ;

        // PostgreSQL epoch: 2000-01-01 00:00:00 UTC
        let pg_epoch = NaiveDate::from_ymd_opt(2000, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        // 2024-01-15 10:30:00 UTC = epoch + 24 years + 10h 30m
        let target = Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(10, 30, 0)
                .unwrap(),
        );
        let usec = target
            .naive_utc()
            .signed_duration_since(pg_epoch)
            .num_microseconds()
            .unwrap();

        let bytes = usec.to_be_bytes();
        let dt = DateTime::<Utc>::from_sql(&ty, Some(&bytes), Format::Binary).unwrap();
        assert_eq!(dt, target);
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_datetime_binary_roundtrip() {
        use chrono::{DateTime, NaiveDate, TimeZone, Utc};
        let ty = Type::TIMESTAMPTZ;
        let original = Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(2024, 6, 15)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap(),
        );

        // Encode as binary
        let mut buf = Vec::new();
        original.to_sql(&ty, &mut buf, Format::Binary).unwrap();
        assert_eq!(buf.len(), 8);

        // Decode back
        let decoded = DateTime::<Utc>::from_sql(&ty, Some(&buf), Format::Binary).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_option_some() {
        let ty = Type::INT4;
        assert_eq!(
            Option::<i32>::from_sql(&ty, Some(b"7"), Format::Text).unwrap(),
            Some(7)
        );
    }

    #[test]
    fn test_option_none() {
        let ty = Type::INT4;
        assert_eq!(
            Option::<i32>::from_sql(&ty, None, Format::Text).unwrap(),
            None
        );
    }

    #[test]
    fn test_null_non_optional_fails() {
        let ty = Type::INT4;
        assert!(i32::from_sql(&ty, None, Format::Text).is_err());
    }

    // -----------------------------------------------------------------------
    // ToSql round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn test_i32_to_sql_text() {
        let ty = Type::INT4;
        let mut buf = Vec::new();
        42i32.to_sql(&ty, &mut buf, Format::Text).unwrap();
        assert_eq!(&buf, b"42");
    }

    #[test]
    fn test_i32_to_sql_binary() {
        let ty = Type::INT4;
        let mut buf = Vec::new();
        42i32.to_sql(&ty, &mut buf, Format::Binary).unwrap();
        assert_eq!(&buf, &[0, 0, 0, 42]);
    }

    #[test]
    fn test_bool_to_sql_text() {
        let ty = Type::BOOL;
        let mut buf = Vec::new();
        true.to_sql(&ty, &mut buf, Format::Text).unwrap();
        assert_eq!(&buf, b"t");
    }

    #[test]
    fn test_string_to_sql() {
        let ty = Type::TEXT;
        let mut buf = Vec::new();
        "hello"
            .to_string()
            .to_sql(&ty, &mut buf, Format::Text)
            .unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn test_vec_u8_to_sql_text_hex() {
        let ty = Type::BYTEA;
        let mut buf = Vec::new();
        vec![0xDEu8, 0xAD, 0xBE, 0xEF]
            .to_sql(&ty, &mut buf, Format::Text)
            .unwrap();
        assert_eq!(&buf, b"\\xDEADBEEF");
    }

    #[test]
    fn test_vec_u8_to_sql_binary() {
        let ty = Type::BYTEA;
        let mut buf = Vec::new();
        vec![1u8, 2, 3]
            .to_sql(&ty, &mut buf, Format::Binary)
            .unwrap();
        assert_eq!(&buf, &[1, 2, 3]);
    }

    #[test]
    fn test_option_to_sql_some() {
        let ty = Type::INT4;
        let mut buf = Vec::new();
        Some(7i32).to_sql(&ty, &mut buf, Format::Text).unwrap();
        assert_eq!(&buf, b"7");
    }

    #[test]
    fn test_option_to_sql_none() {
        let ty = Type::INT4;
        let mut buf = Vec::new();
        None::<i32>.to_sql(&ty, &mut buf, Format::Text).unwrap();
        assert!(buf.is_empty());
    }

    // ========================================================================
    // Property-based tests (proptest)
    // ========================================================================

    proptest::proptest! {
        #[test]
        fn proptest_i32_roundtrip(val in proptest::arbitrary::any::<i32>()) {
            let ty = Type::INT4;
            let mut buf = Vec::new();
            val.to_sql(&ty, &mut buf, Format::Binary).unwrap();
            let decoded = i32::from_sql(&ty, Some(&buf), Format::Binary).unwrap();
            assert_eq!(val, decoded);
        }

        #[test]
        fn proptest_i64_roundtrip(val in proptest::arbitrary::any::<i64>()) {
            let ty = Type::INT8;
            let mut buf = Vec::new();
            val.to_sql(&ty, &mut buf, Format::Binary).unwrap();
            let decoded = i64::from_sql(&ty, Some(&buf), Format::Binary).unwrap();
            assert_eq!(val, decoded);
        }

        #[test]
        fn proptest_f64_roundtrip(val in proptest::arbitrary::any::<f64>()) {
            let ty = Type::FLOAT8;
            if val.is_nan() {
                let mut buf = Vec::new();
                val.to_sql(&ty, &mut buf, Format::Binary).unwrap();
                let decoded = f64::from_sql(&ty, Some(&buf), Format::Binary).unwrap();
                assert!(decoded.is_nan());
            } else if !val.is_infinite() {
                let mut buf = Vec::new();
                val.to_sql(&ty, &mut buf, Format::Binary).unwrap();
                let decoded = f64::from_sql(&ty, Some(&buf), Format::Binary).unwrap();
                assert_eq!(val, decoded);
            }
        }

        #[test]
        fn proptest_string_roundtrip(s in "\\PC*") {
            let ty = Type::TEXT;
            let mut buf = Vec::new();
            s.to_sql(&ty, &mut buf, Format::Text).unwrap();
            let decoded = String::from_sql(&ty, Some(&buf), Format::Text).unwrap();
            assert_eq!(s, decoded);
        }

        #[test]
        fn proptest_option_i32_roundtrip(val in proptest::arbitrary::any::<Option<i32>>()) {
            let ty = Type::INT4;
            match val {
                Some(v) => {
                    let mut buf = Vec::new();
                    v.to_sql(&ty, &mut buf, Format::Binary).unwrap();
                    let decoded: Option<i32> = Option::from_sql(&ty, Some(&buf), Format::Binary).unwrap();
                    assert_eq!(Some(v), decoded);
                }
                None => {
                    let decoded: Option<i32> = Option::from_sql(&ty, None, Format::Binary).unwrap();
                    assert!(decoded.is_none());
                }
            }
        }

        #[test]
        fn proptest_bool_roundtrip(val in proptest::arbitrary::any::<bool>()) {
            let ty = Type::BOOL;
            // Text format
            let mut buf = Vec::new();
            val.to_sql(&ty, &mut buf, Format::Text).unwrap();
            let decoded = bool::from_sql(&ty, Some(&buf), Format::Text).unwrap();
            assert_eq!(val, decoded);
            // Binary format
            let mut buf = Vec::new();
            val.to_sql(&ty, &mut buf, Format::Binary).unwrap();
            let decoded = bool::from_sql(&ty, Some(&buf), Format::Binary).unwrap();
            assert_eq!(val, decoded);
        }
    }
}
