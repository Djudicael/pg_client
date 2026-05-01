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
