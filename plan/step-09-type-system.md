# Step 09 - Type System & Data Encoding

## Goal
Implement comprehensive PostgreSQL type mapping between Rust types and PostgreSQL types, supporting both text and binary encoding formats.

## Context
PostgreSQL has a rich type system identified by OIDs (Object Identifiers). Data can be transferred in text format (human-readable strings) or binary format (PostgreSQL's internal representation). The extended query protocol supports binary; simple query always uses text.

## Tasks

### 9.1 - Type OID registry
```rust
pub mod oid {
    pub const BOOL: Oid = 16;
    pub const BYTEA: Oid = 17;
    pub const CHAR: Oid = 18;
    pub const INT8: Oid = 20;
    pub const INT2: Oid = 21;
    pub const INT4: Oid = 23;
    pub const TEXT: Oid = 25;
    pub const OID: Oid = 26;
    pub const JSON: Oid = 114;
    pub const FLOAT4: Oid = 700;
    pub const FLOAT8: Oid = 701;
    pub const VARCHAR: Oid = 1043;
    pub const DATE: Oid = 1082;
    pub const TIME: Oid = 1083;
    pub const TIMESTAMP: Oid = 1114;
    pub const TIMESTAMPTZ: Oid = 1184;
    pub const INTERVAL: Oid = 1186;
    pub const NUMERIC: Oid = 1700;
    pub const UUID: Oid = 2950;
    pub const JSONB: Oid = 3802;

    // Array types
    pub const BOOL_ARRAY: Oid = 1000;
    pub const INT2_ARRAY: Oid = 1005;
    pub const INT4_ARRAY: Oid = 1007;
    pub const INT8_ARRAY: Oid = 1016;
    pub const TEXT_ARRAY: Oid = 1009;
    pub const FLOAT4_ARRAY: Oid = 1021;
    pub const FLOAT8_ARRAY: Oid = 1022;
    pub const VARCHAR_ARRAY: Oid = 1015;
    pub const UUID_ARRAY: Oid = 2951;
}
```

### 9.2 - ToSql trait (Rust → PostgreSQL)
```rust
pub trait ToSql {
    /// Encode this value as PostgreSQL binary format
    fn to_sql(&self) -> Result<Option<Vec<u8>>, PgError>;

    /// Return the PostgreSQL OID for this type (for type inference)
    fn type_oid(&self) -> Oid;
}

// Implementations for Rust standard types:
impl ToSql for bool { ... }         // OID 16, binary: 1 byte
impl ToSql for i16 { ... }          // OID 21, binary: 2 bytes BE
impl ToSql for i32 { ... }          // OID 23, binary: 4 bytes BE
impl ToSql for i64 { ... }          // OID 20, binary: 8 bytes BE
impl ToSql for f32 { ... }          // OID 700, binary: 4 bytes IEEE 754 BE
impl ToSql for f64 { ... }          // OID 701, binary: 8 bytes IEEE 754 BE
impl ToSql for &str { ... }         // OID 25 (text), binary: UTF-8 bytes
impl ToSql for String { ... }       // OID 25
impl ToSql for &[u8] { ... }        // OID 17 (bytea), binary: raw bytes
impl ToSql for Vec<u8> { ... }      // OID 17

// Option<T> maps to SQL NULL
impl<T: ToSql> ToSql for Option<T> {
    fn to_sql(&self) -> Result<Option<Vec<u8>>, PgError> {
        match self {
            Some(v) => v.to_sql(),
            None => Ok(None),  // NULL
        }
    }
}
```

### 9.3 - FromSql trait (PostgreSQL → Rust)
```rust
pub trait FromSql: Sized {
    /// Decode from PostgreSQL binary format
    fn from_sql(oid: Oid, raw: &[u8]) -> Result<Self, PgError>;

    /// Decode from PostgreSQL text format
    fn from_sql_text(oid: Oid, raw: &[u8]) -> Result<Self, PgError>;

    /// Handle SQL NULL
    fn from_sql_null() -> Result<Self, PgError> {
        Err(PgError::UnexpectedNull)
    }
}

// Option<T> handles NULL gracefully
impl<T: FromSql> FromSql for Option<T> {
    fn from_sql_null() -> Result<Self, PgError> {
        Ok(None)
    }
    fn from_sql(oid: Oid, raw: &[u8]) -> Result<Self, PgError> {
        T::from_sql(oid, raw).map(Some)
    }
}
```

### 9.4 - Type implementations

#### Numeric types
| Rust Type | PG Type | Binary Encoding |
|-----------|---------|-----------------|
| `bool` | BOOL | 1 byte: 0/1 |
| `i16` | INT2 | 2 bytes big-endian |
| `i32` | INT4 | 4 bytes big-endian |
| `i64` | INT8 | 8 bytes big-endian |
| `f32` | FLOAT4 | 4 bytes IEEE 754 BE |
| `f64` | FLOAT8 | 8 bytes IEEE 754 BE |

#### String types
| Rust Type | PG Type | Binary Encoding |
|-----------|---------|-----------------|
| `String` / `&str` | TEXT, VARCHAR, CHAR, NAME | UTF-8 bytes |
| `Vec<u8>` / `&[u8]` | BYTEA | raw bytes |

#### Date/Time types (custom structs - no chrono dependency)
```rust
/// PostgreSQL DATE: days since 2000-01-01
pub struct PgDate(pub i32);

/// PostgreSQL TIME: microseconds since midnight
pub struct PgTime(pub i64);

/// PostgreSQL TIMESTAMP: microseconds since 2000-01-01 00:00:00
pub struct PgTimestamp(pub i64);

/// PostgreSQL TIMESTAMPTZ: same as TIMESTAMP but in UTC
pub struct PgTimestampTz(pub i64);

/// PostgreSQL INTERVAL
pub struct PgInterval {
    pub microseconds: i64,
    pub days: i32,
    pub months: i32,
}
```

These provide raw access. Users can convert to/from their preferred date library.

#### UUID
```rust
pub struct PgUuid(pub [u8; 16]);

impl PgUuid {
    pub fn from_str(s: &str) -> Result<Self, PgError>;
    pub fn to_string(&self) -> String;  // hyphenated format
}
```

#### JSON / JSONB
```rust
/// Raw JSON value (stored as string)
pub struct PgJson(pub String);

/// Raw JSONB value (stored as bytes with version prefix)
pub struct PgJsonb(pub Vec<u8>);
```

#### NUMERIC (arbitrary precision)
```rust
/// PostgreSQL NUMERIC - stored as string to avoid precision loss
pub struct PgNumeric(pub String);

// Binary format: sign(u16) + weight(i16) + dscale(u16) + ndigits(u16) + digits(u16[])
// Text format is simpler and safer for arbitrary precision
```

#### Arrays
```rust
pub struct PgArray<T: ToSql> {
    pub values: Vec<Option<T>>,
    pub element_oid: Oid,
}

impl<T: ToSql> ToSql for PgArray<T> { ... }
impl<T: FromSql> FromSql for Vec<Option<T>> { ... }
```

### 9.5 - Text format decoders
For simple query protocol (always text format):
```rust
impl FromSql for i32 {
    fn from_sql_text(_oid: Oid, raw: &[u8]) -> Result<Self, PgError> {
        let s = std::str::from_utf8(raw)?;
        s.parse::<i32>().map_err(|e| PgError::TypeConversion(e.to_string()))
    }
}
```

### 9.6 - Type registry for custom types
```rust
pub struct TypeRegistry {
    /// Map from OID to type info
    types: HashMap<Oid, TypeInfo>,
}

pub struct TypeInfo {
    pub oid: Oid,
    pub name: String,
    pub kind: TypeKind,
}

pub enum TypeKind {
    Simple,
    Array { element_oid: Oid },
    Range { element_oid: Oid },
    Enum { variants: Vec<String> },
    Composite { fields: Vec<(String, Oid)> },
    Domain { base_oid: Oid },
}
```

### 9.7 - Feature-gated optional type integrations
Behind feature flags, provide optional conversions:
```toml
[features]
uuid = ["dep:uuid"]       # uuid crate integration
serde_json = ["dep:serde_json"]  # serde_json::Value for JSON/JSONB
```

## File Layout
```
crates/pg-types/src/
├── lib.rs
├── oid.rs            (OID constants)
├── to_sql.rs         (ToSql trait + impls)
├── from_sql.rs       (FromSql trait + impls)
├── numeric.rs        (integer, float encoding/decoding)
├── text.rs           (String, &str)
├── bytea.rs          (binary data)
├── datetime.rs       (PgDate, PgTime, PgTimestamp, etc.)
├── uuid.rs           (PgUuid)
├── json.rs           (PgJson, PgJsonb)
├── pg_numeric.rs     (PgNumeric - arbitrary precision)
├── array.rs          (PgArray)
├── registry.rs       (TypeRegistry for custom types)
└── error.rs
```

## Acceptance Criteria
- [ ] All standard Rust types map to PostgreSQL types correctly
- [ ] Binary encoding/decoding for all supported types
- [ ] Text encoding/decoding for all supported types (simple query)
- [ ] NULL handling via `Option<T>`
- [ ] Date/time types with raw access (no chrono dependency)
- [ ] UUID support
- [ ] JSON/JSONB support
- [ ] NUMERIC arbitrary precision
- [ ] Array types
- [ ] Custom type registry for enums, composites, domains
- [ ] Compiles for wasm32-wasip2

## Testing
- Round-trip test for every type: Rust → PG binary → Rust
- Text format parsing for every type
- Edge cases: MAX/MIN values, NaN, Infinity for floats
- NULL handling
- Array with NULLs
- Large NUMERIC values
- Unicode strings
- Empty bytea, empty text
