//! Coercition de types — conversion d'une valeur JSON vers le type `PostgreSQL` attendu.
//!
//! *Coercer* une valeur : tenter de la convertir vers le `PgType` inféré au Pass 1.
//! Ex : la valeur JSON `42.0` est coercée vers `Integer` si le schéma attend un entier ; une
//! chaîne (même `"42"`) ne l'est jamais vers un type numérique : c'est une anomalie.
//! En cas d'échec (valeur incompatible), retourne `CoerceResult::Anomaly` — la valeur
//! est alors enregistrée comme anomalie et remplacée par NULL en base.
//!
//! Fonctions :
//! - enum `CoerceResult` — résultat d'une tentative de coercition (Ok/Null/Anomaly/`OkWithAnomaly`).
//! - fn `coerce` — dispatch principal selon le `PgType` cible (NULL court-circuité en amont).
//! - fn `coerce_pg_array` — sérialise un tableau JSON en littéral `PostgreSQL` `{e1,e2,...}` ; un élément
//!   non convertible devient `NULL`, le premier est rapporté (une anomalie par cellule, #84).
//! - fn `coerce_pg_array_element` — représentation d'un élément dans le littéral, ou la raison de son rejet.
//! - fn `quote_array_text` — élément texte entre guillemets ; octet nul ou dépassement de `VARCHAR(n)` = rejet.
//! - fn `coerce_integer` — coercition vers INTEGER, produit `Ok`/`Anomaly`.
//! - fn `coerce_bigint` — coercition vers BIGINT, produit `Ok`/`Anomaly`.
//! - fn `coerce_float` — coercition vers DOUBLE PRECISION, produit `Ok`/`Anomaly`.
//! - fn `coerce_bool` — coercition vers BOOLEAN, produit `Ok`/`Anomaly`.
//! - fn `coerce_uuid` — coercition vers UUID, produit `Ok`/`Anomaly`.
//! - fn `coerce_date` — coercition vers DATE, produit `Ok`/`Anomaly`.
//! - fn `coerce_timestamp` — coercition vers TIMESTAMP, produit `Ok`/`Anomaly`.
//! - fn `coerce_text` — coercition vers TEXT/VARCHAR/JSONB, produit `Ok`/`Anomaly`.
//! - fn `format_float` — formate un f64 fini en chaîne, `None` si NaN/infini.
//! - fn `json_type_name` — nom du type JSON pour les messages d'anomalie.

use serde_json::Value;

use crate::db::copy_text::{escape_copy_text, CopyEscaped};
use crate::schema::type_tracker::PgType;

/// Result of attempting to coerce a JSON value to a `PgType`.
pub enum CoerceResult {
    /// Successfully converted: a COPY-safe string ready to write to the buffer.
    Ok(CopyEscaped),
    /// Value is SQL NULL
    Null,
    /// Value could not be coerced to the target type
    Anomaly {
        actual_value: String,
        actual_type: &'static str,
    },
    /// PG array written with at least one element replaced by `NULL`. Carries the literal and the
    /// first faulty element: the caller records one anomaly for the whole cell (#84).
    OkWithAnomaly {
        value: CopyEscaped,
        actual_value: String,
        actual_type: &'static str,
    },
}

/// Coerce a JSON value to the COPY text-format representation for the given `PgType`.
/// Returns `None` on anomaly (caller records the anomaly and inserts NULL).
#[must_use]
#[allow(clippy::too_many_lines)] // exhaustive dispatch over all PgType variants
pub fn coerce(value: &Value, pg_type: &PgType) -> CoerceResult {
    if matches!(value, Value::Null) {
        return CoerceResult::Null;
    }

    match pg_type {
        PgType::Integer => coerce_integer(value),
        PgType::BigInt => coerce_bigint(value),
        PgType::DoublePrecision => coerce_float(value),
        PgType::Boolean => coerce_bool(value),
        PgType::Uuid => coerce_uuid(value),
        PgType::Date => coerce_date(value),
        PgType::Timestamp => coerce_timestamp(value),
        PgType::VarChar(max_len) => {
            let result = coerce_text(value);
            // Guard against values longer than the inferred VARCHAR width.
            // Pass 1 may not have observed the longest string in the dataset,
            // so we treat oversized values as anomalies (→ NULL) rather than
            // letting PostgreSQL abort the COPY.
            if let CoerceResult::Ok(ref s) = result {
                // s is COPY-escaped; char count of escaped form is always >= raw form,
                // so use the raw string length for the limit check.
                let raw_len = match value {
                    Value::String(sv) => sv.chars().count(),
                    _ => s.as_str().len(),
                };
                if raw_len > *max_len as usize {
                    return CoerceResult::Anomaly {
                        actual_value: value.to_string(),
                        actual_type: "string_too_long",
                    };
                }
            }
            result
        }
        PgType::Text | PgType::Jsonb => coerce_text(value),
        PgType::Array(elem_type) => match value {
            Value::Array(arr) => coerce_pg_array(arr, elem_type),
            // Mixed scalar/array field (issue #48): a scalar occurrence becomes a one-element array.
            Value::Object(_) => CoerceResult::Anomaly {
                actual_value: value.to_string(),
                actual_type: json_type_name(value),
            },
            scalar => coerce_pg_array(std::slice::from_ref(scalar), elem_type),
        },
    }
}

/// Serialize a JSON array as a `PostgreSQL` array literal for COPY text format.
/// Format: `{elem1,elem2,NULL,elem3}` with COPY-level escaping applied to the whole literal.
/// An element that cannot be stored becomes `NULL`; the first one is reported, so the caller records
/// one anomaly for the cell, not one per element (#84).
fn coerce_pg_array(arr: &[Value], elem_type: &PgType) -> CoerceResult {
    let mut parts = Vec::with_capacity(arr.len());
    let mut first_bad: Option<(String, &'static str)> = None;
    for item in arr {
        if matches!(item, Value::Null) {
            parts.push("NULL".to_string());
            continue;
        }
        match coerce_pg_array_element(item, elem_type) {
            Ok(elem_str) => parts.push(elem_str),
            Err(bad) => {
                parts.push("NULL".to_string());
                first_bad.get_or_insert(bad);
            }
        }
    }
    // Build `{e1,e2,...}` and COPY-escape the whole literal.
    // An element with a null byte was replaced by NULL in coerce_pg_array_element,
    // so escape_copy_text will always return Some here.
    let literal = format!("{{{}}}", parts.join(","));
    let escaped =
        escape_copy_text(&literal).expect("array literal is null-byte-free by construction");
    match first_bad {
        None => CoerceResult::Ok(escaped),
        Some((actual_value, actual_type)) => CoerceResult::OkWithAnomaly {
            value: escaped,
            actual_value,
            actual_type,
        },
    }
}

/// Produce the array-literal representation of one element (no outer COPY escaping yet), or the
/// reason it cannot be stored: the element as JSON text and the anomaly type.
/// Text-like types are double-quoted; numeric/boolean are bare.
fn coerce_pg_array_element(
    value: &Value,
    pg_type: &PgType,
) -> Result<String, (String, &'static str)> {
    match pg_type {
        PgType::Integer | PgType::BigInt | PgType::DoublePrecision | PgType::Boolean => {
            match coerce(value, pg_type) {
                CoerceResult::Ok(s) => Ok(s.as_str().to_owned()), // numeric/bool: ASCII-safe, bare in array literal
                CoerceResult::Anomaly {
                    actual_value,
                    actual_type,
                } => Err((actual_value, actual_type)),
                // out of reach: nulls are filtered by the caller and a scalar type never yields OkWithAnomaly
                CoerceResult::Null | CoerceResult::OkWithAnomaly { .. } => {
                    Err((value.to_string(), json_type_name(value)))
                }
            }
        }
        // Dates, UUIDs and timestamps must be JSON strings, as for their scalar coerce_*
        // (the content of a date is not checked there either).
        PgType::Uuid | PgType::Date | PgType::Timestamp if !matches!(value, Value::String(_)) => {
            Err((value.to_string(), json_type_name(value)))
        }
        PgType::VarChar(max_len) => quote_array_text(value, Some(*max_len as usize)),
        _ => quote_array_text(value, None),
    }
}

/// Double-quote an element for array syntax. `\` and `"` are escaped so the array parser reads them
/// back; the outer `escape_copy_text` then COPY-escapes the whole literal. Numbers, booleans and
/// nested arrays/objects keep their JSON text (#83). A null byte or a `VARCHAR(n)` overflow
/// (`max_chars`) is an anomaly, as for a scalar: `PostgreSQL` would reject the element and abort the COPY.
fn quote_array_text(
    value: &Value,
    max_chars: Option<usize>,
) -> Result<String, (String, &'static str)> {
    let raw = match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let mut out = String::with_capacity(raw.len() + 2);
    out.push('"');
    let mut chars = 0usize;
    for c in raw.chars() {
        chars += 1;
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\0' => return Err((value.to_string(), "string_contains_null_byte")),
            other => out.push(other),
        }
    }
    if max_chars.is_some_and(|max| chars > max) {
        return Err((value.to_string(), "string_too_long"));
    }
    out.push('"');
    Ok(out)
}

#[allow(clippy::too_many_lines)] // exhaustive type dispatch per JSON variant
fn coerce_integer(value: &Value) -> CoerceResult {
    match value {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i32::try_from(i).is_ok() {
                    return CoerceResult::Ok(CopyEscaped::from_safe_ascii(i.to_string()));
                }
            }
            if let Some(f) = n.as_f64() {
                if f.fract() == 0.0 && f >= f64::from(i32::MIN) && f <= f64::from(i32::MAX) {
                    #[allow(clippy::cast_possible_truncation)] // bounds-checked above
                    return CoerceResult::Ok(CopyEscaped::from_safe_ascii((f as i64).to_string()));
                }
            }
            CoerceResult::Anomaly {
                actual_value: value.to_string(),
                actual_type: "number_out_of_range",
            }
        }
        // never parsed: a string in a numeric column is an anomaly, even when it reads as a number
        Value::String(s) => CoerceResult::Anomaly {
            actual_value: s.clone(),
            actual_type: "string",
        },
        _ => CoerceResult::Anomaly {
            actual_value: value.to_string(),
            actual_type: json_type_name(value),
        },
    }
}

#[allow(clippy::too_many_lines)] // exhaustive type dispatch per JSON variant
#[allow(clippy::cast_precision_loss)] // i64::MIN/MAX as f64 bounds check, already reasoned about exact behavior (see comment below)
fn coerce_bigint(value: &Value) -> CoerceResult {
    match value {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                return CoerceResult::Ok(CopyEscaped::from_safe_ascii(i.to_string()));
            }
            if let Some(f) = n.as_f64() {
                if f.fract() == 0.0 {
                    // i64::MIN as f64 = -2^63 (exact). i64::MAX as f64 rounds up to 2^63,
                    // so `f < i64::MAX as f64` correctly excludes 2^63 and above.
                    if f >= (i64::MIN as f64) && f < (i64::MAX as f64) {
                        #[allow(clippy::cast_possible_truncation)]
                        return CoerceResult::Ok(CopyEscaped::from_safe_ascii(
                            (f as i64).to_string(),
                        ));
                    }
                    return CoerceResult::Anomaly {
                        actual_value: value.to_string(),
                        actual_type: "integer_overflow",
                    };
                }
            }
            CoerceResult::Anomaly {
                actual_value: value.to_string(),
                actual_type: "float",
            }
        }
        // never parsed: a string in a numeric column is an anomaly, even when it reads as a number
        Value::String(s) => CoerceResult::Anomaly {
            actual_value: s.clone(),
            actual_type: "string",
        },
        _ => CoerceResult::Anomaly {
            actual_value: value.to_string(),
            actual_type: json_type_name(value),
        },
    }
}

#[allow(clippy::too_many_lines)] // exhaustive type dispatch per JSON variant
fn coerce_float(value: &Value) -> CoerceResult {
    match value {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                return format_float(f).map_or_else(
                    || CoerceResult::Anomaly {
                        actual_value: value.to_string(),
                        actual_type: "float_not_finite",
                    },
                    |s| CoerceResult::Ok(CopyEscaped::from_safe_ascii(s)),
                );
            }
            CoerceResult::Anomaly {
                actual_value: value.to_string(),
                actual_type: "number",
            }
        }
        // never parsed: a string in a numeric column is an anomaly, even when it reads as a number
        Value::String(s) => CoerceResult::Anomaly {
            actual_value: s.clone(),
            actual_type: "string",
        },
        _ => CoerceResult::Anomaly {
            actual_value: value.to_string(),
            actual_type: json_type_name(value),
        },
    }
}

fn coerce_bool(value: &Value) -> CoerceResult {
    match value {
        Value::Bool(b) => CoerceResult::Ok(CopyEscaped::from_safe_ascii(if *b {
            "t".to_string()
        } else {
            "f".to_string()
        })),
        Value::String(s) => match s.to_lowercase().as_str() {
            "true" | "yes" | "on" => CoerceResult::Ok(CopyEscaped::from_safe_ascii("t")),
            "false" | "no" | "off" => CoerceResult::Ok(CopyEscaped::from_safe_ascii("f")),
            _ => CoerceResult::Anomaly {
                actual_value: s.clone(),
                actual_type: "string",
            },
        },
        Value::Number(n) => match n.as_i64() {
            Some(1) => CoerceResult::Ok(CopyEscaped::from_safe_ascii("t")),
            Some(0) => CoerceResult::Ok(CopyEscaped::from_safe_ascii("f")),
            _ => CoerceResult::Anomaly {
                actual_value: value.to_string(),
                actual_type: "number",
            },
        },
        _ => CoerceResult::Anomaly {
            actual_value: value.to_string(),
            actual_type: json_type_name(value),
        },
    }
}

fn coerce_uuid(value: &Value) -> CoerceResult {
    match value {
        Value::String(s) => escape_copy_text(s).map_or_else(
            || CoerceResult::Anomaly {
                actual_value: value.to_string(),
                actual_type: "string_contains_null_byte",
            },
            CoerceResult::Ok,
        ),
        _ => CoerceResult::Anomaly {
            actual_value: value.to_string(),
            actual_type: json_type_name(value),
        },
    }
}

fn coerce_date(value: &Value) -> CoerceResult {
    match value {
        Value::String(s) => escape_copy_text(s).map_or_else(
            || CoerceResult::Anomaly {
                actual_value: value.to_string(),
                actual_type: "string_contains_null_byte",
            },
            CoerceResult::Ok,
        ),
        _ => CoerceResult::Anomaly {
            actual_value: value.to_string(),
            actual_type: json_type_name(value),
        },
    }
}

fn coerce_timestamp(value: &Value) -> CoerceResult {
    match value {
        Value::String(s) => escape_copy_text(s).map_or_else(
            || CoerceResult::Anomaly {
                actual_value: value.to_string(),
                actual_type: "string_contains_null_byte",
            },
            CoerceResult::Ok,
        ),
        _ => CoerceResult::Anomaly {
            actual_value: value.to_string(),
            actual_type: json_type_name(value),
        },
    }
}

fn coerce_text(value: &Value) -> CoerceResult {
    match value {
        Value::String(s) => escape_copy_text(s).map_or_else(
            || CoerceResult::Anomaly {
                actual_value: value.to_string(),
                actual_type: "string_contains_null_byte",
            },
            CoerceResult::Ok,
        ),
        Value::Number(n) => CoerceResult::Ok(CopyEscaped::from_safe_ascii(n.to_string())),
        Value::Bool(b) => CoerceResult::Ok(CopyEscaped::from_safe_ascii(b.to_string())),
        _ => CoerceResult::Anomaly {
            actual_value: value.to_string(),
            actual_type: json_type_name(value),
        },
    }
}

fn format_float(f: f64) -> Option<String> {
    if f.is_nan() || f.is_infinite() {
        None
    } else {
        Some(format!("{f}"))
    }
}

const fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_coerce_integer() {
        assert!(matches!(
            coerce(&json!(42), &PgType::Integer),
            CoerceResult::Ok(_)
        ));
        assert!(matches!(
            coerce(&json!("N/A"), &PgType::Integer),
            CoerceResult::Anomaly { .. }
        ));
    }

    /// Une chaîne dans une colonne numérique n'est jamais convertie : NULL + anomalie, même
    /// si elle se lit comme un nombre (valeurs aberrantes OFF, zéros de tête de "007").
    #[test]
    fn test_string_in_numeric_column_is_anomaly() {
        for (value, pg_type) in [
            ("42", PgType::Integer),
            ("007", PgType::BigInt),
            ("10000000000000000000", PgType::DoublePrecision),
        ] {
            assert!(
                matches!(
                    coerce(&json!(value), &pg_type),
                    CoerceResult::Anomaly {
                        actual_type: "string",
                        ..
                    }
                ),
                "{value} → {pg_type:?}"
            );
        }
    }

    #[test]
    fn test_coerce_bool() {
        assert!(matches!(
            coerce(&json!(true), &PgType::Boolean),
            CoerceResult::Ok(_)
        ));
        assert!(matches!(
            coerce(&json!("yes"), &PgType::Boolean),
            CoerceResult::Ok(_)
        ));
        assert!(matches!(
            coerce(&json!("maybe"), &PgType::Boolean),
            CoerceResult::Anomaly { .. }
        ));
    }

    #[test]
    #[allow(clippy::approx_constant)] // 3.14 is an arbitrary test float, not pi
    fn test_coerce_float_nan_infinity() {
        // NaN et Infinity ne sont pas des nombres JSON valides : en chaîne, ce sont des anomalies
        assert!(matches!(
            coerce_float(&serde_json::Value::String("NaN".to_string())),
            CoerceResult::Anomaly {
                actual_type: "string",
                ..
            }
        ));
        assert!(matches!(
            coerce_float(&serde_json::Value::String("Infinity".to_string())),
            CoerceResult::Anomaly {
                actual_type: "string",
                ..
            }
        ));
        assert!(matches!(
            coerce_float(&serde_json::Value::String("-Infinity".to_string())),
            CoerceResult::Anomaly {
                actual_type: "string",
                ..
            }
        ));
        // Les floats normaux passent toujours
        assert!(matches!(
            coerce(&json!(3.14), &PgType::DoublePrecision),
            CoerceResult::Ok(_)
        ));
    }

    #[test]
    fn test_coerce_text_null_byte() {
        assert!(matches!(
            coerce_text(&serde_json::Value::String("hello\x00world".to_string())),
            CoerceResult::Anomaly {
                actual_type: "string_contains_null_byte",
                ..
            }
        ));
    }

    #[test]
    fn test_pg_array_null_byte_element_becomes_null() {
        // PG rejette les null bytes dans les arrays text. Un élément qui en contient devient NULL
        // et laisse une anomalie, comme un scalaire texte (avant #84 : retiré en silence).
        let arr = vec![
            serde_json::Value::String("hello\x00world".to_string()),
            serde_json::Value::String("normal".to_string()),
        ];
        let result = coerce_pg_array(&arr, &PgType::Text);
        let CoerceResult::OkWithAnomaly {
            value,
            actual_value,
            actual_type,
        } = result
        else {
            panic!("expected OkWithAnomaly");
        };
        assert_eq!(value.as_str(), r#"{NULL,"normal"}"#);
        assert_eq!(actual_type, "string_contains_null_byte");
        assert!(
            actual_value.contains("hello"),
            "the faulty element is reported, got {actual_value}"
        );
    }

    #[test]
    fn test_null_always_null() {
        assert!(matches!(
            coerce(&Value::Null, &PgType::Integer),
            CoerceResult::Null
        ));
        assert!(matches!(
            coerce(&Value::Null, &PgType::Text),
            CoerceResult::Null
        ));
    }

    #[test]
    fn test_coerce_bigint_overflow_is_anomaly() {
        // 10^20 > i64::MAX — must produce Anomaly, not silent saturating cast to i64::MAX.
        let huge_pos = serde_json::from_str::<Value>("100000000000000000000").unwrap();
        assert!(
            matches!(
                coerce(&huge_pos, &PgType::BigInt),
                CoerceResult::Anomaly { .. }
            ),
            "positive integer overflow must be an anomaly"
        );

        // Negative overflow (< i64::MIN)
        let huge_neg = serde_json::from_str::<Value>("-100000000000000000000").unwrap();
        assert!(
            matches!(
                coerce(&huge_neg, &PgType::BigInt),
                CoerceResult::Anomaly { .. }
            ),
            "negative integer overflow must be an anomaly"
        );

        // i64::MAX must still coerce OK
        assert!(matches!(
            coerce(&json!(i64::MAX), &PgType::BigInt),
            CoerceResult::Ok(_)
        ));

        // 1e18 is a whole-number float within i64 range — must still coerce OK
        let ok_float = serde_json::from_str::<Value>("1e18").unwrap();
        assert!(
            matches!(coerce(&ok_float, &PgType::BigInt), CoerceResult::Ok(_)),
            "whole-number float within i64 range must coerce OK"
        );
    }

    // ── issue #48 : occurrence scalaire dans une colonne PG array → tableau à un élément ──

    fn coerce_ok(value: &Value, pg_type: &PgType) -> String {
        match coerce(value, pg_type) {
            CoerceResult::Ok(s) => s.as_str().to_owned(),
            _ => panic!("expected Ok for {value}"),
        }
    }

    #[test]
    fn test_coerce_scalar_into_pg_array_wraps() {
        let text_arr = PgType::Array(Box::new(PgType::Text));
        assert_eq!(coerce_ok(&json!("bio"), &text_arr), r#"{"bio"}"#);
        assert_eq!(
            coerce_ok(&json!(5), &PgType::Array(Box::new(PgType::Integer))),
            "{5}"
        );
        assert_eq!(
            coerce_ok(&json!(true), &PgType::Array(Box::new(PgType::Boolean))),
            "{t}"
        );
        // a number widened to TEXT[] is wrapped as text
        assert_eq!(coerce_ok(&json!(5), &text_arr), r#"{"5"}"#);
        assert_eq!(coerce_ok(&json!([]), &text_arr), "{}");
    }

    #[test]
    fn test_coerce_null_into_pg_array_stays_null() {
        assert!(matches!(
            coerce(&Value::Null, &PgType::Array(Box::new(PgType::Text))),
            CoerceResult::Null
        ));
    }

    // ── issue #84 : un élément non convertible → NULL dans le tableau + UNE anomalie par cellule ──

    fn array_of(elem: PgType) -> PgType {
        PgType::Array(Box::new(elem))
    }

    /// (literal, first faulty element, anomaly type) of a cell written with at least one `NULL` element.
    fn coerce_partial(value: &Value, pg_type: &PgType) -> (String, String, &'static str) {
        match coerce(value, pg_type) {
            CoerceResult::OkWithAnomaly {
                value,
                actual_value,
                actual_type,
            } => (value.as_str().to_owned(), actual_value, actual_type),
            _ => panic!("expected OkWithAnomaly"),
        }
    }

    #[test]
    fn test_pg_array_element_too_long_for_varchar_becomes_null() {
        // VARCHAR(2)[]: "abc" and "abcd" overflow, only the first one is reported
        let (literal, bad, kind) =
            coerce_partial(&json!(["ab", "abc", "abcd"]), &array_of(PgType::VarChar(2)));
        assert_eq!(literal, r#"{"ab",NULL,NULL}"#);
        assert_eq!((bad.as_str(), kind), (r#""abc""#, "string_too_long"));
    }

    #[test]
    fn test_pg_array_non_string_element_in_date_array_becomes_null() {
        let (literal, bad, kind) =
            coerce_partial(&json!(["2024-01-01", 5]), &array_of(PgType::Date));
        assert_eq!(literal, r#"{"2024-01-01",NULL}"#);
        assert_eq!((bad.as_str(), kind), ("5", "number"));
    }

    #[test]
    fn test_pg_array_polygon_in_float_array_becomes_null() {
        // GeoJSON polygon: one ring (an array) where a number is expected
        let polygon = json!([[[0, 0], [1, 0], [1, 1], [0, 0]]]);
        let (literal, _, kind) = coerce_partial(&polygon, &array_of(PgType::DoublePrecision));
        assert_eq!((literal.as_str(), kind), ("{NULL}", "array"));
    }

    #[test]
    fn test_pg_array_reports_first_faulty_element_only() {
        let (literal, bad, kind) =
            coerce_partial(&json!(["abc", 7, "xyz"]), &array_of(PgType::Integer));
        assert_eq!(literal, "{NULL,7,NULL}");
        assert_eq!((bad.as_str(), kind), ("abc", "string"));
    }

    #[test]
    fn test_pg_array_null_elements_are_not_anomalies() {
        assert_eq!(
            coerce_ok(&json!([1, null, 3]), &array_of(PgType::Integer)),
            "{1,NULL,3}"
        );
    }

    #[test]
    fn test_scalar_occurrence_too_long_for_varchar_array_becomes_null() {
        // mixed field (#48): the scalar is wrapped as a one-element array and checked like one
        let (literal, _, kind) = coerce_partial(&json!("toolong"), &array_of(PgType::VarChar(2)));
        assert_eq!((literal.as_str(), kind), ("{NULL}", "string_too_long"));
    }

    #[test]
    fn test_pg_array_nested_arrays_stay_json_text_with_the_inferred_type() {
        use crate::schema::registry::{RegistryConfig, SchemaRegistry};
        // Written with the type Pass 1 really infers, not a hand-built one. Arrays do not widen a
        // VARCHAR: elements that are all arrays give TEXT[], strings mixed with arrays give VARCHAR(n)[].
        let inferred = |row: &Value| {
            let mut reg = SchemaRegistry::new(RegistryConfig::default());
            reg.observe_root("t", row.as_object().unwrap());
            reg.finalize()[0]
                .find_by_original("m")
                .unwrap()
                .pg_type
                .clone()
        };

        // all arrays → TEXT[]: nested arrays stay JSON text (#83), no anomaly
        let all_arrays = json!({"m": [["a"], ["b"]]});
        let pg_type = inferred(&all_arrays);
        assert_eq!(pg_type, array_of(PgType::Text));
        assert_eq!(
            coerce_ok(&all_arrays["m"], &pg_type),
            r#"{"[\\"a\\"]","[\\"b\\"]"}"#
        ); // COPY-escaped

        // strings mixed with arrays → VARCHAR(2)[]: the nested array overflows, which used to abort the COPY
        let mixed = json!({"m": ["a", ["b"]]});
        let pg_type = inferred(&mixed);
        assert_eq!(pg_type, array_of(PgType::VarChar(2)));
        let (literal, _, kind) = coerce_partial(&mixed["m"], &pg_type);
        assert_eq!(
            (literal.as_str(), kind),
            (r#"{"a",NULL}"#, "string_too_long")
        );
    }
}
