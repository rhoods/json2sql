use serde_json::Value;

use crate::schema::type_tracker::PgType;

/// Result of attempting to coerce a JSON value to a PgType.
pub enum CoerceResult {
    /// Successfully converted: the string to include in COPY text format
    Ok(String),
    /// Value is SQL NULL
    Null,
    /// Value could not be coerced to the target type
    Anomaly { actual_value: String, actual_type: &'static str },
}

/// Coerce a JSON value to the COPY text-format representation for the given PgType.
/// Returns `None` on anomaly (caller records the anomaly and inserts NULL).
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
                    _ => s.len(),
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
            _ => CoerceResult::Anomaly {
                actual_value: value.to_string(),
                actual_type: json_type_name(value),
            },
        },
    }
}

/// Serialize a JSON array as a PostgreSQL array literal for COPY text format.
/// Format: `{elem1,elem2,NULL,elem3}` with COPY-level escaping applied to the whole literal.
fn coerce_pg_array(arr: &[Value], elem_type: &PgType) -> CoerceResult {
    let mut parts = Vec::with_capacity(arr.len());
    for item in arr {
        if matches!(item, Value::Null) {
            parts.push("NULL".to_string());
            continue;
        }
        let elem_str = coerce_pg_array_element(item, elem_type);
        parts.push(elem_str);
    }
    // Build `{e1,e2,...}` and COPY-escape the whole literal.
    // Array literals are built from coerced elements which cannot contain null bytes.
    let literal = format!("{{{}}}", parts.join(","));
    CoerceResult::Ok(escape_copy_text(&literal).unwrap_or_default())
}

/// Produce the array-literal representation of one element (no outer COPY escaping yet).
/// Text-like types are double-quoted; numeric/boolean are bare.
fn coerce_pg_array_element(value: &Value, pg_type: &PgType) -> String {
    match pg_type {
        PgType::Integer | PgType::BigInt | PgType::DoublePrecision | PgType::Boolean => {
            match coerce(value, pg_type) {
                CoerceResult::Ok(s) => s,
                _ => "NULL".to_string(),
            }
        }
        _ => {
            // Text-like: get the raw string and double-quote it for array syntax.
            // Escape `\` and `"` inside the element so the array parser can read them back.
            // The outer escape_copy_text call will then COPY-escape the whole literal.
            // Null bytes are stripped here (PostgreSQL rejects them); the caller's
            // unwrap_or_default() on the outer escape_copy_text is safe because
            // we guarantee no null bytes survive into the array literal.
            let raw = match value {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let mut out = String::with_capacity(raw.len() + 2);
            out.push('"');
            for c in raw.chars() {
                match c {
                    '\\' => out.push_str("\\\\"),
                    '"' => out.push_str("\\\""),
                    '\0' => {} // strip null bytes — PostgreSQL rejects them in array elements
                    other => out.push(other),
                }
            }
            out.push('"');
            out
        }
    }
}

fn coerce_integer(value: &Value) -> CoerceResult {
    match value {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    return CoerceResult::Ok(i.to_string());
                }
            }
            if let Some(f) = n.as_f64() {
                if f.fract() == 0.0 && f >= i32::MIN as f64 && f <= i32::MAX as f64 {
                    return CoerceResult::Ok((f as i64).to_string());
                }
            }
            CoerceResult::Anomaly {
                actual_value: value.to_string(),
                actual_type: "number_out_of_range",
            }
        }
        Value::String(s) => {
            if let Ok(i) = s.trim().parse::<i32>() {
                CoerceResult::Ok(i.to_string())
            } else {
                CoerceResult::Anomaly {
                    actual_value: s.clone(),
                    actual_type: "string",
                }
            }
        }
        _ => CoerceResult::Anomaly {
            actual_value: value.to_string(),
            actual_type: json_type_name(value),
        },
    }
}

fn coerce_bigint(value: &Value) -> CoerceResult {
    match value {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                return CoerceResult::Ok(i.to_string());
            }
            if let Some(f) = n.as_f64() {
                if f.fract() == 0.0 {
                    return CoerceResult::Ok((f as i64).to_string());
                }
            }
            CoerceResult::Anomaly {
                actual_value: value.to_string(),
                actual_type: "float",
            }
        }
        Value::String(s) => {
            if let Ok(i) = s.trim().parse::<i64>() {
                CoerceResult::Ok(i.to_string())
            } else {
                CoerceResult::Anomaly {
                    actual_value: s.clone(),
                    actual_type: "string",
                }
            }
        }
        _ => CoerceResult::Anomaly {
            actual_value: value.to_string(),
            actual_type: json_type_name(value),
        },
    }
}

fn coerce_float(value: &Value) -> CoerceResult {
    match value {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                return match format_float(f) {
                    Some(s) => CoerceResult::Ok(s),
                    None => CoerceResult::Anomaly {
                        actual_value: value.to_string(),
                        actual_type: "float_not_finite",
                    },
                };
            }
            CoerceResult::Anomaly {
                actual_value: value.to_string(),
                actual_type: "number",
            }
        }
        Value::String(s) => {
            if let Ok(f) = s.trim().parse::<f64>() {
                match format_float(f) {
                    Some(s) => CoerceResult::Ok(s),
                    None => CoerceResult::Anomaly {
                        actual_value: value.to_string(),
                        actual_type: "float_not_finite",
                    },
                }
            } else {
                CoerceResult::Anomaly {
                    actual_value: s.clone(),
                    actual_type: "string",
                }
            }
        }
        _ => CoerceResult::Anomaly {
            actual_value: value.to_string(),
            actual_type: json_type_name(value),
        },
    }
}

fn coerce_bool(value: &Value) -> CoerceResult {
    match value {
        Value::Bool(b) => CoerceResult::Ok(if *b { "t".to_string() } else { "f".to_string() }),
        Value::String(s) => match s.to_lowercase().as_str() {
            "true" | "yes" | "1" | "on" => CoerceResult::Ok("t".to_string()),
            "false" | "no" | "0" | "off" => CoerceResult::Ok("f".to_string()),
            _ => CoerceResult::Anomaly {
                actual_value: s.clone(),
                actual_type: "string",
            },
        },
        Value::Number(n) => match n.as_i64() {
            Some(1) => CoerceResult::Ok("t".to_string()),
            Some(0) => CoerceResult::Ok("f".to_string()),
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
        Value::String(s) => CoerceResult::Ok(s.clone()),
        _ => CoerceResult::Anomaly {
            actual_value: value.to_string(),
            actual_type: json_type_name(value),
        },
    }
}

fn coerce_date(value: &Value) -> CoerceResult {
    match value {
        Value::String(s) => CoerceResult::Ok(s.clone()),
        _ => CoerceResult::Anomaly {
            actual_value: value.to_string(),
            actual_type: json_type_name(value),
        },
    }
}

fn coerce_timestamp(value: &Value) -> CoerceResult {
    match value {
        Value::String(s) => CoerceResult::Ok(s.clone()),
        _ => CoerceResult::Anomaly {
            actual_value: value.to_string(),
            actual_type: json_type_name(value),
        },
    }
}

fn coerce_text(value: &Value) -> CoerceResult {
    match value {
        Value::String(s) => match escape_copy_text(s) {
            Some(escaped) => CoerceResult::Ok(escaped),
            None => CoerceResult::Anomaly {
                actual_value: value.to_string(),
                actual_type: "string_contains_null_byte",
            },
        },
        Value::Number(n) => CoerceResult::Ok(n.to_string()),
        Value::Bool(b) => CoerceResult::Ok(b.to_string()),
        _ => CoerceResult::Anomaly {
            actual_value: value.to_string(),
            actual_type: json_type_name(value),
        },
    }
}

/// Escape a string for PostgreSQL COPY text format.
/// Special characters: backslash, tab, newline, carriage return.
/// Returns `None` if the string contains a null byte (PostgreSQL rejects them;
/// callers should treat this as an anomaly rather than silently stripping the byte).
pub fn escape_copy_text(s: &str) -> Option<String> {
    if s.contains('\0') {
        return None;
    }
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            other => out.push(other),
        }
    }
    Some(out)
}

fn format_float(f: f64) -> Option<String> {
    if f.is_nan() || f.is_infinite() {
        None
    } else {
        Some(format!("{}", f))
    }
}

fn json_type_name(v: &Value) -> &'static str {
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
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_coerce_integer() {
        assert!(matches!(coerce(&json!(42), &PgType::Integer), CoerceResult::Ok(_)));
        assert!(matches!(coerce(&json!("42"), &PgType::Integer), CoerceResult::Ok(_)));
        assert!(matches!(coerce(&json!("N/A"), &PgType::Integer), CoerceResult::Anomaly { .. }));
    }

    #[test]
    fn test_coerce_bool() {
        assert!(matches!(coerce(&json!(true), &PgType::Boolean), CoerceResult::Ok(_)));
        assert!(matches!(coerce(&json!("yes"), &PgType::Boolean), CoerceResult::Ok(_)));
        assert!(matches!(coerce(&json!("maybe"), &PgType::Boolean), CoerceResult::Anomaly { .. }));
    }

    #[test]
    fn test_escape_copy_text() {
        assert_eq!(escape_copy_text("hello\tworld"), Some("hello\\tworld".to_string()));
        assert_eq!(escape_copy_text("line1\nline2"), Some("line1\\nline2".to_string()));
        assert_eq!(escape_copy_text("back\\slash"), Some("back\\\\slash".to_string()));
        assert_eq!(escape_copy_text("null\x00byte"), None);
    }

    #[test]
    fn test_coerce_float_nan_infinity() {
        // NaN et Infinity ne sont pas des valeurs JSON valides → anomalie
        // On ne peut pas les créer via serde_json::json!(), on passe par f64 directement
        assert!(matches!(
            coerce_float(&serde_json::Value::String("NaN".to_string())),
            CoerceResult::Anomaly { actual_type: "float_not_finite", .. }
        ));
        assert!(matches!(
            coerce_float(&serde_json::Value::String("Infinity".to_string())),
            CoerceResult::Anomaly { actual_type: "float_not_finite", .. }
        ));
        assert!(matches!(
            coerce_float(&serde_json::Value::String("-Infinity".to_string())),
            CoerceResult::Anomaly { actual_type: "float_not_finite", .. }
        ));
        // Les floats normaux passent toujours
        assert!(matches!(coerce(&json!(3.14), &PgType::DoublePrecision), CoerceResult::Ok(_)));
    }

    #[test]
    fn test_coerce_text_null_byte() {
        assert!(matches!(
            coerce_text(&serde_json::Value::String("hello\x00world".to_string())),
            CoerceResult::Anomaly { actual_type: "string_contains_null_byte", .. }
        ));
    }

    #[test]
    fn test_pg_array_null_byte_stripped() {
        // Un null byte dans un élément texte d'array doit être supprimé
        // (PG rejette les null bytes dans les arrays text aussi)
        let arr = vec![
            serde_json::Value::String("hello\x00world".to_string()),
            serde_json::Value::String("normal".to_string()),
        ];
        let result = coerce_pg_array(&arr, &PgType::Text);
        assert!(matches!(result, CoerceResult::Ok(_)));
        if let CoerceResult::Ok(s) = result {
            assert!(!s.contains('\0'), "null byte should be stripped from array element");
            assert!(s.contains("helloworld"), "content sans null byte doit être présent");
        }
    }

    #[test]
    fn test_null_always_null() {
        assert!(matches!(coerce(&Value::Null, &PgType::Integer), CoerceResult::Null));
        assert!(matches!(coerce(&Value::Null, &PgType::Text), CoerceResult::Null));
    }
}
