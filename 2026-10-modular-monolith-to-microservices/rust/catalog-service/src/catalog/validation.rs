use super::CatalogError;

pub const MAX_QUANTITY: u64 = 100;

pub fn validate_quantity(quantity: u64) -> Result<u64, CatalogError> {
    if (1..=MAX_QUANTITY).contains(&quantity) {
        Ok(quantity)
    } else {
        Err(CatalogError::InvalidInput(format!(
            "quantity must be between 1 and {MAX_QUANTITY}"
        )))
    }
}

pub fn validate_id(id: u64, field: &str) -> Result<u64, CatalogError> {
    if id >= 1 {
        Ok(id)
    } else {
        Err(not_positive(field))
    }
}

/// Parses an id taken from a URL path segment.
pub fn parse_id(raw: &str, field: &str) -> Result<u64, CatalogError> {
    raw.parse::<u64>()
        .map_err(|_| not_positive(field))
        .and_then(|id| validate_id(id, field))
}

fn not_positive(field: &str) -> CatalogError {
    CatalogError::InvalidInput(format!("{field} must be a positive integer"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantity_must_be_within_bounds() {
        assert_eq!(validate_quantity(1), Ok(1));
        assert_eq!(validate_quantity(MAX_QUANTITY), Ok(MAX_QUANTITY));
        assert!(matches!(
            validate_quantity(0),
            Err(CatalogError::InvalidInput(_))
        ));
        assert!(matches!(
            validate_quantity(MAX_QUANTITY + 1),
            Err(CatalogError::InvalidInput(_))
        ));
    }

    #[test]
    fn parse_id_accepts_only_positive_integers() {
        assert_eq!(parse_id("42", "product id"), Ok(42));
        for raw in ["0", "-1", "abc", "", "1.5"] {
            assert_eq!(
                parse_id(raw, "product id"),
                Err(CatalogError::InvalidInput(
                    "product id must be a positive integer".into()
                )),
                "input {raw:?}"
            );
        }
    }
}
