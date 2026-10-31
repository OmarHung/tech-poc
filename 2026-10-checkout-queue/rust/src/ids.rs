//! Ticket and order ids: 128 random bits as 32 lowercase hex characters. Holding a ticket id is
//! what lets you check out, so it must not be guessable from someone else's.

const HEX: &[u8; 16] = b"0123456789abcdef";

pub fn new_id() -> String {
    random_bytes::<16>()
        .iter()
        .flat_map(|b| [HEX[usize::from(b >> 4)], HEX[usize::from(b & 0x0f)]])
        .map(char::from)
        .collect()
}

pub fn is_valid(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Bytes straight from the operating system's CSPRNG.
pub fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    // The OS generator failing is not something a request can recover from.
    getrandom::fill(&mut bytes).expect("the operating system's random generator is unavailable");
    bytes
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn ids_are_random_and_validate_their_own_format() {
        let ids: Vec<String> = (0..100).map(|_| new_id()).collect();

        assert_eq!(ids.iter().collect::<HashSet<_>>().len(), 100);
        assert!(ids.iter().all(|id| is_valid(id)), "{ids:?}");
        assert!(is_valid(&"abcdef0123456789".repeat(2)));
        assert!(!is_valid(&"ABCDEF0123456789".repeat(2)));
        assert!(!is_valid(&ids[0][..31]));
        assert!(!is_valid(&format!("{}0", ids[0])));
        assert!(!is_valid(""));
        assert!(!is_valid(&"g".repeat(32)));
    }
}
