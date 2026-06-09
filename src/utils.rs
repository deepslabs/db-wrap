/// Compute the SHA-256 hash of `msg` and return it as raw bytes.
pub fn sha2_hash256(msg: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(msg);
    hasher.finalize()[..].to_vec()
}

/// Format `bytes` as a hex string, hiding the middle with a `padding` ellipsis.
/// `hl` = number of leading bytes to show, `el` = number of ending bytes.
pub fn bytes_to_hide_hex(bytes: &[u8], hl: usize, el: usize, padding: Option<&str>) -> String {
    let length = bytes.len();
    let hex_str = hex::encode(bytes);
    let mut pad_str = "...";
    if let Some(pad) = padding {
        pad_str = pad;
    }
    let mut hl = hl;
    let mut el = el;
    match hl + el {
        l if l == length => pad_str = "",
        l if l > length => {
            hl = length;
            el = 0;
            pad_str = "";
        }
        _ => (),
    }

    "0x".to_string() + &hex_str[..hl * 2] + pad_str + &hex_str[(length - el) * 2..]
}

/// Shortcut: show the first 6 & last 6 bytes of a SHA-256 hash as hex.
pub fn debug_hash_data(data: &[u8]) -> String {
    bytes_to_hide_hex(&sha2_hash256(data), 6, 6, None)
}
