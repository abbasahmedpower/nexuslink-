//! Cryptographic utilities for KDE Connect certificate verification.
//! Implements constant-time fingerprint comparison via `subtle` and FIPS 180-4 SHA-256 via `sha2`.

use sha2::{Digest, Sha256};
use std::fmt;
use subtle::ConstantTimeEq;

/// 32-byte SHA-256 hash representation of a TLS certificate.
#[derive(Clone, Copy, Eq)]
pub struct CertificateFingerprint(pub [u8; 32]);

impl PartialEq for CertificateFingerprint {
    fn eq(&self, other: &Self) -> bool {
        self.verify_constant_time(other)
    }
}

impl CertificateFingerprint {
    pub fn from_der(der_bytes: &[u8]) -> Self {
        Self(sha256_digest(der_bytes))
    }

    /// Formats fingerprint as KDE Connect standard colon-delimited hex string:
    /// e.g. "a1:b2:c3:d4:..."
    pub fn to_colon_hex(&self) -> String {
        let mut s = String::with_capacity(32 * 3 - 1);
        for (i, b) in self.0.iter().enumerate() {
            if i > 0 {
                s.push(':');
            }
            use std::fmt::Write;
            let _ = write!(s, "{:02x}", b);
        }
        s
    }

    /// Validates and parses a colon-hex string (e.g. "a1:b2:...") or raw 64-char hex string.
    #[allow(clippy::chunks_exact_to_as_chunks)]
    pub fn parse_str(input: &str) -> Option<Self> {
        let clean = input.replace(':', "");
        if clean.len() != 64 {
            return None;
        }
        let mut bytes = [0u8; 32];
        for (i, chunk) in clean.as_bytes().chunks_exact(2).enumerate() {
            let s = std::str::from_utf8(chunk).ok()?;
            bytes[i] = u8::from_str_radix(s, 16).ok()?;
        }
        Some(Self(bytes))
    }

    /// Verifies candidate fingerprint against this expected fingerprint using constant-time comparison.
    pub fn verify_constant_time(&self, candidate: &CertificateFingerprint) -> bool {
        self.0.ct_eq(&candidate.0).into()
    }
}

impl fmt::Debug for CertificateFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CertificateFingerprint({})", self.to_colon_hex())
    }
}

impl fmt::Display for CertificateFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_colon_hex())
    }
}

/// Constant-time byte slice comparison to defeat timing side-channel attacks via `subtle`.
pub fn verify_fingerprint_constant_time(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}

/// Standard FIPS 180-4 SHA-256 digest function via audited `sha2` crate.
pub fn sha256_digest(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}
