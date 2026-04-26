// Copyright 2026 James Gober. Licensed under Apache-2.0.

//! TTL types and helpers.
#![allow(dead_code)]

use std::time::Duration;

#[cfg(feature = "ttl")]
use crate::Error;

/// Time-to-live specification for a record.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use emdb::Ttl;
///
/// let never = Ttl::Never;
/// let default = Ttl::Default;
/// let short = Ttl::After(Duration::from_secs(5));
///
/// assert!(matches!(never, Ttl::Never));
/// assert!(matches!(default, Ttl::Default));
/// assert!(matches!(short, Ttl::After(_)));
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ttl {
    /// Use the global default TTL from the builder.
    ///
    /// If no default is configured, this behaves as [`Ttl::Never`].
    Default,
    /// Explicit no-expiration.
    Never,
    /// Expire after the given duration from insertion.
    After(Duration),
}

#[cfg(not(feature = "ttl"))]
pub(crate) type Record = Vec<u8>;

#[cfg(feature = "ttl")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Record {
    pub(crate) value: Vec<u8>,
    pub(crate) expires_at: Option<u64>,
}

#[cfg(feature = "ttl")]
pub(crate) fn now_unix_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(u64::MAX as u128) as u64,
        Err(_before_epoch) => 0,
    }
}

#[cfg(feature = "ttl")]
pub(crate) fn expires_from_ttl(
    ttl: Ttl,
    default_ttl: Option<Duration>,
    now_ms: u64,
) -> crate::Result<Option<u64>> {
    let chosen = match ttl {
        Ttl::Default => default_ttl,
        Ttl::Never => None,
        Ttl::After(duration) => Some(duration),
    };

    let Some(duration) = chosen else {
        return Ok(None);
    };

    let delta = duration.as_millis();
    let absolute = (now_ms as u128)
        .checked_add(delta)
        .ok_or(Error::TtlOverflow)?;
    let expires_at = u64::try_from(absolute).map_err(|_overflow| Error::TtlOverflow)?;
    Ok(Some(expires_at))
}

#[cfg(feature = "ttl")]
pub(crate) fn is_expired(expires_at: Option<u64>, now_ms: u64) -> bool {
    match expires_at {
        Some(deadline) => deadline <= now_ms,
        None => false,
    }
}

#[cfg(feature = "ttl")]
pub(crate) fn remaining_ttl(expires_at: u64, now_ms: u64) -> Option<Duration> {
    if expires_at <= now_ms {
        return None;
    }

    Some(Duration::from_millis(expires_at - now_ms))
}

#[cfg(feature = "ttl")]
pub(crate) fn record_new(value: Vec<u8>, expires_at: Option<u64>) -> Record {
    Record { value, expires_at }
}

#[cfg(not(feature = "ttl"))]
pub(crate) fn record_new(value: Vec<u8>, _expires_at: Option<u64>) -> Record {
    value
}

pub(crate) fn record_value(record: &Record) -> &[u8] {
    #[cfg(feature = "ttl")]
    {
        record.value.as_slice()
    }

    #[cfg(not(feature = "ttl"))]
    {
        record.as_slice()
    }
}

pub(crate) fn record_into_value(record: Record) -> Vec<u8> {
    #[cfg(feature = "ttl")]
    {
        record.value
    }

    #[cfg(not(feature = "ttl"))]
    {
        record
    }
}

#[cfg(feature = "ttl")]
pub(crate) fn record_expires_at(record: &Record) -> Option<u64> {
    #[cfg(feature = "ttl")]
    {
        record.expires_at
    }
}

#[cfg(feature = "ttl")]
pub(crate) fn record_set_persist(record: &mut Record) -> bool {
    #[cfg(feature = "ttl")]
    {
        let changed = record.expires_at.is_some();
        record.expires_at = None;
        changed
    }
}

#[cfg(all(test, feature = "ttl"))]
mod tests {
    use super::*;

    #[cfg(feature = "ttl")]
    #[test]
    fn test_ttl_default_without_global_is_never() {
        let now = 100_u64;
        let expires = expires_from_ttl(Ttl::Default, None, now);
        assert!(matches!(expires, Ok(None)));
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_ttl_after_zero_is_immediate() {
        let now = 123_u64;
        let expires = expires_from_ttl(Ttl::After(Duration::ZERO), None, now);
        assert!(matches!(expires, Ok(Some(deadline)) if deadline == now));
        assert!(is_expired(Some(now), now));
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_ttl_after_uses_duration() {
        let now = 1_000_u64;
        let expires = expires_from_ttl(Ttl::After(Duration::from_secs(2)), None, now);
        assert!(matches!(expires, Ok(Some(deadline)) if deadline == 3_000));
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_ttl_default_uses_global() {
        let now = 500_u64;
        let expires = expires_from_ttl(Ttl::Default, Some(Duration::from_millis(25)), now);
        assert!(matches!(expires, Ok(Some(deadline)) if deadline == 525));
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_ttl_overflow_is_reported() {
        let now = u64::MAX;
        let result = expires_from_ttl(Ttl::After(Duration::from_millis(1)), None, now);
        assert!(matches!(result, Err(Error::TtlOverflow)));
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_remaining_ttl_boundary_is_none() {
        assert_eq!(remaining_ttl(100, 100), None);
    }

    #[cfg(feature = "ttl")]
    #[test]
    fn test_record_persist_clears_expiration() {
        let mut record = record_new(vec![1], Some(10));
        assert!(record_set_persist(&mut record));
        assert_eq!(record_expires_at(&record), None);
    }
}
