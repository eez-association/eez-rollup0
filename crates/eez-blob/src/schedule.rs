//! Which blob limit and sidecar wrapper an L1 timestamp falls under.
//!
//! The Composer sizes candidates and the proof signer admits them from the same
//! parsed schedule, so the two can never disagree about how many blobs fit.

use std::{env, str::FromStr};

use crate::BlobSidecarEncoding;

const ENV_GENESIS_TIMESTAMP: &str = "EEZ_L1_GENESIS_TIMESTAMP";
const ENV_SLOT_SECONDS: &str = "EEZ_L1_SLOT_SECONDS";
const ENV_BLOB_FORKS: &str = "EEZ_L1_BLOB_FORKS";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ScheduleError {
    #[error("{0} is required for blob settlement")]
    Missing(&'static str),
    #[error("{name}: {reason}")]
    Invalid { name: &'static str, reason: String },
}

/// One timestamp-activated L1 blob rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobFork {
    pub activation_timestamp: u64,
    pub max_blobs_per_transaction: u64,
    pub max_blobs_per_block: u64,
    pub sidecar_encoding: BlobSidecarEncoding,
}

impl BlobFork {
    /// Blobs one settlement transaction may carry: bounded by both caps.
    #[must_use]
    pub fn max_blobs(self) -> u64 {
        self.max_blobs_per_transaction.min(self.max_blobs_per_block)
    }
}

/// Ordered fork rules. Parsed from
/// `<activation>:<per_tx>:<per_block>:<eip4844|eip7594>,…`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobSchedule {
    forks: Vec<BlobFork>,
}

impl BlobSchedule {
    /// # Errors
    ///
    /// [`ScheduleError::Invalid`] for an empty schedule, a zero limit or a
    /// duplicated activation.
    pub fn new(mut forks: Vec<BlobFork>) -> Result<Self, ScheduleError> {
        let invalid = |reason: &str| ScheduleError::Invalid {
            name: ENV_BLOB_FORKS,
            reason: reason.into(),
        };
        if forks.is_empty() {
            return Err(invalid("at least one fork is required"));
        }
        forks.sort_unstable_by_key(|fork| fork.activation_timestamp);
        if forks
            .windows(2)
            .any(|pair| pair[0].activation_timestamp == pair[1].activation_timestamp)
        {
            return Err(invalid("duplicate activation timestamp"));
        }
        if forks
            .iter()
            .any(|fork| fork.max_blobs_per_transaction == 0 || fork.max_blobs_per_block == 0)
        {
            return Err(invalid("blob limits must be nonzero"));
        }
        Ok(Self { forks })
    }

    /// The rule active at `timestamp`, if any fork has activated.
    #[must_use]
    pub fn fork_at(&self, timestamp: u64) -> Option<BlobFork> {
        self.forks
            .iter()
            .rev()
            .find(|fork| fork.activation_timestamp <= timestamp)
            .copied()
    }

    /// [`BlobFork::max_blobs`] of the rule active at `timestamp`.
    #[must_use]
    pub fn max_blobs_at(&self, timestamp: u64) -> Option<usize> {
        self.fork_at(timestamp)
            .and_then(|fork| usize::try_from(fork.max_blobs()).ok())
    }
}

impl FromStr for BlobSchedule {
    type Err = ScheduleError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = |reason: String| ScheduleError::Invalid {
            name: ENV_BLOB_FORKS,
            reason,
        };
        let forks = value
            .split(',')
            .map(|rule| {
                let fields: Vec<&str> = rule.trim().split(':').collect();
                let [activation, per_tx, per_block, encoding] = fields.as_slice() else {
                    return Err(invalid(format!(
                        "expected <activation>:<per_tx>:<per_block>:<encoding>, got {rule:?}"
                    )));
                };
                let number = |field: &str| {
                    field
                        .parse::<u64>()
                        .map_err(|error| invalid(format!("{field:?}: {error}")))
                };
                Ok(BlobFork {
                    activation_timestamp: number(activation)?,
                    max_blobs_per_transaction: number(per_tx)?,
                    max_blobs_per_block: number(per_block)?,
                    sidecar_encoding: match *encoding {
                        "eip4844" => BlobSidecarEncoding::Eip4844,
                        "eip7594" => BlobSidecarEncoding::Eip7594,
                        other => {
                            return Err(invalid(format!("unknown sidecar encoding {other:?}")));
                        }
                    },
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(forks)
    }
}

/// L1 slot grid: a settlement target must be the start of a slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L1SlotClock {
    pub genesis_timestamp: u64,
    pub slot_seconds: u64,
}

impl L1SlotClock {
    /// # Errors
    ///
    /// [`ScheduleError::Invalid`] for a zero slot length.
    pub fn new(genesis_timestamp: u64, slot_seconds: u64) -> Result<Self, ScheduleError> {
        if slot_seconds == 0 {
            return Err(ScheduleError::Invalid {
                name: ENV_SLOT_SECONDS,
                reason: "must be nonzero".into(),
            });
        }
        Ok(Self {
            genesis_timestamp,
            slot_seconds,
        })
    }

    #[must_use]
    pub fn is_slot_start(self, timestamp: u64) -> bool {
        timestamp
            .checked_sub(self.genesis_timestamp)
            .is_some_and(|elapsed| elapsed.is_multiple_of(self.slot_seconds))
    }

    /// The first slot start strictly after `timestamp`.
    #[must_use]
    pub fn next_slot_after(self, timestamp: u64) -> u64 {
        match timestamp.checked_sub(self.genesis_timestamp) {
            Some(elapsed) => {
                self.genesis_timestamp + (elapsed / self.slot_seconds + 1) * self.slot_seconds
            }
            None => self.genesis_timestamp,
        }
    }
}

/// Read the shared L1 slot grid and blob schedule from `EEZ_L1_*`.
///
/// # Errors
///
/// [`ScheduleError`] for any missing or malformed variable.
pub fn l1_schedule_from_env() -> Result<(L1SlotClock, BlobSchedule), ScheduleError> {
    let read = |name: &'static str| env::var(name).map_err(|_| ScheduleError::Missing(name));
    let number = |name: &'static str| {
        read(name)?
            .trim()
            .parse::<u64>()
            .map_err(|error| ScheduleError::Invalid {
                name,
                reason: error.to_string(),
            })
    };
    let clock = L1SlotClock::new(number(ENV_GENESIS_TIMESTAMP)?, number(ENV_SLOT_SECONDS)?)?;
    Ok((clock, read(ENV_BLOB_FORKS)?.parse()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_fork_bounds_one_transaction_by_both_caps() {
        let schedule: BlobSchedule = "100:6:9:eip4844, 200:6:2:eip7594".parse().unwrap();
        assert_eq!(schedule.max_blobs_at(99), None);
        assert_eq!(schedule.max_blobs_at(100), Some(6));
        assert_eq!(schedule.max_blobs_at(199), Some(6));
        assert_eq!(schedule.max_blobs_at(200), Some(2));
        assert_eq!(
            schedule.fork_at(250).unwrap().sidecar_encoding,
            BlobSidecarEncoding::Eip7594,
        );
        assert_eq!(
            schedule.fork_at(150).unwrap().sidecar_encoding,
            BlobSidecarEncoding::Eip4844,
        );
    }

    #[test]
    fn fork_order_in_the_variable_does_not_matter() {
        let forward: BlobSchedule = "0:6:6:eip4844,10:9:9:eip7594".parse().unwrap();
        let reversed: BlobSchedule = "10:9:9:eip7594,0:6:6:eip4844".parse().unwrap();
        assert_eq!(forward, reversed);
    }

    #[test]
    fn malformed_schedules_are_rejected() {
        for invalid in [
            "",
            "0:6:6",
            "0:6:6:eip4844:extra",
            "0:0:6:eip4844",
            "0:6:0:eip4844",
            "0:6:6:blob",
            "x:6:6:eip4844",
            "0:6:6:eip4844,0:9:9:eip7594",
        ] {
            assert!(invalid.parse::<BlobSchedule>().is_err(), "{invalid:?}");
        }
    }

    #[test]
    fn slot_clock_accepts_only_slot_starts_after_genesis() {
        let clock = L1SlotClock::new(100, 12).unwrap();
        assert!(clock.is_slot_start(100));
        assert!(clock.is_slot_start(124));
        assert!(!clock.is_slot_start(125));
        assert!(!clock.is_slot_start(88));
        assert!(L1SlotClock::new(0, 0).is_err());

        assert_eq!(clock.next_slot_after(100), 112);
        assert_eq!(clock.next_slot_after(111), 112);
        assert_eq!(clock.next_slot_after(112), 124);
        assert_eq!(clock.next_slot_after(50), 100);
    }
}
