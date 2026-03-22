use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

/// Status of a hypothesis lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum HypothesisStatus {
    Idle,
    Running,
    Passed,
    Failed,
    Stopped,
}

/// Operation type for adapter interactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum OpType {
    Put,
    Delete,
    DeleteRange,
    Cas,
    Get,
    RangeScan,
    List,
    Fence,
}
