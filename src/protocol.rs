use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const VERSION: u32 = 1;
pub const MAX_FRAME: usize = 64 * 1024;

#[derive(Deserialize)]
#[serde(untagged)]
pub enum Incoming {
    Completion(Completion),
    Request(Request),
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Completion {
    Chunk {
        call_id: u64,
        data: Value,
    },
    Complete {
        call_id: u64,
        #[serde(flatten)]
        outcome: Outcome,
    },
}

#[derive(Deserialize)]
pub struct Request {
    pub id: String,
    #[serde(flatten)]
    pub command: Command,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    Hello {
        protocol: u32,
    },
    List,
    Describe {
        generation: u64,
    },
    Watch,
    Register {
        #[serde(flatten)]
        interface: Interface,
        #[serde(default)]
        activation_notice: bool,
    },
    Promote {
        expected_active: Option<u64>,
    },
    Activate {
        generation: u64,
        expected_active: Option<u64>,
    },
    Call {
        service: String,
        generation: Option<u64>,
        interface_version: u32,
        method: String,
        params: Value,
        #[serde(default)]
        stream: bool,
    },
}

#[derive(Deserialize, Serialize)]
pub struct Interface {
    pub name: String,
    pub interface_version: u32,
    pub methods: BTreeMap<String, Method>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<InterfaceKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maintainer: Option<String>,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceKind {
    ModelProvider,
}

#[derive(Deserialize, Serialize)]
pub struct Method {
    pub description: String,
    pub params_schema: Value,
    pub result_schema: Value,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Ok { result: Value },
    Error { error: RpcError },
}

#[derive(Deserialize, Serialize)]
pub struct RpcError {
    pub code: String,
    pub message: String,
}

impl RpcError {
    pub fn new(code: &str, message: &str) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}
