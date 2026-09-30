use std::collections::BTreeSet;

use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;

/// Capabilities proven by one thread-authority generation.
///
/// AUTHORITY-NOTE: Capabilities are explicit because "the thread exists" does
/// not answer whether it is live-addressable, restart-resumable, or a valid
/// fork source. Collapsing those facts into one boolean is how routers learn
/// interpretive dance.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadAuthorityCapability {
    LiveDelivery,
    ColdResume,
    ForkSource,
    AdminMutation,
}

/// Storage-side authority identity for one logical thread.
///
/// This intentionally does not contain a socket path or RPC method. Transport
/// binding belongs to the host layer. A filesystem pathname is placement; it is
/// not ontology, and we have already paid tuition for that lesson.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadAuthorityRefV1 {
    pub thread_id: ThreadId,
    pub authority_id: String,
    pub authority_generation: String,
    pub store_id: String,
    pub capabilities: BTreeSet<ThreadAuthorityCapability>,
    pub durable_sequence: Option<u64>,
    pub durable_record_digest: Option<[u8; 32]>,
}

impl ThreadAuthorityRefV1 {
    pub fn has(&self, capability: ThreadAuthorityCapability) -> bool {
        self.capabilities.contains(&capability)
    }
}

/// Host transport binding layered over storage authority.
///
/// AUTHORITY-NOTE: A router may change transport only by producing a new
/// explicit binding for the same authority generation (or an explicit
/// projection handled outside this type). It may not infer ownership from
/// thread_id because a UUID remains stubbornly unwilling to become a routing
/// table.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadAuthorityBindingV1 {
    pub authority: ThreadAuthorityRefV1,
    pub server_instance_id: String,
    pub endpoint_id: String,
    pub endpoint_generation: String,
    pub transport: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_do_not_imply_each_other() {
        let thread_id = ThreadId::new();
        let authority = ThreadAuthorityRefV1 {
            thread_id,
            authority_id: "runtime-a".to_string(),
            authority_generation: "gen-1".to_string(),
            store_id: "store-a".to_string(),
            capabilities: BTreeSet::from([ThreadAuthorityCapability::LiveDelivery]),
            durable_sequence: None,
            durable_record_digest: None,
        };

        assert!(authority.has(ThreadAuthorityCapability::LiveDelivery));
        assert!(!authority.has(ThreadAuthorityCapability::ColdResume));
    }
}
