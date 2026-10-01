use codex_protocol::protocol::EventMsg;
use codex_rollout::RolloutItem;

#[derive(Debug, thiserror::Error)]
pub enum PendingTurnError {
    #[error("terminal turn event is missing its turn id")]
    TerminalTurnMissingId,
    #[error("one append batch contains multiple terminal turn events")]
    MultipleTerminalEvents,
    #[error("cannot append more rollout items after terminal turn {turn_id} was sealed")]
    AppendAfterTerminal { turn_id: String },
    #[error("commit acknowledgement for {actual} does not match sealed turn {expected}")]
    CommitTurnMismatch { expected: String, actual: String },
    #[error("failed to encode terminal turn event: {0}")]
    TerminalEncoding(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalEventIdentity {
    pub turn_id: String,
    pub digest: [u8; 32],
}

/// Identify the one terminal event carried by an append batch.
///
/// The digest covers the serialized terminal RolloutItem, not the full turn.
/// That gives retry/idempotency handling a stable receipt even when a lost
/// acknowledgement causes only the terminal event to be replayed.
pub fn terminal_event_identity(
    items: &[RolloutItem],
) -> Result<Option<TerminalEventIdentity>, PendingTurnError> {
    let mut terminal = None;

    for item in items {
        let turn_id = match item {
            RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => Some(event.turn_id.clone()),
            RolloutItem::EventMsg(EventMsg::TurnAborted(event)) => Some(
                event
                    .turn_id
                    .clone()
                    .ok_or(PendingTurnError::TerminalTurnMissingId)?,
            ),
            _ => None,
        };

        let Some(turn_id) = turn_id else {
            continue;
        };
        if terminal.is_some() {
            return Err(PendingTurnError::MultipleTerminalEvents);
        }

        let encoded = serde_json::to_vec(item)?;
        terminal = Some(TerminalEventIdentity {
            turn_id,
            digest: *blake3::hash(&encoded).as_bytes(),
        });
    }

    Ok(terminal)
}

#[derive(Debug, Clone)]
pub struct SealedTurn {
    pub turn_id: String,
    pub items: Vec<RolloutItem>,
}

/// RAM-only accumulation for the currently open logical turn.
///
/// FORK-RAM: Nothing stored here is durable. Terminal detection changes the
/// state from open to sealed, but the canonical items remain resident until the
/// journal append succeeds and mark_committed() explicitly releases them.
///
/// Process loss before that acknowledgement loses the open/sealed turn by
/// design. Journal failure does not get to erase resident state merely because
/// the filesystem had a difficult afternoon.
#[derive(Debug, Default)]
pub struct PendingTurn {
    items: Vec<RolloutItem>,
    terminal_turn_id: Option<String>,
}

impl PendingTurn {
    pub fn push(&mut self, items: &[RolloutItem]) -> Result<(), PendingTurnError> {
        if let Some(turn_id) = self.terminal_turn_id.as_ref() {
            return Err(PendingTurnError::AppendAfterTerminal {
                turn_id: turn_id.clone(),
            });
        }

        let terminal = terminal_event_identity(items)?;

        self.items.extend(items.iter().cloned());
        self.terminal_turn_id = terminal.map(|terminal| terminal.turn_id);
        Ok(())
    }

    pub fn sealed(&self) -> Option<SealedTurn> {
        self.terminal_turn_id.as_ref().map(|turn_id| SealedTurn {
            turn_id: turn_id.clone(),
            items: self.items.clone(),
        })
    }

    pub fn mark_committed(&mut self, turn_id: &str) -> Result<(), PendingTurnError> {
        let Some(expected) = self.terminal_turn_id.as_ref() else {
            return Ok(());
        };
        if expected != turn_id {
            return Err(PendingTurnError::CommitTurnMismatch {
                expected: expected.clone(),
                actual: turn_id.to_string(),
            });
        }

        self.items.clear();
        self.terminal_turn_id = None;
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::protocol::TurnCompleteEvent;

    fn terminal(turn_id: &str) -> RolloutItem {
        RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
            turn_id: turn_id.to_string(),
            last_agent_message: None,
            error: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
            time_to_first_token_ms: None,
        }))
    }

    #[test]
    fn ordinary_items_remain_ram_only_until_terminal_event() {
        let mut pending = PendingTurn::default();
        pending.push(&[]).expect("empty append should work");
        assert!(pending.sealed().is_none());
        assert!(pending.is_empty());
    }

    #[test]
    fn terminal_event_seals_but_does_not_release_the_turn() {
        let mut pending = PendingTurn::default();
        pending.push(&[terminal("turn-1")]).expect("terminal append");

        let sealed = pending.sealed().expect("terminal event should seal a turn");
        assert_eq!(sealed.turn_id, "turn-1");
        assert_eq!(sealed.items.len(), 1);
        assert!(!pending.is_empty());

        // JOURNAL-NOTE: resident state advances only after the writer has
        // acknowledged the complete terminal frame.
        pending
            .mark_committed("turn-1")
            .expect("matching commit should clear the turn");
        assert!(pending.is_empty());
        assert!(pending.sealed().is_none());
    }

    #[test]
    fn sealed_turn_rejects_late_persistable_items() {
        let mut pending = PendingTurn::default();
        pending.push(&[terminal("turn-1")]).expect("terminal append");

        let err = pending
            .push(&[])
            .expect_err("sealed turn must reject later append attempts");
        assert!(matches!(
            err,
            PendingTurnError::AppendAfterTerminal { .. }
        ));
    }
}
