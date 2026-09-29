use codex_protocol::protocol::EventMsg;
use codex_rollout::RolloutItem;

#[derive(Debug, thiserror::Error)]
pub enum PendingTurnError {
    #[error("terminal turn event is missing its turn id")]
    TerminalTurnMissingId,
    #[error("one append batch contains multiple terminal turn events")]
    MultipleTerminalEvents,
}

#[derive(Debug)]
pub struct SealedTurn {
    pub turn_id: String,
    pub items: Vec<RolloutItem>,
}

/// RAM-only accumulation for the currently open logical turn.
///
/// FORK-RAM: Nothing stored here is durable. The object collects canonical
/// RolloutItem values until upstream emits TurnComplete or TurnAborted, at
/// which point ownership moves into one immutable terminal frame.
///
/// A partially completed turn disappearing with the process is an intentional
/// fork policy. Calling it "almost durable" would not improve recovery.
#[derive(Debug, Default)]
pub struct PendingTurn {
    items: Vec<RolloutItem>,
}

impl PendingTurn {
    pub fn push(
        &mut self,
        items: &[RolloutItem],
    ) -> Result<Option<SealedTurn>, PendingTurnError> {
        let mut terminal_turn_id = None;

        for item in items {
            let candidate = match item {
                RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => {
                    Some(event.turn_id.clone())
                }
                RolloutItem::EventMsg(EventMsg::TurnAborted(event)) => {
                    Some(
                        event
                            .turn_id
                            .clone()
                            .ok_or(PendingTurnError::TerminalTurnMissingId)?,
                    )
                }
                _ => None,
            };

            if let Some(candidate) = candidate {
                if terminal_turn_id.replace(candidate).is_some() {
                    return Err(PendingTurnError::MultipleTerminalEvents);
                }
            }
        }

        self.items.extend(items.iter().cloned());

        let Some(turn_id) = terminal_turn_id else {
            return Ok(None);
        };

        Ok(Some(SealedTurn {
            turn_id,
            items: std::mem::take(&mut self.items),
        }))
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::protocol::TurnCompleteEvent;

    #[test]
    fn ordinary_items_remain_ram_only_until_terminal_event() {
        let mut pending = PendingTurn::default();
        assert!(pending.push(&[]).expect("empty append should work").is_none());
        assert!(pending.is_empty());
    }

    #[test]
    fn terminal_event_seals_the_accumulated_turn() {
        let mut pending = PendingTurn::default();
        let terminal = RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
            turn_id: "turn-1".to_string(),
            last_agent_message: None,
            error: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
            time_to_first_token_ms: None,
        }));

        let sealed = pending
            .push(&[terminal])
            .expect("terminal append should work")
            .expect("terminal event should seal a turn");

        assert_eq!(sealed.turn_id, "turn-1");
        assert_eq!(sealed.items.len(), 1);
        assert!(pending.is_empty());
    }
}
