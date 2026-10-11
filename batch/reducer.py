import hashlib
import json


def compute_state_hash(state: dict) -> str:
    """Computes deterministic SHA-256 state hash over canonical business properties."""
    keys_to_hash = [
        "transaction_id",
        "run_id",
        "card_id",
        "merchant_id",
        "mcc",
        "issuer_bank_id",
        "acquirer_bank_id",
        "currency",
        "authorized_amount",
        "decision_id",
        "decision",
        "decision_source",
        "status",
        "latest_applied_sequence",
        "cleared_amount",
        "reversed_amount",
        "charged_back_amount",
        "original_event_time_ms",
        "authorization_date",
    ]
    canonical = {k: state.get(k) for k in sorted(keys_to_hash)}
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def is_state_changed(current_state: dict | None, new_state: dict | None) -> bool:
    """Returns True if new_state represents a materialized state change compared to current_state."""
    if current_state is None and new_state is not None:
        return True
    if current_state is not None and new_state is None:
        return True
    if current_state is None and new_state is None:
        return False
    return current_state.get("state_hash") != new_state.get("state_hash")


class TransactionReducer:
    """Reduces chronological lifecycle events for a transaction into a canonical state snapshot."""

    def __init__(self):
        self.quarantine_count = 0
        self.quarantined_events = []
        self.unapplied_count = 0
        self.unapplied_events = []

    def _quarantine(self, evt: dict) -> None:
        self.quarantine_count += 1
        self.quarantined_events.append(evt)

    def reduce_transaction(
        self,
        current_state: dict | None,
        events: list[dict],
    ) -> tuple[dict | None, list[dict]]:
        """Sorts events by logical_seq and applies contiguous state transitions."""
        if not events and current_state is None:
            return None, []

        sorted_events = sorted(events, key=lambda e: e.get("logical_seq", 0))
        state = dict(current_state) if current_state else None
        applied_events = []

        for evt in sorted_events:
            seq = evt.get("logical_seq", 0)
            event_type = evt.get("event_type")
            payload = evt.get("payload")
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = {}
            elif not isinstance(payload, dict):
                payload = {}

            # Seq 1: AUTH_DECISION
            if seq == 1 or event_type == "AUTH_DECISION":
                if state is not None:
                    continue  # already initialized
                decision = payload.get("decision", "DECLINE")
                status = "AUTHORIZED" if decision == "APPROVE" else "DECLINED"
                event_time_ms = evt.get("event_time_ms", 0)
                amount = payload.get("amount", 0)

                state = {
                    "transaction_id": evt.get("transaction_id", ""),
                    "run_id": evt.get("run_id", ""),
                    "card_id": payload.get("card_id", ""),
                    "merchant_id": payload.get("merchant_id", ""),
                    "mcc": payload.get("mcc", ""),
                    "issuer_bank_id": payload.get("issuer_bank_id", ""),
                    "acquirer_bank_id": payload.get("acquirer_bank_id", ""),
                    "currency": payload.get("currency", "USD"),
                    "authorized_amount": amount,
                    "decision_id": evt.get("event_id", ""),
                    "decision": decision,
                    "decision_source": payload.get("decision_source", "ENGINE"),
                    "decision_reason": payload.get("decision_reason"),
                    "decision_flags": payload.get("decision_flags", []),
                    "rule_version": payload.get("rule_version"),
                    "reference_version": payload.get("reference_version"),
                    "status": status,
                    "latest_applied_sequence": 1,
                    "cleared_amount": 0,
                    "reversed_amount": 0,
                    "charged_back_amount": 0,
                    "original_event_time_ms": event_time_ms,
                    "latest_lifecycle_time_ms": event_time_ms,
                    "authorization_date": payload.get("authorization_date", "1970-01-01"),
                    "state_version": 1,
                }
                state["state_hash"] = compute_state_hash(state)
                applied_events.append(evt)
                continue

            if state is None:
                # Out-of-order: event arrived before auth decision
                self.unapplied_count += 1
                self.unapplied_events.append(evt)
                continue

            current_status = state["status"]
            current_seq = state["latest_applied_sequence"]

            # Already applied:
            if seq <= current_seq:
                continue

            # Invalid transition detections:
            # 1. DECLINED transaction cannot be cleared, reversed, or charged back
            if current_status == "DECLINED":
                self._quarantine(evt)
                continue

            # 2. REVERSED transaction cannot accept CLEAR, REVERSE, or CHARGEBACK
            if current_status == "REVERSED":
                self._quarantine(evt)
                continue

            # 3. CHARGED_BACK transaction cannot accept REVERSE or CLEAR
            if current_status == "CHARGED_BACK":
                self._quarantine(evt)
                continue

            # Contiguous transitions:
            if seq == 2 and current_seq == 1 and current_status == "AUTHORIZED":
                if event_type == "CLEAR":
                    state["status"] = "CLEARED"
                    state["latest_applied_sequence"] = 2
                    state["cleared_amount"] = state["authorized_amount"]
                    state["latest_lifecycle_time_ms"] = evt.get(
                        "event_time_ms", state["latest_lifecycle_time_ms"]
                    )
                    new_hash = compute_state_hash(state)
                    if new_hash != state.get("state_hash"):
                        state["state_version"] += 1
                        state["state_hash"] = new_hash
                    applied_events.append(evt)
                elif event_type == "REVERSE":
                    state["status"] = "REVERSED"
                    state["latest_applied_sequence"] = 2
                    state["reversed_amount"] = state["authorized_amount"]
                    state["latest_lifecycle_time_ms"] = evt.get(
                        "event_time_ms", state["latest_lifecycle_time_ms"]
                    )
                    new_hash = compute_state_hash(state)
                    if new_hash != state.get("state_hash"):
                        state["state_version"] += 1
                        state["state_hash"] = new_hash
                    applied_events.append(evt)
                else:
                    self._quarantine(evt)

            elif seq == 3 and current_seq == 2 and current_status == "CLEARED":
                if event_type == "REVERSE":
                    state["status"] = "REVERSED"
                    state["latest_applied_sequence"] = 3
                    state["reversed_amount"] = state["cleared_amount"]
                    state["latest_lifecycle_time_ms"] = evt.get(
                        "event_time_ms", state["latest_lifecycle_time_ms"]
                    )
                    new_hash = compute_state_hash(state)
                    if new_hash != state.get("state_hash"):
                        state["state_version"] += 1
                        state["state_hash"] = new_hash
                    applied_events.append(evt)
                elif event_type == "CHARGEBACK":
                    state["status"] = "CHARGED_BACK"
                    state["latest_applied_sequence"] = 3
                    state["charged_back_amount"] = state["cleared_amount"]
                    state["latest_lifecycle_time_ms"] = evt.get(
                        "event_time_ms", state["latest_lifecycle_time_ms"]
                    )
                    new_hash = compute_state_hash(state)
                    if new_hash != state.get("state_hash"):
                        state["state_version"] += 1
                        state["state_hash"] = new_hash
                    applied_events.append(evt)
                else:
                    self._quarantine(evt)
            elif seq > current_seq + 1:
                # Out-of-order event (gap in sequence)
                self.unapplied_count += 1
                self.unapplied_events.append(evt)
            else:
                self._quarantine(evt)

        return state, applied_events


def upsert_transactions(
    spark,
    transactions_df,
    table_path: str | None = None,
) -> str:
    """Upserts transaction state snapshots to TABLE_TRANSACTIONS using Hudi COW upsert."""
    from batch.hudi_tables import TABLE_TRANSACTIONS, write_hudi_table

    target_path = table_path or TABLE_TRANSACTIONS
    write_hudi_table(
        transactions_df,
        "transactions",
        path=target_path,
        operation="upsert",
    )
    res_df = spark.read.format("hudi").load(target_path)
    latest_instant = res_df.agg({"_hoodie_commit_time": "max"}).collect()[0][0]
    return str(latest_instant)
