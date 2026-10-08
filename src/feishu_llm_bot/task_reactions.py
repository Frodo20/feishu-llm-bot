"""Small persistent UI state for varied receipts and one replaceable task-status reaction."""

import hashlib
import logging

RECEIPT_EMOJIS = ("OK", "THUMBSUP", "FISTBUMP")
LOGGER = logging.getLogger(__name__)


def receipt_emoji(message_id):
    # Stable across retries/restarts, unlike random choices made at send time.
    return RECEIPT_EMOJIS[int(hashlib.sha256(message_id.encode()).hexdigest()[:8], 16) % 3]


def desired_status_reaction(state, now):
    if not state.get("status_reactions_enabled"):
        return None
    task = state.get("task_state")
    if state.get("status") == "replied":
        if task == "cancelled":
            return "THANKS"
        if state.get("business_outcome") == "partial":
            return "THINKING"
        if task == "succeeded":
            return "DONE"
        if task in {"failed", "suspended"}:
            return "ERROR"
    if task in {"running", "retry_wait"} and now - state["created_at"] >= 3:
        return "THINKING"
    if task == "queued" and state.get("status_reaction_target"):
        return "THINKING"
    return None


def reaction_pending(state, now):
    desired = desired_status_reaction(state, now)
    return bool(
        desired
        and not state.get("status_reaction_disabled")
        and desired != state.get("status_reaction_emoji")
    )


def advance_status_reaction(client, state, now):
    """At most one API request. The caller persists state and shares the sender call budget."""
    desired = desired_status_reaction(state, now)
    if not reaction_pending(state, now):
        return False
    if state.get("status_reaction_target") != desired:
        state.update(
            status_reaction_target=desired, status_reaction_attempts=0, status_reaction_retry_at=0
        )
    if now < state.get("status_reaction_retry_at", 0):
        return False
    try:
        if state.get("status_reaction_id"):
            # Remove only the reaction created by this bot and saved for this message.
            client.remove_reaction(state["message_id"], state["status_reaction_id"])
            state.update(status_reaction_id=None, status_reaction_emoji=None)
        else:
            ident = client.add_reaction(state["message_id"], desired)
            state.update(status_reaction_id=ident, status_reaction_emoji=desired)
        state.update(status_reaction_attempts=0, status_reaction_retry_at=0)
    except Exception as exc:
        count = state.get("status_reaction_attempts", 0) + 1
        state.update(status_reaction_attempts=count, status_reaction_retry_at=now + 30)
        if count >= 3:
            # Cosmetic failure must never prevent durable result delivery or hold work open forever.
            state["status_reaction_disabled"] = True
        LOGGER.warning("Task status reaction unavailable (%s)", type(exc).__name__)
    return True
