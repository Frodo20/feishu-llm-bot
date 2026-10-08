"""Health adapter for the pinned lark-oapi 1.7.3 WebSocket implementation."""
from __future__ import annotations


def websocket_connected(client: object) -> bool:
    # The SDK has no public initial-connect callback/status property. Its pinned
    # implementation stores the websockets connection in _conn; never read its URL.
    connection = getattr(client, "_conn", None)
    if connection is None:
        return False
    # websockets <=13 exposes open; newer versions expose a State enum.
    if hasattr(connection, "open"):
        return bool(connection.open)
    return getattr(getattr(connection, "state", None), "name", None) == "OPEN"
