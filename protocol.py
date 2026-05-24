import json
import socket
from typing import Any, Dict, Optional


class ConnectionClosed(Exception):
    """Raised when the remote peer closes the TCP connection."""


def send_json(sock: socket.socket, message: Dict[str, Any]) -> None:
    """Send a JSON message using newline framing."""
    payload = json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n"
    sock.sendall(payload.encode("utf-8"))


class JsonLineReader:
    """Incremental reader for newline-delimited JSON messages."""

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.buffer = b""

    def read(self) -> Dict[str, Any]:
        while b"\n" not in self.buffer:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionClosed()
            self.buffer += chunk

        line, self.buffer = self.buffer.split(b"\n", 1)
        if not line.strip():
            return self.read()

        try:
            decoded = json.loads(line.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Mensaje JSON inválido: {line!r}") from exc

        if not isinstance(decoded, dict):
            raise ValueError("El mensaje JSON debe ser un objeto.")
        return decoded


def parse_business_message(text: str) -> Optional[Dict[str, Any]]:
    """Return a business JSON payload if text contains one, otherwise None."""
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def encode_business_message(message: Dict[str, Any]) -> str:
    """Encode a business-level message as text for the chat layer."""
    return json.dumps(message, ensure_ascii=False, separators=(",", ":"))

