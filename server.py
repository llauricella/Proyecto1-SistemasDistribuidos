import argparse
import socket
import threading
from typing import Dict, Optional

from protocol import ConnectionClosed, JsonLineReader, send_json


class ChatServer:
    """Central hub that only relays public and private chat messages."""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.clients: Dict[str, socket.socket] = {}
        self.lock = threading.Lock()
        self.running = threading.Event()

    def start(self) -> None:
        self.running.set()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.host, self.port))
            server_socket.listen()
            print(f"[SERVER] Escuchando en {self.host}:{self.port}", flush=True)

            while self.running.is_set():
                conn, address = server_socket.accept()
                thread = threading.Thread(target=self.handle_client, args=(conn, address), daemon=True)
                thread.start()

    def handle_client(self, conn: socket.socket, address) -> None:
        name: Optional[str] = None
        reader = JsonLineReader(conn)

        try:
            first_message = reader.read()
            if first_message.get("type") != "register" or not first_message.get("name"):
                send_json(conn, {"type": "error", "text": "Debe registrarse con {'type':'register','name':'...'}"})
                return

            requested_name = str(first_message["name"]).strip()
            with self.lock:
                if requested_name in self.clients:
                    send_json(conn, {"type": "error", "text": f"El nombre {requested_name!r} ya está conectado."})
                    return
                name = requested_name
                self.clients[name] = conn

            print(f"[SERVER] {name} conectado desde {address}", flush=True)
            send_json(conn, {"type": "system", "text": f"Registrado como {name}"})
            self.broadcast({"type": "system", "text": f"{name} se conectó"}, exclude=name)

            while True:
                message = reader.read()
                msg_type = message.get("type")
                if msg_type == "chat":
                    self.broadcast({"type": "chat", "from": name, "text": str(message.get("text", ""))})
                elif msg_type == "private":
                    target = str(message.get("target", "")).strip()
                    text = str(message.get("text", ""))
                    self.private_message(sender=name, target=target, text=text)
                else:
                    send_json(conn, {"type": "error", "text": f"Tipo de mensaje no soportado: {msg_type}"})

        except (ConnectionClosed, OSError):
            pass
        except Exception as exc:
            if name:
                print(f"[SERVER] Error con {name}: {exc}", flush=True)
        finally:
            if name:
                with self.lock:
                    self.clients.pop(name, None)
                self.broadcast({"type": "system", "text": f"{name} se desconectó"})
                print(f"[SERVER] {name} desconectado", flush=True)
            try:
                conn.close()
            except OSError:
                pass

    def broadcast(self, message: dict, exclude: Optional[str] = None) -> None:
        with self.lock:
            recipients = [(name, sock) for name, sock in self.clients.items() if name != exclude]

        for _, sock in recipients:
            try:
                send_json(sock, message)
            except OSError:
                continue

    def private_message(self, sender: str, target: str, text: str) -> None:
        with self.lock:
            target_socket = self.clients.get(target)
            sender_socket = self.clients.get(sender)

        if target_socket is None:
            if sender_socket is not None:
                send_json(sender_socket, {"type": "error", "text": f"No existe el nodo destino {target!r}"})
            return

        send_json(target_socket, {"type": "private", "from": sender, "text": text})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Servidor central de chat distribuido.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5050)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ChatServer(args.host, args.port).start()

