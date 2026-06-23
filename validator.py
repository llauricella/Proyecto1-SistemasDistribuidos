import argparse
import random
import socket
import threading
import time
from typing import Dict, List
from blockchain import Block, GENESIS_HASH, validate_block
from protocol import ConnectionClosed, JsonLineReader, encode_business_message, parse_business_message, send_json


class ValidatorClient:
    def __init__(self, name: str, host: str, port: int, difficulty: int, fault_rate: float, delay: float):
        self.name = name
        self.host = host
        self.port = port
        self.difficulty = difficulty
        self.fault_rate = fault_rate
        self.delay = delay
        self.sock: socket.socket
        self.reader: JsonLineReader
        self.ledger: List[Block] = []
        self.pending: Dict[str, Block] = {}
        self.running = threading.Event()

    @property
    def last_hash(self) -> str:
        return self.ledger[-1].hash if self.ledger else GENESIS_HASH

    def announce_presence(self) -> None:
        """Anuncia al resto que este validador está activo (descubrimiento de presencia)."""
        send_json(self.sock, {"type": "chat", "text": encode_business_message({"kind": "HELLO", "node": self.name})})

    def start(self) -> None:
        self.running.set()
        self.sock = socket.create_connection((self.host, self.port))
        self.reader = JsonLineReader(self.sock)
        send_json(self.sock, {"type": "register", "name": self.name})
        print(f"[{self.name}] Conectado al servidor", flush=True)
        self.announce_presence()

        try:
            while self.running.is_set():
                event = self.reader.read()
                self.handle_event(event)
        except ConnectionClosed:
            print(f"[{self.name}] Conexión cerrada por el servidor", flush=True)
        finally:
            self.sock.close()

    def handle_event(self, event: dict) -> None:
        event_type = event.get("type")
        if event_type == "private":
            payload = parse_business_message(str(event.get("text", "")))
            if payload:
                self.handle_business_message(event.get("from", ""), payload)

        elif event_type == "chat":
            payload = parse_business_message(str(event.get("text", "")))
            if payload:
                kind = payload.get("kind")
                if kind == "CONSENSUS_REACHED":
                    self.handle_consensus(payload)
                elif kind == "WHO":
                    # El monitor pregunta quién está activo: respondemos con presencia.
                    self.announce_presence()

        elif event_type == "system":
            print(f"[{self.name}] SISTEMA: {event.get('text')}", flush=True)
        elif event_type == "error":
            print(f"[{self.name}] ERROR: {event.get('text')}", flush=True)

    def handle_business_message(self, sender: str, payload: dict) -> None:
        if payload.get("kind") != "BLOCK_PROPOSAL":
            return

        block = Block.from_dict(payload["block"])
        self.pending[block.id] = block
        print(f"[{self.name}] Bloque recibido por privado desde {sender}: {block.id}", flush=True)
        if self.delay > 0:
            time.sleep(self.delay)
        is_valid, errors = validate_block(block, self.last_hash, self.difficulty)
        if self.fault_rate > 0 and random.random() < self.fault_rate:
            is_valid = not is_valid
            errors.append("fallo simulado: voto invertido")

        vote = {
            "kind": "VOTE",
            "block_id": block.id,
            "validator": self.name,
            "vote": "BLOQUE_OK" if is_valid else "BLOQUE_INVALIDO",
            "hash": block.hash,
            "errors": errors,
            "timestamp": time.time(),
        }

        send_json(self.sock, {"type": "chat", "text": encode_business_message(vote)})
        print(f"[{self.name}] Voto emitido: {vote['vote']} para {block.id}", flush=True)

    def handle_consensus(self, payload: dict) -> None:
        block_id = str(payload.get("block_id"))
        accepted = bool(payload.get("accepted"))
        block = self.pending.pop(block_id, None)

        if accepted and block:
            if block.previous_hash == self.last_hash:
                self.ledger.append(block)
                print(f"[{self.name}] Ledger actualizado con {block.id}. Altura={len(self.ledger)}", flush=True)
            else:
                print(f"[{self.name}] FORK detectado al aplicar {block.id}", flush=True)
        else:
            print(f"[{self.name}] Consenso rechazó {block_id}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Nodo procesador validador.")
    parser.add_argument("--name", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5050)
    parser.add_argument("--difficulty", type=int, default=3)
    parser.add_argument("--fault-rate", type=float, default=0.0)
    parser.add_argument("--delay", type=float, default=0.0)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ValidatorClient(
        name=args.name,
        host=args.host,
        port=args.port,
        difficulty=args.difficulty,
        fault_rate=args.fault_rate,
        delay=args.delay,
    ).start()
