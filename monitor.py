import argparse, socket, sys, threading, time
from dataclasses import dataclass, field
from typing import Dict, List, Set

from blockchain import Block, GENESIS_HASH, chunk_transactions, load_transactions, make_block
from protocol import ConnectionClosed, JsonLineReader, encode_business_message, parse_business_message, send_json


@dataclass
class ConsensusRound:
    block: Block
    started_at: float
    votes: Dict[str, str] = field(default_factory=dict)
    errors: Dict[str, List[str]] = field(default_factory=dict)
    decided: bool = False


class MonitorClient:
    def __init__(
        self,
        host: str,
        port: int,
        validators: List[str],
        difficulty: int,
        block_size: int,
        timeout: float,
    ):
        self.name = "Monitor"
        self.host = host
        self.port = port
        self.validators = validators
        self.difficulty = difficulty
        self.block_size = block_size
        self.timeout = timeout
        self.sock: socket.socket
        self.reader: JsonLineReader
        self.ledger: List[Block] = []
        self.rounds: Dict[str, ConsensusRound] = {}
        self.connected_nodes: Set[str] = set()
        self.lock = threading.Lock()
        self.running = threading.Event()

    @property
    def quorum(self) -> int:
        return (len(self.validators) // 2) + 1

    @property
    def last_hash(self) -> str:
        return self.ledger[-1].hash if self.ledger else GENESIS_HASH

    def connect(self) -> None:
        self.running.set()
        self.sock = socket.create_connection((self.host, self.port))
        self.reader = JsonLineReader(self.sock)
        send_json(self.sock, {"type": "register", "name": self.name})
        listener = threading.Thread(target=self.listen, daemon=True)
        listener.start()
        print(f"[MONITOR] Conectado. Validadores configurados={self.validators}. Quórum={self.quorum}", flush=True)

    def listen(self) -> None:
        try:
            while self.running.is_set():
                event = self.reader.read()
                self.handle_event(event)
        except ConnectionClosed:
            print("[MONITOR] Conexión cerrada por el servidor", flush=True)
        finally:
            self.running.clear()

    def handle_event(self, event: dict) -> None:
        event_type = event.get("type")
        if event_type == "chat":
            payload = parse_business_message(str(event.get("text", "")))
            if payload and payload.get("kind") == "VOTE":
                self.handle_vote(payload)
            else:
                print(f"[CHAT] {event.get('from')}: {event.get('text')}", flush=True)
        elif event_type == "system":
            text = str(event.get("text", ""))
            self.update_connected_nodes(text)
            print(f"[SISTEMA] {text}", flush=True)
        elif event_type == "error":
            print(f"[ERROR] {event.get('text')}", flush=True)
        elif event_type == "private":
            print(f"[PRIVADO] {event.get('from')}: {event.get('text')}", flush=True)

    def update_connected_nodes(self, text: str) -> None:
        if text.endswith(" se conectó"):
            self.connected_nodes.add(text.replace(" se conectó", ""))
        elif text.endswith(" se desconectó"):
            self.connected_nodes.discard(text.replace(" se desconectó", ""))

    def handle_vote(self, payload: dict) -> None:
        block_id = str(payload.get("block_id"))
        validator = str(payload.get("validator"))
        vote = str(payload.get("vote"))

        with self.lock:
            round_state = self.rounds.get(block_id)
            if round_state is None or round_state.decided:
                return
            if validator not in self.validators:
                print(f"[MONITOR] Voto ignorado de nodo no configurado: {validator}", flush=True)
                return

            round_state.votes[validator] = vote
            round_state.errors[validator] = [str(err) for err in payload.get("errors", [])]
            ok_votes = sum(1 for value in round_state.votes.values() if value == "BLOQUE_OK")
            bad_votes = sum(1 for value in round_state.votes.values() if value == "BLOQUE_INVALIDO")

            print(
                f"[MONITOR] Voto recibido {validator}->{vote} para {block_id}. "
                f"OK={ok_votes}, INVALIDO={bad_votes}, requeridos={self.quorum}",
                flush=True,
            )

            if ok_votes >= self.quorum:
                self.accept_round(round_state)
            elif bad_votes >= self.quorum:
                self.reject_round(round_state, "Mayoría de votos inválidos")

    def accept_round(self, round_state: ConsensusRound) -> None:
        round_state.decided = True
        block = round_state.block
        latency = time.time() - round_state.started_at
        fork = block.previous_hash != self.last_hash

        if not fork:
            self.ledger.append(block)

        announcement = {
            "kind": "CONSENSUS_REACHED",
            "block_id": block.id,
            "accepted": not fork,
            "hash": block.hash,
            "height": len(self.ledger),
            "latency_seconds": round(latency, 4),
            "votes": round_state.votes,
            "fork_detected": fork,
        }
        send_json(self.sock, {"type": "chat", "text": encode_business_message(announcement)})

        if fork:
            print(f"[MONITOR] FORK detectado: {block.id} no fue insertado.", flush=True)
        else:
            print(f"[MONITOR] CONSENSO ALCANZADO para {block.id}. Latencia={latency:.4f}s", flush=True)
            self.print_ledger()

    def reject_round(self, round_state: ConsensusRound, reason: str) -> None:
        round_state.decided = True
        latency = time.time() - round_state.started_at
        announcement = {
            "kind": "CONSENSUS_REACHED",
            "block_id": round_state.block.id,
            "accepted": False,
            "hash": round_state.block.hash,
            "height": len(self.ledger),
            "latency_seconds": round(latency, 4),
            "votes": round_state.votes,
            "reason": reason,
        }
        send_json(self.sock, {"type": "chat", "text": encode_business_message(announcement)})
        print(f"[MONITOR] Bloque {round_state.block.id} rechazado: {reason}", flush=True)
        for validator, errors in round_state.errors.items():
            if errors:
                print(f"  - {validator}: {'; '.join(errors)}", flush=True)

    def load_and_process(self, path: str) -> None:
        transactions = load_transactions(path)
        groups = chunk_transactions(transactions, self.block_size)
        print(f"[MONITOR] {len(transactions)} transacciones cargadas en {len(groups)} bloques.", flush=True)

        for index, group in enumerate(groups, start=len(self.ledger) + 1):
            block = make_block(index, group, self.last_hash, self.difficulty)
            self.propose_block(block)
            self.wait_for_decision(block.id)

    def propose_block(self, block: Block) -> None:
        round_state = ConsensusRound(block=block, started_at=time.time())
        with self.lock:
            self.rounds[block.id] = round_state

        proposal = {
            "kind": "BLOCK_PROPOSAL",
            "block": block.to_dict(),
            "difficulty": self.difficulty,
            "created_by": self.name,
        }

        print(f"[MONITOR] Proponiendo {block.id} hash={block.hash[:16]}... a {self.validators}", flush=True)
        for validator in self.validators:
            send_json(
                self.sock,
                {"type": "private", "target": validator, "text": encode_business_message(proposal)},
            )

    def wait_for_decision(self, block_id: str) -> None:
        deadline = time.time() + self.timeout
        while time.time() < deadline:
            with self.lock:
                round_state = self.rounds[block_id]
                if round_state.decided:
                    return
            time.sleep(0.1)

        with self.lock:
            round_state = self.rounds[block_id]
            if not round_state.decided:
                self.reject_round(round_state, f"Timeout de {self.timeout}s sin quórum")

    def print_ledger(self) -> None:
        print("\n[LEDGER GLOBAL]", flush=True)
        if not self.ledger:
            print("  Ledger vacío", flush=True)
        for position, block in enumerate(self.ledger, start=1):
            print(
                f"  {position}. {block.id} hash={block.hash[:16]} prev={block.previous_hash[:16]} "
                f"tx={len(block.transactions)} nonce={block.nonce}",
                flush=True,
            )
        print("", flush=True)

    def repl(self) -> None:
        print("Comandos: cargar <archivo>, estado, salir", flush=True)
        while self.running.is_set():
            try:
                command = input("monitor> ").strip()
            except (EOFError, KeyboardInterrupt):
                command = "salir"

            if not command:
                continue
            if command == "salir":
                self.running.clear()
                self.sock.close()
                return
            if command == "estado":
                self.print_ledger()
                continue
            if command.startswith("cargar "):
                self.load_and_process(command.split(" ", 1)[1])
                continue
            print("Comando no reconocido.", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Nodo monitor del consenso distribuido.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5050)
    parser.add_argument("--validators", required=True, help="Lista separada por comas, ej: V1,V2,V3")
    parser.add_argument("--difficulty", type=int, default=3)
    parser.add_argument("--block-size", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--auto", help="Archivo de transacciones a procesar automáticamente.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    validators = [name.strip() for name in args.validators.split(",") if name.strip()]
    if not validators:
        print("Debe indicar al menos un validador.", file=sys.stderr)
        sys.exit(1)

    monitor = MonitorClient(
        host=args.host,
        port=args.port,
        validators=validators,
        difficulty=args.difficulty,
        block_size=args.block_size,
        timeout=args.timeout,
    )
    monitor.connect()
    time.sleep(1.0)

    if args.auto:
        monitor.load_and_process(args.auto)
        time.sleep(1.0)
        monitor.running.clear()
        monitor.sock.close()
    else:
        monitor.repl()

