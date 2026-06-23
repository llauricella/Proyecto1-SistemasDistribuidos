import argparse, socket, sys, threading, time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set

from blockchain import Block, GENESIS_HASH, chunk_transactions, load_transactions, make_block
from protocol import ConnectionClosed, JsonLineReader, encode_business_message, parse_business_message, send_json


@dataclass
class ConsensusRound:
    block: Block
    started_at: float
    quorum: int
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
        observer: Optional[Callable[[str, dict], None]] = None,
    ):
        self.name = "Monitor"
        self.host = host
        self.port = port
        self.validators = validators
        self.difficulty = difficulty
        self.block_size = block_size
        self.timeout = timeout
        self.observer = observer
        self.sock: socket.socket
        self.reader: JsonLineReader
        self.ledger: List[Block] = []
        self.rounds: Dict[str, ConsensusRound] = {}
        self.connected_nodes: Set[str] = set()
        self.active_validators: Set[str] = set()
        self.lock = threading.Lock()
        self.running = threading.Event()

    # ----- Observador (para GUI). No afecta la salida de consola. -----
    def _emit(self, kind: str, **data) -> None:
        if self.observer:
            try:
                self.observer(kind, data)
            except Exception:
                pass

    def _log(self, text: str) -> None:
        print(text, flush=True)
        self._emit("log", text=text)

    @property
    def quorum(self) -> int:
        """Quórum dinámico: mayoría simple sobre validadores activos (configurados y conectados)."""
        base = self.active_validators & set(self.validators)
        if not base:
            base = set(self.validators)
        return (len(base) // 2) + 1

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
        # Descubrimiento de presencia: pregunta quién está activo.
        send_json(self.sock, {"type": "chat", "text": encode_business_message({"kind": "WHO"})})
        self._log(f"[MONITOR] Conectado. Validadores configurados={self.validators}. Quórum={self.quorum}")
        self._emit("connected", validators=list(self.validators), quorum=self.quorum)

    def listen(self) -> None:
        try:
            while self.running.is_set():
                event = self.reader.read()
                self.handle_event(event)
        except ConnectionClosed:
            self._log("[MONITOR] Conexión cerrada por el servidor")
        finally:
            self.running.clear()

    def handle_event(self, event: dict) -> None:
        event_type = event.get("type")
        if event_type == "chat":
            payload = parse_business_message(str(event.get("text", "")))
            if payload and payload.get("kind") == "VOTE":
                self.handle_vote(payload)
            elif payload and payload.get("kind") == "HELLO":
                self.register_active(str(payload.get("node", "")))
            else:
                self._log(f"[CHAT] {event.get('from')}: {event.get('text')}")
        elif event_type == "system":
            text = str(event.get("text", ""))
            self.update_connected_nodes(text)
            self._log(f"[SISTEMA] {text}")
        elif event_type == "error":
            self._log(f"[ERROR] {event.get('text')}")
        elif event_type == "private":
            self._log(f"[PRIVADO] {event.get('from')}: {event.get('text')}")

    def register_active(self, node: str) -> None:
        if not node:
            return
        with self.lock:
            self.connected_nodes.add(node)
            if node in self.validators:
                self.active_validators.add(node)
        self._emit("nodes", connected=sorted(self.connected_nodes),
                   active=sorted(self.active_validators), quorum=self.quorum)

    def update_connected_nodes(self, text: str) -> None:
        changed = False
        with self.lock:
            if text.endswith(" se conectó"):
                node = text.replace(" se conectó", "")
                self.connected_nodes.add(node)
                if node in self.validators:
                    self.active_validators.add(node)
                changed = True
            elif text.endswith(" se desconectó"):
                node = text.replace(" se desconectó", "")
                self.connected_nodes.discard(node)
                self.active_validators.discard(node)
                changed = True
        if changed:
            self._emit("nodes", connected=sorted(self.connected_nodes),
                       active=sorted(self.active_validators), quorum=self.quorum)

    def handle_vote(self, payload: dict) -> None:
        block_id = str(payload.get("block_id"))
        validator = str(payload.get("validator"))
        vote = str(payload.get("vote"))

        with self.lock:
            round_state = self.rounds.get(block_id)
            if round_state is None or round_state.decided:
                return
            if validator not in self.validators:
                self._log(f"[MONITOR] Voto ignorado de nodo no configurado: {validator}")
                return

            round_state.votes[validator] = vote
            round_state.errors[validator] = [str(err) for err in payload.get("errors", [])]
            ok_votes = sum(1 for value in round_state.votes.values() if value == "BLOQUE_OK")
            bad_votes = sum(1 for value in round_state.votes.values() if value == "BLOQUE_INVALIDO")
            quorum = self.quorum
            round_state.quorum = quorum

            self._log(
                f"[MONITOR] Voto recibido {validator}->{vote} para {block_id}. "
                f"OK={ok_votes}, INVALIDO={bad_votes}, requeridos={quorum}"
            )
            self._emit("vote", block_id=block_id, validator=validator, vote=vote,
                       ok=ok_votes, bad=bad_votes, quorum=quorum,
                       errors=round_state.errors[validator])

            if ok_votes >= quorum:
                self.accept_round(round_state)
            elif bad_votes >= quorum:
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
            self._log(f"[MONITOR] FORK detectado: {block.id} no fue insertado.")
        else:
            self._log(f"[MONITOR] CONSENSO ALCANZADO para {block.id}. Latencia={latency:.4f}s")
            self.print_ledger()

        self._emit("decision", announcement=announcement, accepted=not fork, fork=fork,
                   reason=None, ledger=[b.to_dict() for b in self.ledger])

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
        self._log(f"[MONITOR] Bloque {round_state.block.id} rechazado: {reason}")
        for validator, errors in round_state.errors.items():
            if errors:
                self._log(f"  - {validator}: {'; '.join(errors)}")

        self._emit("decision", announcement=announcement, accepted=False, fork=False,
                   reason=reason, ledger=[b.to_dict() for b in self.ledger])

    def load_and_process(self, path: str) -> None:
        transactions = load_transactions(path)
        groups = chunk_transactions(transactions, self.block_size)

        self._log(f"[MONITOR] {len(transactions)} transacciones cargadas en {len(groups)} bloques.")
        self._emit("loaded", n_tx=len(transactions), n_blocks=len(groups))

        for index, group in enumerate(groups, start=len(self.ledger) + 1):
            block = make_block(index, group, self.last_hash, self.difficulty)
            self.propose_block(block)
            self.wait_for_decision(block.id)

    def propose_block(self, block: Block) -> None:
        round_state = ConsensusRound(block=block, started_at=time.time(), quorum=self.quorum)
        with self.lock:
            self.rounds[block.id] = round_state

        proposal = {
            "kind": "BLOCK_PROPOSAL",
            "block": block.to_dict(),
            "difficulty": self.difficulty,
            "created_by": self.name,
        }

        self._log(f"[MONITOR] Proponiendo {block.id} hash={block.hash[:16]}... a {self.validators}")
        self._emit("proposal", block=block.to_dict(), validators=list(self.validators), quorum=round_state.quorum)
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
        self._log("\n[LEDGER GLOBAL]")
        if not self.ledger:
            self._log("  Ledger vacío")
        for position, block in enumerate(self.ledger, start=1):
            self._log(
                f"  {position}. {block.id} hash={block.hash[:16]} prev={block.previous_hash[:16]} "
                f"tx={len(block.transactions)} nonce={block.nonce}"
            )
        self._log("")

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
