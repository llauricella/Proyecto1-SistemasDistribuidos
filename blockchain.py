import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple


GENESIS_HASH = "0" * 64


@dataclass
class Block:
    id: str
    transactions: List[str]
    previous_hash: str
    nonce: int = 0
    timestamp: float = field(default_factory=time.time)
    hash: str = ""

    def canonical_payload(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "transactions": self.transactions,
            "previous_hash": self.previous_hash,
            "nonce": self.nonce,
            "timestamp": self.timestamp,
        }

    def compute_hash(self) -> str:
        raw = json.dumps(self.canonical_payload(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def mine(self, difficulty: int) -> "Block":
        prefix = "0" * difficulty
        while True:
            digest = self.compute_hash()
            if digest.startswith(prefix):
                self.hash = digest
                return self
            self.nonce += 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "transactions": self.transactions,
            "previous_hash": self.previous_hash,
            "nonce": self.nonce,
            "timestamp": self.timestamp,
            "hash": self.hash,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Block":
        return cls(
            id=str(data["id"]),
            transactions=[str(item) for item in data["transactions"]],
            previous_hash=str(data["previous_hash"]),
            nonce=int(data["nonce"]),
            timestamp=float(data["timestamp"]),
            hash=str(data["hash"]),
        )


def validate_block(block: Block, expected_previous_hash: str, difficulty: int) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    calculated_hash = block.compute_hash()
    prefix = "0" * difficulty

    if block.previous_hash != expected_previous_hash:
        errors.append(
            f"previous_hash no coincide: esperado={expected_previous_hash[:12]} recibido={block.previous_hash[:12]}"
        )

    if calculated_hash != block.hash:
        errors.append(
            f"hash corrupto: calculado={calculated_hash[:12]} recibido={block.hash[:12]}"
        )

    if not block.hash.startswith(prefix):
        errors.append(f"acertijo no resuelto: hash no empieza por {prefix!r}")

    if not block.transactions:
        errors.append("bloque sin transacciones")

    return len(errors) == 0, errors


def load_transactions(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as file:
        return [line.strip() for line in file if line.strip() and not line.strip().startswith("#")]


def chunk_transactions(transactions: List[str], block_size: int) -> List[List[str]]:
    return [transactions[index : index + block_size] for index in range(0, len(transactions), block_size)]


def make_block(block_id: int, transactions: List[str], previous_hash: str, difficulty: int) -> Block:
    return Block(id=f"B{block_id:04d}", transactions=transactions, previous_hash=previous_hash).mine(difficulty)

