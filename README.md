# Sistema de Consenso y Validación de Bloques mediante un Chat Distribuido

Proyecto de Sistemas Distribuidos implementado en Python con `socket`, `threading`, `hashlib` y mensajes JSON sobre TCP.

## Roles del sistema

- `server.py`: servidor central tipo hub. Solo acepta conexiones, registra nombres, difunde mensajes públicos y reenvía mensajes privados con `/w <nodo>`. No contiene lógica de negocio, blockchain, hashes ni consenso.
- `monitor.py`: cliente monitor. Carga transacciones, crea bloques candidatos, los distribuye por mensajes privados, cuenta votos públicos, calcula quórum y consolida el ledger.
- `validator.py`: cliente procesador validador. Recibe bloques por privado, valida hash y acertijo criptográfico, vota públicamente y actualiza su copia del ledger cuando el monitor anuncia consenso.
- `protocol.py`: utilidades de framing JSON por TCP.
- `blockchain.py`: estructuras y funciones criptográficas compartidas por monitor y validadores.

## Requisitos

- Python 3.10 o superior.
- No requiere librerías externas.

## Ejecución manual

Abre una terminal para el servidor:

```bash
python server.py --host 127.0.0.1 --port 5050
```

Abre una terminal por cada validador:

```bash
python validator.py --name V1 --host 127.0.0.1 --port 5050
python validator.py --name V2 --host 127.0.0.1 --port 5050
python validator.py --name V3 --host 127.0.0.1 --port 5050
```

Abre una terminal para el monitor:

```bash
python monitor.py --host 127.0.0.1 --port 5050 --validators V1,V2,V3
```

Dentro del monitor puedes escribir:

```text
cargar data/transactions.txt
estado
salir
```

También puedes ejecutar el monitor automáticamente:

```bash
python monitor.py --host 127.0.0.1 --port 5050 --validators V1,V2,V3 --auto data/transactions.txt
```

## Prueba rápida automatizada

Desde la carpeta del proyecto:

```bash
python run_demo.py
```

El script levanta un servidor, tres validadores y el monitor en modo automático. Al final imprime un resumen del consenso y detiene todos los procesos.

## Protocolo de chat

El sistema usa JSON delimitado por salto de línea. Los clientes envían al servidor:

```json
{"type": "register", "name": "V1"}
{"type": "chat", "text": "mensaje publico"}
{"type": "private", "target": "V1", "text": "{\"kind\":\"BLOCK_PROPOSAL\",...}"}
```

El servidor entrega eventos normalizados:

```json
{"type": "chat", "from": "V1", "text": "..."}
{"type": "private", "from": "Monitor", "text": "..."}
{"type": "system", "text": "..."}
```

Los mensajes de negocio van encapsulados como JSON dentro de `text`, de modo que el servidor sigue siendo un chat genérico.

## Criterio criptográfico

Cada bloque contiene:

- `id`
- `transactions`
- `previous_hash`
- `nonce`
- `timestamp`
- `hash`

El hash se calcula con SHA-256 sobre el contenido canónico del bloque. Un bloque válido debe cumplir:

1. El hash calculado debe coincidir con el hash recibido.
2. El hash debe comenzar con una cantidad configurable de ceros (`difficulty`).
3. El `previous_hash` debe coincidir con el último hash aceptado por el nodo.

## Consenso y quórum

El monitor calcula el quórum dinámicamente como:

```text
floor(validadores_activos / 2) + 1
```

Un bloque se inserta en el ledger cuando recibe mayoría simple de votos `BLOQUE_OK`. Si gana `BLOQUE_INVALIDO` o expira el tiempo de espera, el bloque queda rechazado y se registra en el log.

## Fallos simulados

Los validadores soportan opciones para simular fallos:

```bash
python validator.py --name V2 --fault-rate 0.25
python validator.py --name V3 --delay 1.5
```

- `--fault-rate`: probabilidad de emitir un voto incorrecto.
- `--delay`: retraso artificial antes de votar.

Estas opciones ayudan a demostrar resiliencia ante nodos inconsistentes o lentos.

