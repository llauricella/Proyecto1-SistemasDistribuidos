# Sistema de Consenso y Validación de Bloques mediante un Chat Distribuido

Proyecto de Sistemas Distribuidos en Python con `socket`, `threading`, `hashlib` y mensajes JSON sobre TCP. Incluye una interfaz gráfica opcional (PySide6) para el nodo Monitor.

---

## 1. Roles del sistema

- `server.py`: servidor central tipo hub. Solo acepta conexiones, registra nombres, difunde mensajes públicos y reenvía mensajes privados. No contiene lógica de negocio, blockchain, hashes ni consenso.
- `monitor.py`: cliente monitor. Carga transacciones, crea bloques candidatos, los distribuye por mensajes privados, cuenta votos públicos, calcula el quórum y consolida el ledger.
- `validator.py`: cliente procesador/validador. Recibe bloques por privado, valida hash y acertijo criptográfico, vota públicamente y actualiza su copia del ledger cuando el monitor anuncia consenso.
- `protocol.py`: utilidades de framing JSON por TCP.
- `blockchain.py`: estructuras y funciones criptográficas compartidas.
- `gui_monitor.py`: interfaz gráfica del Monitor (opcional). Es una segunda "cara" del monitor: se conecta como un cliente más y visualiza ledger, votos, quórum, latencia, forks y log en tiempo real.

## 2. Requisitos

- Python 3.10 o superior.
- El núcleo (servidor, validadores, monitor por terminal) **no requiere librerías externas**.
- La interfaz gráfica requiere `PySide6`:

```bash
pip install -r requirements.txt
```

---

## 3. Parámetros: qué es cada uno y sus rangos

Estos parámetros se pasan por línea de comandos (terminal) o se ajustan en la interfaz gráfica.

### Parámetros de conexión y consenso (servidor / monitor)

| Parámetro | Quién lo usa | Rango / valor | Significado |
|---|---|---|---|
| `--host` | todos | IP o nombre (def. `127.0.0.1`) | Dirección del servidor. En una sola máquina se deja en `127.0.0.1`. |
| `--port` | todos | 1–65535 (def. `5050`) | Puerto TCP del servidor. Debe ser el mismo en todos los nodos. |
| `--validators` | monitor | lista separada por comas (ej. `V1,V2,V3`) | Validadores que el monitor **espera**. El quórum se calcula sobre los que estén realmente conectados. |
| `--difficulty` | monitor y validadores | entero 1–8 (def. `3`) | Nº de ceros con que debe empezar el hash (dificultad del acertijo/PoW). **Debe ser igual en monitor y validadores**, o rechazarán bloques válidos. |
| `--block-size` | monitor | entero 1–50 (def. `3`) | Cuántas transacciones entran en cada bloque. Determina cuántos bloques se generan. |
| `--timeout` | monitor | segundos 1–120 (def. `10`) | Tiempo máximo que el monitor espera el quórum de un bloque. Si expira, el bloque se rechaza por *timeout*. |
| `--auto` | monitor | ruta de archivo | Procesa ese archivo automáticamente al arrancar (sin REPL). |

### Parámetros de simulación de fallos (validadores)

| Parámetro | Rango / valor | Significado |
|---|---|---|
| `--fault-rate` | decimal 0.0–1.0 (def. `0.0`) | **Probabilidad de votar mal.** En cada bloque, con esa probabilidad el validador invierte su voto (un bloque válido lo marca como `BLOQUE_INVALIDO`). `0` = siempre honesto, `1` = siempre miente, `0.5` = la mitad de las veces. |
| `--delay` | segundos ≥ 0.0 (def. `0.0`; en la GUI hasta 10) | **Retraso artificial antes de votar.** Simula un nodo lento. Solo afecta la latencia del consenso si el voto de ese nodo es necesario para alcanzar el quórum. |

> **Nota sobre el quórum:** se calcula dinámicamente como `floor(validadores_activos / 2) + 1`, donde *activos* son los validadores conectados en ese momento. Con 3 activos el quórum es 2; con 2 activos es 2; con 1 activo es 1.

---

## 4. Uso por terminal

Cada componente va en su propia terminal. **Orden obligatorio: servidor → validadores → monitor.**

Servidor:

```bash
python server.py --host 127.0.0.1 --port 5050
```

Un validador por terminal (ejemplos con y sin fallos):

```bash
python validator.py --name V1 --host 127.0.0.1 --port 5050
python validator.py --name V2 --host 127.0.0.1 --port 5050 --fault-rate 0.25
python validator.py --name V3 --host 127.0.0.1 --port 5050 --delay 1.5
```

Monitor (modo interactivo):

```bash
python monitor.py --host 127.0.0.1 --port 5050 --validators V1,V2,V3
```

Dentro del monitor:

```text
cargar data/caso1_basico.txt
estado
salir
```

Monitor en modo automático (procesa y termina):

```bash
python monitor.py --port 5050 --validators V1,V2,V3 --auto data/caso1_basico.txt
```

### Agregar o quitar validadores (terminal)

- **Agregar:** abre otra terminal con `python validator.py --name V4 --port 5050` y, si quieres que cuente para el quórum, inclúyelo en `--validators` del monitor (`V1,V2,V3,V4`).
- **Quitar:** simplemente no lo lances, o ciérralo con `Ctrl+C`. El monitor lo detecta como caído y recalcula el quórum.
- El nombre (`--name`) debe ser **único**; el servidor rechaza nombres repetidos.

### Prueba rápida automatizada

```bash
python run_demo.py
```

Levanta servidor, tres validadores y el monitor en modo automático, imprime el resultado del consenso y detiene todos los procesos.

---

## 5. Uso de la interfaz gráfica

```bash
python gui_monitor.py
```

La ventana tiene: arriba la **barra de conexión**; a la izquierda **Nodos** y **Lanzador integrado**; al centro el **Ledger**; a la derecha las pestañas **Ronda actual**, **Métricas** y **Log crudo**.

Orden de uso siempre: **servidor → validadores → Conectar → Cargar transacciones.**

### Opción A — Todo desde la ventana (lanzador integrado)

1. En **Lanzador integrado** pulsa **"Iniciar servidor"** (verás líneas `[SERVER]` en *Log crudo*).
2. Configura la tabla de validadores (ver abajo) y pulsa **"Lanzar validadores"**.
3. En la barra de arriba pulsa **"Conectar"**. Los nodos se ponen en verde (activos).
4. Pulsa **"Cargar"**, elige un archivo de `data/` y observa el consenso en vivo.
5. Para probar otro caso pulsa **"Reiniciar"**: desconecta, detiene los procesos, limpia todo y vuelve al estado inicial.

### Opción B — Conectar a nodos ya levantados

Levanta servidor y validadores en terminales (sección 4). En la GUI ajusta Host/Puerto/Validadores, pulsa **"Conectar"** y luego **"Cargar"**. El lanzador integrado se ignora.

### La tabla de validadores (agregar, quitar y configurar)

La tabla tiene una fila por validador, con columnas **Nodo**, **Fallo** y **Retardo**:

- **Agregar validador:** botón **"+ nodo"** (añade una fila nueva con nombre automático Vn).
- **Quitar validador:** selecciona la fila y pulsa **"− nodo"** (si no seleccionas nada, quita la última).
- **Renombrar:** doble clic en la celda **Nodo** y escribe el nombre.
- **Fallo (`fault-rate`):** rango 0.00–1.00. Probabilidad de voto incorrecto.
- **Retardo (`delay`):** en segundos. Simula un nodo lento.
- Los valores de Fallo y Retardo se cambian con la **rueda del ratón** o **escribiendo** dentro de la celda.

> **Importante:** "Lanzar validadores" arranca exactamente las filas que haya en la tabla en ese momento. Para hacer una corrida con **solo dos validadores** (escenario E1), quita la fila V3 con **"− nodo"** antes de lanzar. Si declaras `V1,V2,V3` en la barra de conexión pero solo lanzas V1 y V2, el panel de nodos mostrará V3 en rojo (caído) y el quórum quedará en 2.

### Qué muestra cada zona

- **Nodos:** punto verde = activo, rojo = caído; etiqueta con el quórum requerido y el nº de activos.
- **Ronda actual:** bloque propuesto, hash, barra de progreso (votos OK vs quórum), voto de cada validador (✓ OK / ✗ inválido con su error) y el resultado final (CONSENSO / RECHAZADO / FORK).
- **Ledger:** cada bloque como tarjeta encadenada (id, hash, previous_hash, nonce).
- **Métricas:** una fila por bloque con resultado, latencia, votos OK/INVÁLIDO y si hubo fork.
- **Log crudo:** todos los mensajes tal cual.

> Deja ~1 segundo entre iniciar el servidor y lanzar los validadores. Al terminar usa **"Detener todo"** o **"Reiniciar"** para cerrar los procesos limpiamente.

---

## 6. Casos de prueba (carpeta `data/`)

Cada archivo varía el volumen y la estructura de las transacciones (con `--block-size 3`):

| Archivo | Transacciones | Bloques | Qué demuestra |
|---|---|---|---|
| `caso1_basico.txt` | 9 | 3 | Consenso limpio de extremo a extremo. |
| `caso2_bloque_unico.txt` | 2 | 1 | Caso mínimo: el ledger crece de 0 a 1 desde el génesis. |
| `caso3_carga_alta.txt` | 21 | 7 | Rendimiento y latencia en varias rondas. |
| `caso4_parser_robusto.txt` | 6 | 2 | El parser ignora comentarios (`#`) y líneas en blanco. |

El archivo define **qué** se procesa; el comportamiento de cada nodo (fallo, retardo, dificultad) es independiente y se fija al lanzar cada validador.

---

## 7. Escenarios de simulación

Recetas verificadas. Monitor siempre con `--validators V1,V2,V3` y `--block-size 3` salvo que se indique.

**A · Consenso limpio** — V1, V2, V3 sin banderas, `caso1_basico.txt`.
→ Quórum 2, los 3 bloques con CONSENSO ALCANZADO, latencias de ~1 ms, ledger de altura 3.

**B · Acertijo / dificultad** — los tres validadores y el monitor con `--difficulty 4`, `caso1`.
→ El minado tarda más y cada hash empieza con 4 ceros. Demuestra que el proof-of-work es real. (Dificultad 5 ≈ varios segundos por bloque.)

**C · Resiliencia ante nodo defectuoso** — `V2 --fault-rate 0.5`, los demás normales, `caso1`.
→ V2 a veces vota INVÁLIDO, pero V1+V3 dan el quórum: el consenso **igual se alcanza**.

**D · Latencia y nodos lentos** — `caso1`:
- D1 (minoría lenta): solo `V3 --delay 2.0` → latencia **baja** (~1 ms); el lento no se espera porque V1+V2 ya hacen quórum.
- D2 (el quórum necesita al lento): `V2 --delay 1.0` y `V3 --delay 1.0` → latencia **~1 s** por bloque.
- Lección: un nodo lento solo frena el consenso si su voto es necesario para el quórum.

**E · Caída de nodo / quórum dinámico** — `caso1`, monitor con los tres configurados:
- E1: lanza solo V1 y V2 → quórum 2, consenso OK con dos nodos.
- E2: lanza solo V1 → quórum 1, consenso con un solo nodo.

**F · Rechazo por mayoría inválida** — `V2 --fault-rate 1.0` y `V3 --fault-rate 1.0`, `caso2_bloque_unico.txt`.
→ Dos votos INVÁLIDO alcanzan el quórum: el bloque se **rechaza** ("Mayoría de votos inválidos") y el ledger queda vacío.

**G · Timeout (falta de quórum)** — monitor con `--timeout 3`; lanza V1 y `V2 --delay 10` (no lances V3), `caso2`.
→ Solo llega un voto a tiempo: el bloque se rechaza por **timeout** de 3 s.

> **Sobre los forks:** el monitor propone los bloques en secuencia, encadenando cada uno al último hash aceptado, por lo que en operación normal **no se produce un fork**. La detección (`fork_detected`) existe como red de seguridad; la topología en estrella con un único proponente serializa las propuestas y evita bifurcaciones.

---

## 8. Protocolo de chat

JSON delimitado por salto de línea. Los clientes envían al servidor:

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

Los mensajes de negocio van encapsulados como JSON dentro de `text`, así el servidor sigue siendo un chat genérico. Tipos de negocio: `BLOCK_PROPOSAL` (Monitor → validador, privado), `VOTE` (validador → todos, público), `CONSENSUS_REACHED` (Monitor → todos, público) y `WHO`/`HELLO` (descubrimiento de presencia).

### Descubrimiento de presencia (WHO/HELLO)

Para que el quórum sea dinámico sin modificar el servidor: cada validador emite `HELLO` al conectarse; el Monitor emite `WHO` al arrancar y los validadores responden `HELLO`. Así el Monitor conoce los validadores activos sin importar el orden de arranque, y detecta caídas por el mensaje de sistema del servidor.

## 9. Criterio criptográfico

Cada bloque contiene `id`, `transactions`, `previous_hash`, `nonce`, `timestamp` y `hash`. El hash se calcula con SHA-256 sobre el contenido canónico. Un bloque es válido si:

1. El hash calculado coincide con el recibido.
2. El hash empieza con `difficulty` ceros.
3. El `previous_hash` coincide con el último hash aceptado por el nodo.

## 10. Arquitectura de hilos

- **Servidor:** un hilo por cliente.
- **Monitor (núcleo):** un hilo lee el socket mientras el principal coordina las rondas; estado compartido protegido con `Lock`.
- **GUI:** el hilo del socket nunca toca widgets; emite **señales de Qt** entregadas al hilo de la interfaz. La carga de transacciones corre en un `QThread` aparte y los procesos del lanzador usan `QProcess`.
