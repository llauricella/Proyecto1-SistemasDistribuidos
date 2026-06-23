
import sys
import threading
from functools import partial

from PySide6.QtCore import Qt, QObject, Signal, QThread, QProcess, QProcessEnvironment
from PySide6.QtGui import QFont, QAction
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QLineEdit, QPushButton, QSpinBox,
    QDoubleSpinBox, QHBoxLayout, QVBoxLayout, QGridLayout, QFrame, QScrollArea,
    QTabWidget, QPlainTextEdit, QTableWidget, QTableWidgetItem, QHeaderView,
    QSplitter, QFileDialog, QMessageBox, QGroupBox, QSizePolicy, QProgressBar,
)

from monitor import MonitorClient

MONO = "Consolas, 'DejaVu Sans Mono', Menlo, monospace"

# ----------------------------------------------------------------------------- 
# Hoja de estilo (look oscuro y limpio)
# -----------------------------------------------------------------------------
STYLE = """
QMainWindow, QWidget { background: #0f1419; color: #d7dde4; font-size: 13px; }
QGroupBox {
    border: 1px solid #243040; border-radius: 8px; margin-top: 14px; padding: 8px;
    font-weight: 600; color: #9fb3c8;
}
QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 4px; }
QLineEdit, QSpinBox, QDoubleSpinBox {
    background: #1a2230; border: 1px solid #2b3a4f; border-radius: 6px; padding: 5px 7px;
    selection-background-color: #3b82f6;
}
QLineEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus { border: 1px solid #3b82f6; }
QPushButton {
    background: #1f6feb; color: white; border: none; border-radius: 6px;
    padding: 7px 14px; font-weight: 600;
}
QPushButton:hover { background: #388bfd; }
QPushButton:disabled { background: #30363d; color: #6e7681; }
QPushButton#ghost { background: #21304a; color: #9fc3ff; }
QPushButton#ghost:hover { background: #2b3e5f; }
QPushButton#danger { background: #b1361b; }
QPushButton#danger:hover { background: #da3633; }
QTabWidget::pane { border: 1px solid #243040; border-radius: 8px; top: -1px; }
QTabBar::tab {
    background: #161c26; padding: 8px 16px; border-top-left-radius: 6px;
    border-top-right-radius: 6px; color: #8b98a8; margin-right: 2px;
}
QTabBar::tab:selected { background: #1f2937; color: #e6edf3; }
QPlainTextEdit {
    background: #0a0e13; border: 1px solid #243040; border-radius: 8px;
    font-family: %s; font-size: 12px; color: #c9d4df;
}
QTableWidget {
    background: #0a0e13; gridline-color: #1c2733; border: 1px solid #243040;
    border-radius: 8px; font-family: %s; font-size: 12px;
}
QHeaderView::section {
    background: #161c26; color: #9fb3c8; border: none; padding: 6px; font-weight: 600;
}
QScrollArea { border: none; }
QProgressBar {
    border: 1px solid #2b3a4f; border-radius: 6px; text-align: center;
    background: #11161d; height: 18px;
}
QProgressBar::chunk { background: #2ea043; border-radius: 5px; }
QLabel#pill {
    border-radius: 10px; padding: 2px 10px; font-weight: 700; font-size: 12px;
}
""" % (MONO, MONO)


# -----------------------------------------------------------------------------
# Puente de hilos: el observador del MonitorClient (que corre en hilos de socket)
# emite señales Qt, que se entregan de forma segura al hilo de la GUI.
# -----------------------------------------------------------------------------
class Bridge(QObject):
    event = Signal(str, dict)


class ConnectWorker(QThread):
    ok = Signal()
    fail = Signal(str)

    def __init__(self, monitor: MonitorClient):
        super().__init__()
        self.monitor = monitor

    def run(self):
        try:
            self.monitor.connect()
            self.ok.emit()
        except Exception as exc:  # noqa: BLE001
            self.fail.emit(str(exc))


class LoadWorker(QThread):
    done = Signal()

    def __init__(self, monitor: MonitorClient, path: str):
        super().__init__()
        self.monitor = monitor
        self.path = path

    def run(self):
        try:
            self.monitor.load_and_process(self.path)
        finally:
            self.done.emit()


# -----------------------------------------------------------------------------
# Tarjeta de bloque para el ledger
# -----------------------------------------------------------------------------
class BlockCard(QFrame):
    def __init__(self, height: int, block: dict):
        super().__init__()
        self.setStyleSheet(
            "QFrame { background: #131b26; border: 1px solid #2a3a4f; border-radius: 10px; }"
        )
        lay = QVBoxLayout(self)
        lay.setContentsMargins(12, 10, 12, 10)
        lay.setSpacing(3)

        top = QHBoxLayout()
        bid = QLabel(f"#{height}  {block['id']}")
        bid.setStyleSheet("color:#e6edf3; font-weight:700; font-size:14px; border:none;")
        ntx = QLabel(f"{len(block['transactions'])} tx")
        ntx.setStyleSheet(
            "color:#9fc3ff; background:#1d2c44; border-radius:9px; padding:1px 9px; border:none;"
        )
        top.addWidget(bid)
        top.addStretch()
        top.addWidget(ntx)
        lay.addLayout(top)

        h = QLabel(f"hash  {block['hash']}")
        h.setStyleSheet(f"color:#2ea043; font-family:{MONO}; font-size:11px; border:none;")
        h.setWordWrap(True)
        p = QLabel(f"prev  {block['previous_hash']}")
        p.setStyleSheet(f"color:#6e7e90; font-family:{MONO}; font-size:11px; border:none;")
        p.setWordWrap(True)
        meta = QLabel(f"nonce={block['nonce']}")
        meta.setStyleSheet("color:#8b98a8; font-size:11px; border:none;")
        lay.addWidget(h)
        lay.addWidget(p)
        lay.addWidget(meta)


# -----------------------------------------------------------------------------
# Ventana principal
# -----------------------------------------------------------------------------
class MonitorDashboard(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Monitor · Consenso Distribuido de Bloques")
        self.resize(900, 580)
        self.setStyleSheet(STYLE)

        self.monitor: MonitorClient | None = None
        self.bridge = Bridge()
        self.bridge.event.connect(self.on_event)
        self.node_rows: dict[str, QLabel] = {}
        self.vote_rows: dict[str, QLabel] = {}
        self.proc_server: QProcess | None = None
        self.proc_validators: dict[str, QProcess] = {}
        self.connect_worker = None
        self.load_worker = None

        self._build_ui()

    # ---------------------------- construcción UI ----------------------------
    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(8)

        root.addWidget(self._build_topbar())

        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(self._build_left_panel())
        splitter.addWidget(self._build_ledger_panel())
        splitter.addWidget(self._build_right_tabs())
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 2)
        splitter.setStretchFactor(2, 2)
        splitter.setSizes([280, 480, 520])
        root.addWidget(splitter, 1)

        self.status = QLabel("Desconectado")
        self.status.setObjectName("pill")
        self.status.setStyleSheet("background:#3d2326; color:#ff7b72;")
        self.statusBar().addPermanentWidget(self.status)
        self.statusBar().showMessage("Listo. Conéctate a un servidor o usa el lanzador integrado.")

    def _build_topbar(self) -> QWidget:
        box = QGroupBox("Conexión del Monitor")
        g = QGridLayout(box)
        g.setHorizontalSpacing(10)

        self.in_host = QLineEdit("127.0.0.1")
        self.in_port = QSpinBox(); self.in_port.setRange(1, 65535); self.in_port.setValue(5050)
        self.in_validators = QLineEdit("V1,V2,V3")
        self.in_difficulty = QSpinBox(); self.in_difficulty.setRange(1, 8); self.in_difficulty.setValue(3)
        self.in_blocksize = QSpinBox(); self.in_blocksize.setRange(1, 50); self.in_blocksize.setValue(3)
        self.in_timeout = QDoubleSpinBox(); self.in_timeout.setRange(1, 120); self.in_timeout.setValue(10.0)

        self.in_host.setFixedWidth(110)
        self.in_validators.setMinimumWidth(60)

        def lbl(t):
            x = QLabel(t); x.setStyleSheet("color:#8b98a8;"); return x

        g.addWidget(lbl("Host"), 0, 0); g.addWidget(self.in_host, 0, 1)
        g.addWidget(lbl("Puerto"), 0, 2); g.addWidget(self.in_port, 0, 3)
        g.addWidget(lbl("Validadores"), 0, 4); g.addWidget(self.in_validators, 0, 5)
        g.addWidget(lbl("Dificultad"), 0, 6); g.addWidget(self.in_difficulty, 0, 7)
        g.addWidget(lbl("Bloque"), 0, 8); g.addWidget(self.in_blocksize, 0, 9)
        g.addWidget(lbl("Timeout"), 0, 10); g.addWidget(self.in_timeout, 0, 11)

        self.btn_connect = QPushButton("Conectar")
        self.btn_connect.clicked.connect(self.on_connect)
        self.btn_load = QPushButton("Cargar txs..")
        self.btn_load.setObjectName("ghost")
        self.btn_load.setEnabled(False)
        self.btn_load.clicked.connect(self.on_load)
        g.addWidget(self.btn_connect, 0, 12)
        g.addWidget(self.btn_load, 0, 13)
        g.setColumnStretch(5, 1)
        return box

    def _build_left_panel(self) -> QWidget:
        wrap = QWidget()
        v = QVBoxLayout(wrap)
        v.setContentsMargins(0, 0, 0, 0)

        # --- estado de nodos ---
        nodes_box = QGroupBox("Nodos validadores")
        nv = QVBoxLayout(nodes_box)
        self.nodes_layout = QVBoxLayout()
        self.nodes_layout.setSpacing(4)
        nv.addLayout(self.nodes_layout)
        self.lbl_quorum = QLabel("Quórum requerido: —")
        self.lbl_quorum.setStyleSheet("color:#d29922; font-weight:700; padding-top:6px;")
        nv.addWidget(self.lbl_quorum)
        nv.addStretch()
        v.addWidget(nodes_box, 1)

        # --- lanzador integrado ---
        v.addWidget(self._build_launcher())
        return wrap

    def _build_launcher(self) -> QWidget:
        box = QGroupBox("Lanzador integrado")
        v = QVBoxLayout(box)

        self.btn_srv = QPushButton("Iniciar servidor")
        self.btn_srv.setObjectName("ghost")
        self.btn_srv.clicked.connect(self.toggle_server)
        v.addWidget(self.btn_srv)

        # tabla de validadores a lanzar (nombre, fault-rate, delay)
        self.launch_table = QTableWidget(0, 3)
        self.launch_table.setHorizontalHeaderLabels(["Nodo", "Fallo", "Retardo"])
        self.launch_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.launch_table.verticalHeader().setVisible(False)
        self.launch_table.setMaximumHeight(150)
        v.addWidget(self.launch_table)

        row = QHBoxLayout()
        b_add = QPushButton("+ nodo"); b_add.setObjectName("ghost"); b_add.clicked.connect(self.add_launch_row)
        b_start = QPushButton("Lanzar validadores"); b_start.clicked.connect(self.start_validators)
        b_stop = QPushButton("Detener todo"); b_stop.setObjectName("danger"); b_stop.clicked.connect(self.stop_all)
        row.addWidget(b_add); row.addWidget(b_start)
        v.addLayout(row)
        v.addWidget(b_stop)

        for n in ("V1", "V2", "V3"):
            self.add_launch_row(name=n)
        return box

    def _build_ledger_panel(self) -> QWidget:
        box = QGroupBox("Ledger global (cadena de bloques)")
        v = QVBoxLayout(box)
        self.ledger_count = QLabel("0 bloques")
        self.ledger_count.setStyleSheet("color:#8b98a8;")
        v.addWidget(self.ledger_count)

        self.ledger_scroll = QScrollArea()
        self.ledger_scroll.setWidgetResizable(True)
        inner = QWidget()
        self.ledger_layout = QVBoxLayout(inner)
        self.ledger_layout.setSpacing(6)
        self.ledger_layout.addStretch()
        self.ledger_scroll.setWidget(inner)
        v.addWidget(self.ledger_scroll, 1)
        return box

    def _build_right_tabs(self) -> QWidget:
        tabs = QTabWidget()

        # --- Ronda actual ---
        round_w = QWidget()
        rv = QVBoxLayout(round_w)
        self.lbl_round = QLabel("Sin ronda activa")
        self.lbl_round.setStyleSheet("color:#e6edf3; font-weight:700; font-size:15px;")
        self.lbl_round_hash = QLabel("")
        self.lbl_round_hash.setStyleSheet(f"color:#6e7e90; font-family:{MONO}; font-size:11px;")
        rv.addWidget(self.lbl_round)
        rv.addWidget(self.lbl_round_hash)

        self.vote_progress = QProgressBar()
        self.vote_progress.setFormat("OK: %v / %m")
        rv.addWidget(self.vote_progress)

        self.votes_box = QVBoxLayout()
        self.votes_box.setSpacing(4)
        rv.addLayout(self.votes_box)
        self.lbl_outcome = QLabel("")
        self.lbl_outcome.setObjectName("pill")
        rv.addWidget(self.lbl_outcome)
        rv.addStretch()
        tabs.addTab(round_w, "Ronda actual")

        # --- Métricas ---
        self.metrics = QTableWidget(0, 6)
        self.metrics.setHorizontalHeaderLabels(["Bloque", "Resultado", "Latencia (s)", "OK", "INVÁLIDO", "Fork"])
        self.metrics.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.metrics.verticalHeader().setVisible(False)
        tabs.addTab(self.metrics, "Métricas")

        # --- Log ---
        log_w = QWidget()
        lv = QVBoxLayout(log_w)
        self.log = QPlainTextEdit(); self.log.setReadOnly(True)
        btn_clear = QPushButton("Limpiar log"); btn_clear.setObjectName("ghost")
        btn_clear.clicked.connect(lambda: self.log.clear())
        lv.addWidget(self.log, 1)
        lv.addWidget(btn_clear, 0, Qt.AlignRight)
        tabs.addTab(log_w, "Log crudo")
        return tabs

    # ------------------------------- conexión --------------------------------
    def on_connect(self):
        if self.monitor is not None:
            return
        validators = [v.strip() for v in self.in_validators.text().split(",") if v.strip()]
        if not validators:
            QMessageBox.warning(self, "Validadores", "Indica al menos un validador.")
            return

        self.monitor = MonitorClient(
            host=self.in_host.text().strip(),
            port=self.in_port.value(),
            validators=validators,
            difficulty=self.in_difficulty.value(),
            block_size=self.in_blocksize.value(),
            timeout=self.in_timeout.value(),
            observer=lambda kind, data: self.bridge.event.emit(kind, data),
        )
        self._reset_nodes(validators)
        self.btn_connect.setEnabled(False)
        self.statusBar().showMessage("Conectando…")

        self.connect_worker = ConnectWorker(self.monitor)
        self.connect_worker.ok.connect(self._connected_ok)
        self.connect_worker.fail.connect(self._connected_fail)
        self.connect_worker.start()

    def _connected_ok(self):
        self.status.setText("Conectado")
        self.status.setStyleSheet("background:#1b3326; color:#3fb950;")
        self.btn_load.setEnabled(True)
        self.statusBar().showMessage("Conectado al servidor.")

    def _connected_fail(self, err: str):
        self.monitor = None
        self.btn_connect.setEnabled(True)
        QMessageBox.critical(self, "Error de conexión",
                             f"No se pudo conectar al servidor.\n\n{err}\n\n¿Está corriendo el servidor?")
        self.statusBar().showMessage("Fallo de conexión.")

    def on_load(self):
        path, _ = QFileDialog.getOpenFileName(self, "Archivo de transacciones", "", "Texto (*.txt);;Todos (*)")
        if not path or not self.monitor:
            return
        self.btn_load.setEnabled(False)
        self.load_worker = LoadWorker(self.monitor, path)
        self.load_worker.done.connect(lambda: self.btn_load.setEnabled(True))
        self.load_worker.start()

    # --------------------------- eventos del monitor -------------------------
    def on_event(self, kind: str, data: dict):
        if kind == "log":
            self.log.appendPlainText(data["text"].rstrip("\n"))
        elif kind == "connected":
            self.lbl_quorum.setText(f"Quórum requerido: {data['quorum']}")
        elif kind == "nodes":
            self._update_nodes(data["active"], data["connected"], data["quorum"])
        elif kind == "proposal":
            self._start_round(data["block"], data["validators"], data["quorum"])
        elif kind == "vote":
            self._update_vote(data)
        elif kind == "decision":
            self._finish_round(data)

    def _reset_nodes(self, validators):
        while self.nodes_layout.count():
            item = self.nodes_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        self.node_rows.clear()
        for name in validators:
            row = QFrame()
            h = QHBoxLayout(row); h.setContentsMargins(2, 2, 2, 2)
            dot = QLabel("●"); dot.setStyleSheet("color:#6e7681; font-size:15px;")
            nm = QLabel(name); nm.setStyleSheet("color:#c9d4df; font-weight:600;")
            st = QLabel("esperando"); st.setStyleSheet("color:#6e7681; font-size:11px;")
            h.addWidget(dot); h.addWidget(nm); h.addStretch(); h.addWidget(st)
            self.nodes_layout.addWidget(row)
            self.node_rows[name] = (dot, st)

    def _update_nodes(self, active, connected, quorum):
        for name, (dot, st) in self.node_rows.items():
            if name in active:
                dot.setStyleSheet("color:#3fb950; font-size:15px;")
                st.setText("activo"); st.setStyleSheet("color:#3fb950; font-size:11px;")
            else:
                dot.setStyleSheet("color:#f85149; font-size:15px;")
                st.setText("caído"); st.setStyleSheet("color:#f85149; font-size:11px;")
        self.lbl_quorum.setText(f"Quórum requerido: {quorum}   (activos: {len(active)})")

    def _start_round(self, block, validators, quorum):
        self.lbl_round.setText(f"Ronda: {block['id']}  ·  {len(block['transactions'])} transacciones")
        self.lbl_round_hash.setText(f"hash {block['hash']}")
        self.lbl_outcome.setText("")
        self.lbl_outcome.setStyleSheet("")
        self.vote_progress.setMaximum(quorum)
        self.vote_progress.setValue(0)
        while self.votes_box.count():
            item = self.votes_box.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        self.vote_rows = {}
        for name in validators:
            lbl = QLabel(f"{name}: —")
            lbl.setStyleSheet("color:#8b98a8; padding:3px 8px; background:#11161d; border-radius:6px;")
            self.votes_box.addWidget(lbl)
            self.vote_rows[name] = lbl

    def _update_vote(self, data):
        name = data["validator"]
        vote = data["vote"]
        lbl = self.vote_rows.get(name)
        if lbl is not None:
            if vote == "BLOQUE_OK":
                lbl.setText(f"{name}: ✓ BLOQUE_OK")
                lbl.setStyleSheet("color:#3fb950; padding:3px 8px; background:#11251a; border-radius:6px; font-weight:600;")
            else:
                errs = "; ".join(data.get("errors", [])) or "voto inválido"
                lbl.setText(f"{name}: ✗ INVÁLIDO — {errs}")
                lbl.setStyleSheet("color:#f85149; padding:3px 8px; background:#2a1517; border-radius:6px; font-weight:600;")
        self.vote_progress.setMaximum(data["quorum"])
        self.vote_progress.setValue(data["ok"])

    def _finish_round(self, data):
        ann = data["announcement"]
        if data["fork"]:
            txt, color = "FORK DETECTADO", "background:#3a2d12; color:#d29922;"
        elif data["accepted"]:
            txt, color = "CONSENSO ALCANZADO", "background:#1b3326; color:#3fb950;"
        else:
            reason = data.get("reason") or "rechazado"
            txt, color = f"RECHAZADO — {reason}", "background:#3d2326; color:#ff7b72;"
        self.lbl_outcome.setText(txt)
        self.lbl_outcome.setStyleSheet(color + " border-radius:10px; padding:4px 12px; font-weight:700;")

        # métricas
        votes = ann.get("votes", {})
        ok = sum(1 for x in votes.values() if x == "BLOQUE_OK")
        bad = sum(1 for x in votes.values() if x == "BLOQUE_INVALIDO")
        r = self.metrics.rowCount()
        self.metrics.insertRow(r)
        cells = [ann["block_id"], "Aceptado" if data["accepted"] else ("Fork" if data["fork"] else "Rechazado"),
                 f"{ann['latency_seconds']:.4f}", str(ok), str(bad), "Sí" if data["fork"] else "No"]
        for c, val in enumerate(cells):
            self.metrics.setItem(r, c, QTableWidgetItem(val))

        # ledger
        self._rebuild_ledger(data["ledger"])

    def _rebuild_ledger(self, blocks):
        while self.ledger_layout.count():
            item = self.ledger_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        for i, blk in enumerate(blocks, start=1):
            if i > 1:
                arrow = QLabel("↓")
                arrow.setAlignment(Qt.AlignHCenter)
                arrow.setStyleSheet("color:#2ea043; font-size:16px;")
                self.ledger_layout.addWidget(arrow)
            self.ledger_layout.addWidget(BlockCard(i, blk))
        self.ledger_layout.addStretch()
        self.ledger_count.setText(f"{len(blocks)} bloque(s)")

    # ------------------------------ lanzador ---------------------------------
    def add_launch_row(self, name: str | None = None):
        r = self.launch_table.rowCount()
        self.launch_table.insertRow(r)
        self.launch_table.setItem(r, 0, QTableWidgetItem(name or f"V{r+1}"))
        fr = QDoubleSpinBox(); fr.setRange(0, 1); fr.setSingleStep(0.05); fr.setDecimals(2)
        dl = QDoubleSpinBox(); dl.setRange(0, 10); dl.setSingleStep(0.1); dl.setDecimals(1)
        self.launch_table.setCellWidget(r, 1, fr)
        self.launch_table.setCellWidget(r, 2, dl)

    def toggle_server(self):
        if self.proc_server is None:
            p = QProcess(self)
            p.setProcessChannelMode(QProcess.MergedChannels)
            p.readyReadStandardOutput.connect(partial(self._pipe, p, "SERVER"))
            p.start(sys.executable, ["server.py", "--host", self.in_host.text().strip(),
                                     "--port", str(self.in_port.value())])
            self.proc_server = p
            self.btn_srv.setText("Detener servidor")
            self.btn_srv.setObjectName("danger"); self.btn_srv.setStyleSheet(STYLE)
        else:
            self.proc_server.kill()
            self.proc_server = None
            self.btn_srv.setText("Iniciar servidor")
            self.btn_srv.setObjectName("ghost"); self.btn_srv.setStyleSheet(STYLE)

    def start_validators(self):
        for r in range(self.launch_table.rowCount()):
            name = self.launch_table.item(r, 0).text().strip()
            if not name or name in self.proc_validators:
                continue
            fr = self.launch_table.cellWidget(r, 1).value()
            dl = self.launch_table.cellWidget(r, 2).value()
            p = QProcess(self)
            p.setProcessChannelMode(QProcess.MergedChannels)
            p.readyReadStandardOutput.connect(partial(self._pipe, p, name))
            p.start(sys.executable, [
                "validator.py", "--name", name,
                "--host", self.in_host.text().strip(), "--port", str(self.in_port.value()),
                "--difficulty", str(self.in_difficulty.value()),
                "--fault-rate", str(fr), "--delay", str(dl),
            ])
            self.proc_validators[name] = p
        self.statusBar().showMessage(f"Validadores activos: {', '.join(self.proc_validators) or '—'}")

    def _pipe(self, proc: QProcess, tag: str):
        data = bytes(proc.readAllStandardOutput()).decode("utf-8", "replace")
        for line in data.splitlines():
            self.log.appendPlainText(f"[{tag}] {line}")

    def stop_all(self):
        for p in list(self.proc_validators.values()):
            p.kill()
        self.proc_validators.clear()
        if self.proc_server is not None:
            self.proc_server.kill()
            self.proc_server = None
            self.btn_srv.setText("Iniciar servidor")
            self.btn_srv.setObjectName("ghost"); self.btn_srv.setStyleSheet(STYLE)
        self.statusBar().showMessage("Procesos detenidos.")

    def closeEvent(self, ev):
        if self.monitor is not None:
            self.monitor.running.clear()
            try:
                self.monitor.sock.close()
            except Exception:
                pass
        self.stop_all()
        ev.accept()


def main():
    app = QApplication(sys.argv)
    app.setFont(QFont("Segoe UI", 10))
    win = MonitorDashboard()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
