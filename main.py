
import os
import sys
import json
import re
from pathlib import Path

from PyQt6.QtCore import Qt, QProcess, QTimer
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QTabWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QPlainTextEdit, QFileDialog, QMessageBox, QLineEdit,
    QTableView, QHeaderView, QFormLayout, QSpinBox, QGroupBox, QSplitter,
    QComboBox, QProgressBar
)
from PyQt6.QtGui import QAction, QStandardItemModel, QStandardItem

APP_NAME = "BaseLinker Tools (Add / Update / ERP / Sync)"
DEFAULT_ENV_FILE = ".env"

SCRIPT_ADD = "add_products.py"
SCRIPT_UPDATE = "update_products.py"
SCRIPT_ERP = "update_erp.py"
SCRIPT_SYNC = "sync_sku_to_id.py"

SKU_JSON = "sku_to_id.json"

KNOWN_LOG_FILES = [
    "add_products.log",
    "update_products.log",
    "update_erp.log",
    "sync_sku_to_id.log",
]

# ---- tiny .env parser/writer (keeps unknown lines as-is) ----
def load_env_text(path: Path) -> str:
    if path.exists():
        return path.read_text(encoding="utf-8", errors="replace")
    return ""

def parse_env(text: str) -> dict:
    env = {}
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "=" not in s:
            continue
        k, v = s.split("=", 1)
        env[k.strip()] = v.strip()
    return env

def upsert_env_key(text: str, key: str, value: str) -> str:
    lines = text.splitlines()
    pat = re.compile(rf"^\s*{re.escape(key)}\s*=")
    replaced = False
    out = []
    for line in lines:
        if pat.match(line):
            out.append(f"{key}={value}")
            replaced = True
        else:
            out.append(line)
    if not replaced:
        if out and out[-1].strip() != "":
            out.append("")
        out.append(f"{key}={value}")
    return "\n".join(out) + ("\n" if text.endswith("\n") else "")

class SkuModel(QStandardItemModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setColumnCount(2)
        self.setHorizontalHeaderLabels(["SKU", "product_id"])

    def load_from_dict(self, d: dict):
        self.setRowCount(0)
        for sku, pid in d.items():
            self.appendRow([QStandardItem(str(sku)), QStandardItem(str(pid))])

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.resize(1200, 780)

        self.project_dir = Path.cwd()
        self.env_path = self.project_dir / DEFAULT_ENV_FILE

        self.process: QProcess | None = None
        self.current_script: str | None = None

        # progress parsing
        self._det_total = None
        self._run_started_ms = None
        self._last_output_ms = None

        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        self._build_menu()
        self._build_run_tab()
        self._build_sku_tab()
        self._build_logs_tab()
        self._build_settings_tab()

        self.statusBar().showMessage("Ready")

        self.log_refresh_timer = QTimer(self)
        self.log_refresh_timer.setInterval(1500)
        self.log_refresh_timer.timeout.connect(self._refresh_log_file_combo)
        self.log_refresh_timer.start()

        self._ui_timer = QTimer(self)
        self._ui_timer.setInterval(500)
        self._ui_timer.timeout.connect(self._tick_running_ui)
        self._ui_timer.start()

        self._refresh_everything()

    # ---------- menu ----------
    def _build_menu(self):
        open_proj = QAction("Open project folder…", self)
        open_proj.triggered.connect(self.choose_project_folder)

        reload_all = QAction("Reload", self)
        reload_all.triggered.connect(self._refresh_everything)

        quit_act = QAction("Quit", self)
        quit_act.triggered.connect(self.close)

        menu = self.menuBar().addMenu("File")
        menu.addAction(open_proj)
        menu.addAction(reload_all)
        menu.addSeparator()
        menu.addAction(quit_act)

    # ---------- Run tab ----------
    def _build_run_tab(self):
        w = QWidget()
        root = QVBoxLayout(w)

        info = QHBoxLayout()
        self.lbl_project = QLabel("")
        self.lbl_project.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        info.addWidget(QLabel("Project:"))
        info.addWidget(self.lbl_project, 1)
        btn_proj = QPushButton("Change…")
        btn_proj.clicked.connect(self.choose_project_folder)
        info.addWidget(btn_proj)
        root.addLayout(info)

        grp = QGroupBox("Run tools")
        g = QVBoxLayout(grp)

        row = QHBoxLayout()
        self.btn_add = QPushButton("ADD products")
        self.btn_add.clicked.connect(lambda: self.run_script(SCRIPT_ADD))
        row.addWidget(self.btn_add)

        self.btn_update = QPushButton("UPDATE products")
        self.btn_update.clicked.connect(lambda: self.run_script(SCRIPT_UPDATE))
        row.addWidget(self.btn_update)

        self.btn_erp = QPushButton("UPDATE ERP_ID (extra_field_9157)")
        self.btn_erp.clicked.connect(lambda: self.run_script(SCRIPT_ERP))
        row.addWidget(self.btn_erp)

        self.btn_sync = QPushButton("SYNC sku_to_id.json")
        self.btn_sync.clicked.connect(lambda: self.run_script(SCRIPT_SYNC))
        row.addWidget(self.btn_sync)

        g.addLayout(row)

        row2 = QHBoxLayout()

        self.progress = QProgressBar()
        self.progress.setTextVisible(True)
        self.progress.setFormat("Idle")
        self.progress.setRange(0, 1)
        self.progress.setValue(0)
        row2.addWidget(self.progress, 2)

        self.btn_stop = QPushButton("STOP")
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self.stop_script)
        row2.addWidget(self.btn_stop)

        self.lbl_running = QLabel("Not running")
        row2.addWidget(self.lbl_running, 1)
        g.addLayout(row2)

        root.addWidget(grp)

        root.addWidget(QLabel("Live output"))
        self.console = QPlainTextEdit()
        self.console.setReadOnly(True)
        self.console.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        root.addWidget(self.console, 1)

        self.tabs.addTab(w, "Run")

    # ---------- SKU tab ----------
    def _build_sku_tab(self):
        from PyQt6.QtCore import QSortFilterProxyModel

        w = QWidget()
        root = QVBoxLayout(w)

        top = QHBoxLayout()
        self.lbl_sku_file = QLabel("")
        self.lbl_sku_file.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        top.addWidget(QLabel("sku_to_id.json:"))
        top.addWidget(self.lbl_sku_file, 1)

        self.btn_reload_sku = QPushButton("Reload")
        self.btn_reload_sku.clicked.connect(self.load_sku_json)
        top.addWidget(self.btn_reload_sku)

        self.btn_open_sku = QPushButton("Open file…")
        self.btn_open_sku.clicked.connect(self.open_sku_file)
        top.addWidget(self.btn_open_sku)

        root.addLayout(top)

        filt = QHBoxLayout()
        filt.addWidget(QLabel("Search SKU:"))
        self.search_sku = QLineEdit()
        self.search_sku.setPlaceholderText("type e.g. SACHS 318…")
        filt.addWidget(self.search_sku, 1)

        self.lbl_count = QLabel("0 records")
        filt.addWidget(self.lbl_count)
        root.addLayout(filt)

        self.sku_model = SkuModel(self)
        self.sku_proxy = QSortFilterProxyModel(self)
        self.sku_proxy.setSourceModel(self.sku_model)
        self.sku_proxy.setFilterCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.sku_proxy.setFilterKeyColumn(0)
        self.search_sku.textChanged.connect(self.sku_proxy.setFilterFixedString)

        self.table = QTableView()
        self.table.setModel(self.sku_proxy)
        self.table.setSortingEnabled(True)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.table.setAlternatingRowColors(True)
        root.addWidget(self.table, 1)

        self.tabs.addTab(w, "SKU map")

    # ---------- Logs tab ----------
    def _build_logs_tab(self):
        w = QWidget()
        root = QVBoxLayout(w)

        top = QHBoxLayout()
        top.addWidget(QLabel("Log file:"))

        self.log_combo = QComboBox()
        self.log_combo.currentTextChanged.connect(lambda _: self.load_selected_log())
        top.addWidget(self.log_combo, 1)

        self.btn_open_log = QPushButton("Open…")
        self.btn_open_log.clicked.connect(self.open_log_file)
        top.addWidget(self.btn_open_log)

        self.btn_reload_log = QPushButton("Reload")
        self.btn_reload_log.clicked.connect(self.load_selected_log)
        top.addWidget(self.btn_reload_log)

        root.addLayout(top)

        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        root.addWidget(self.log_view, 1)

        self.tabs.addTab(w, "Logs")

    # ---------- Settings tab ----------
    def _build_settings_tab(self):
        w = QWidget()
        root = QVBoxLayout(w)

        top = QHBoxLayout()
        self.lbl_env = QLabel("")
        self.lbl_env.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        top.addWidget(QLabel(".env:"))
        top.addWidget(self.lbl_env, 1)
        btn_env = QPushButton("Choose…")
        btn_env.clicked.connect(self.choose_env_file)
        top.addWidget(btn_env)
        root.addLayout(top)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        form_box = QGroupBox("Quick settings")
        form = QFormLayout(form_box)

        self.in_api_token = QLineEdit()
        self.in_xml_url = QLineEdit()
        self.in_api_url = QLineEdit()
        self.in_inventory_id = QLineEdit()
        self.in_new_inventory_id = QLineEdit()
        self.in_price_group_id = QLineEdit()

        self.in_rpm = QSpinBox(); self.in_rpm.setRange(1, 5000)
        self.in_workers = QSpinBox(); self.in_workers.setRange(1, 64)

        form.addRow("API_TOKEN", self.in_api_token)
        form.addRow("XML_URL", self.in_xml_url)
        form.addRow("API_URL", self.in_api_url)
        form.addRow("INVENTORY_ID", self.in_inventory_id)
        form.addRow("NEW_INVENTORY_ID", self.in_new_inventory_id)
        form.addRow("PRICE_GROUP_ID", self.in_price_group_id)
        form.addRow("REQUESTS_PER_MINUTE", self.in_rpm)
        form.addRow("MAX_WORKERS", self.in_workers)

        btns = QHBoxLayout()
        self.btn_apply_form = QPushButton("Apply to editor")
        self.btn_apply_form.clicked.connect(self.apply_form_to_editor)
        btns.addWidget(self.btn_apply_form)

        self.btn_save_env = QPushButton("Save .env")
        self.btn_save_env.clicked.connect(self.save_env)
        btns.addWidget(self.btn_save_env)

        form.addRow(btns)
        splitter.addWidget(form_box)

        right = QWidget()
        rlay = QVBoxLayout(right)
        rlay.addWidget(QLabel("Full .env editor"))
        self.env_editor = QPlainTextEdit()
        self.env_editor.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        rlay.addWidget(self.env_editor, 1)
        splitter.addWidget(right)

        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        root.addWidget(splitter, 1)

        self.tabs.addTab(w, "Settings")

    # ---------- progress helpers ----------
    def _now_ms(self) -> int:
        from PyQt6.QtCore import QDateTime
        return int(QDateTime.currentMSecsSinceEpoch())

    def _tick_running_ui(self):
        if self.process and self.process.state() != QProcess.ProcessState.NotRunning:
            if self._run_started_ms is None:
                return
            elapsed_s = max(0, (self._now_ms() - self._run_started_ms) / 1000.0)
            mm = int(elapsed_s // 60)
            ss = int(elapsed_s % 60)

            if self._det_total is None:
                self.progress.setRange(0, 0)  # indeterminate
                self.progress.setFormat(f"Running… {mm:02d}:{ss:02d}")
            else:
                self.progress.setFormat(f"{self.progress.value():,}/{self._det_total:,}  |  {mm:02d}:{ss:02d}")
        else:
            self.progress.setRange(0, 1)
            self.progress.setValue(0)
            self.progress.setFormat("Idle")
            self._det_total = None
            self._run_started_ms = None
            self._last_output_ms = None

    def _update_progress_from_text(self, chunk: str):
        patterns = [
            r"\[(\d{1,10})\s*/\s*(\d{1,10})\]",
            r"\b(\d{1,10})\s*/\s*(\d{1,10})\b",
            r"do\s+wysyłki\s+(\d{1,10})\s*/\s*(\d{1,10})",
        ]
        for pat in patterns:
            m = re.search(pat, chunk)
            if m:
                cur = int(m.group(1))
                total = int(m.group(2))
                if total <= 0:
                    return
                self._det_total = total
                self.progress.setRange(0, total)
                cur = max(0, min(cur, total))
                self.progress.setValue(cur)
                return

    # ---------- refresh ----------
    def _refresh_everything(self):
        self.lbl_project.setText(str(self.project_dir))
        self.lbl_env.setText(str(self.env_path))
        self.lbl_sku_file.setText(str(self.project_dir / SKU_JSON))
        self._refresh_log_file_combo()
        self.load_env_into_editor()
        self.load_sku_json()
        self.load_selected_log()

    def _refresh_log_file_combo(self):
        current = self.log_combo.currentText()
        self.log_combo.blockSignals(True)
        self.log_combo.clear()

        existing = []
        for lf in KNOWN_LOG_FILES:
            if (self.project_dir / lf).exists():
                existing.append(lf)
        for p in sorted(self.project_dir.glob("*.log")):
            if p.name not in existing:
                existing.append(p.name)
        for lf in KNOWN_LOG_FILES:
            if lf not in existing:
                existing.append(lf)

        self.log_combo.addItems(existing)
        idx = self.log_combo.findText(current)
        if idx >= 0:
            self.log_combo.setCurrentIndex(idx)
        self.log_combo.blockSignals(False)

    # ---------- folder/env selection ----------
    def choose_project_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Choose project folder", str(self.project_dir))
        if not folder:
            return
        self.project_dir = Path(folder)
        self.env_path = self.project_dir / DEFAULT_ENV_FILE
        self._refresh_everything()

    def choose_env_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Choose .env", str(self.project_dir), "Env files (*.env *.*)")
        if not path:
            return
        self.env_path = Path(path)
        self.load_env_into_editor()

    # ---------- env editor ----------
    def load_env_into_editor(self):
        text = load_env_text(self.env_path)
        self.env_editor.setPlainText(text)
        self._fill_quick_form(text)

    def _fill_quick_form(self, text: str):
        env = parse_env(text)
        self.in_api_token.setText(env.get("API_TOKEN", ""))
        self.in_xml_url.setText(env.get("XML_URL", ""))
        self.in_api_url.setText(env.get("API_URL", "https://api.baselinker.com/connector.php"))
        self.in_inventory_id.setText(env.get("INVENTORY_ID", ""))
        self.in_new_inventory_id.setText(env.get("NEW_INVENTORY_ID", ""))
        self.in_price_group_id.setText(env.get("PRICE_GROUP_ID", ""))

        def to_int(s, default):
            try:
                return int(str(s).strip())
            except Exception:
                return default

        self.in_rpm.setValue(to_int(env.get("REQUESTS_PER_MINUTE", 500), 500))
        self.in_workers.setValue(to_int(env.get("MAX_WORKERS", 10), 10))

    def apply_form_to_editor(self):
        text = self.env_editor.toPlainText()
        text = upsert_env_key(text, "API_TOKEN", self.in_api_token.text().strip())
        text = upsert_env_key(text, "XML_URL", self.in_xml_url.text().strip())
        text = upsert_env_key(text, "API_URL", self.in_api_url.text().strip())
        text = upsert_env_key(text, "INVENTORY_ID", self.in_inventory_id.text().strip())
        text = upsert_env_key(text, "NEW_INVENTORY_ID", self.in_new_inventory_id.text().strip())
        text = upsert_env_key(text, "PRICE_GROUP_ID", self.in_price_group_id.text().strip())
        text = upsert_env_key(text, "REQUESTS_PER_MINUTE", str(self.in_rpm.value()))
        text = upsert_env_key(text, "MAX_WORKERS", str(self.in_workers.value()))
        self.env_editor.setPlainText(text)

    def save_env(self):
        try:
            self.apply_form_to_editor()
            text = self.env_editor.toPlainText()
            self.env_path.parent.mkdir(parents=True, exist_ok=True)
            self.env_path.write_text(text, encoding="utf-8")
            QMessageBox.information(self, "Saved", f"Saved: {self.env_path}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save .env:\n{e}")

    # ---------- SKU ----------
    def open_sku_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Open sku_to_id.json", str(self.project_dir), "JSON (*.json);;All (*.*)")
        if not path:
            return
        self._load_sku(Path(path))

    def load_sku_json(self):
        self._load_sku(self.project_dir / SKU_JSON)

    def _load_sku(self, path: Path):
        self.lbl_sku_file.setText(str(path))
        try:
            if not path.exists():
                self.lbl_count.setText("File not found")
                self.sku_model.load_from_dict({})
                return
            data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
            if not isinstance(data, dict):
                raise ValueError("sku_to_id.json must be a JSON object {SKU: product_id}")
            self.sku_model.load_from_dict(data)
            self.lbl_count.setText(f"{len(data):,} records")
        except Exception as e:
            self.sku_model.load_from_dict({})
            self.lbl_count.setText("0 records")
            QMessageBox.critical(self, "Error", f"Failed to load JSON:\n{e}")

    # ---------- Logs ----------
    def open_log_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Open log", str(self.project_dir), "Log files (*.log *.txt);;All (*.*)")
        if not path:
            return
        self._load_log(Path(path))

    def load_selected_log(self):
        name = self.log_combo.currentText().strip()
        if not name:
            return
        self._load_log(self.project_dir / name)

    def _load_log(self, path: Path):
        try:
            if not path.exists():
                self.log_view.setPlainText(f"(No file) {path}")
                return
            txt = path.read_text(encoding="utf-8", errors="replace")
            self.log_view.setPlainText(txt)
            sb = self.log_view.verticalScrollBar()
            sb.setValue(sb.maximum())
        except Exception as e:
            self.log_view.setPlainText(f"Failed to read log:\n{e}")

    # ---------- Runner ----------
    def run_script(self, script_name: str):
        if self.process and self.process.state() != QProcess.ProcessState.NotRunning:
            QMessageBox.warning(self, "Running", "A script is already running. Stop it first.")
            return

        script_path = self.project_dir / script_name
        if not script_path.exists():
            QMessageBox.critical(self, "Missing script", f"Cannot find: {script_path}")
            return

        if not self.env_path.exists():
            reply = QMessageBox.question(
                self, "No .env",
                f".env not found at:\n{self.env_path}\n\nCreate it from editor now?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.Yes:
                self.save_env()
            else:
                return

        self.console.clear()
        self._append_console(f"==> Running {script_name}\n")
        self.current_script = script_name

        # init progress
        self._det_total = None
        self._run_started_ms = self._now_ms()
        self._last_output_ms = self._run_started_ms
        self.progress.setRange(0, 0)
        self.progress.setFormat("Starting…")

        self.process = QProcess(self)
        self.process.setWorkingDirectory(str(self.project_dir))

        python_exe = sys.executable
        self.process.setProgram(python_exe)
        self.process.setArguments([str(script_path)])

        self.process.readyReadStandardOutput.connect(self._on_stdout)
        self.process.readyReadStandardError.connect(self._on_stderr)
        self.process.finished.connect(self._on_finished)
        self.process.errorOccurred.connect(self._on_error)

        self.btn_stop.setEnabled(True)
        self.lbl_running.setText(f"Running: {script_name}")
        self.statusBar().showMessage(f"Running {script_name}…")

        self.process.start()
        if not self.process.waitForStarted(3000):
            QMessageBox.critical(self, "Error", "Failed to start process.")
            self.btn_stop.setEnabled(False)
            self.lbl_running.setText("Not running")

    def stop_script(self):
        if not self.process:
            return
        if self.process.state() == QProcess.ProcessState.NotRunning:
            return
        self._append_console("\n==> STOP requested…\n")
        self.process.terminate()
        if not self.process.waitForFinished(2000):
            self.process.kill()

    def _append_console(self, text: str):
        self.console.moveCursor(self.console.textCursor().MoveOperation.End)
        self.console.insertPlainText(text)
        sb = self.console.verticalScrollBar()
        sb.setValue(sb.maximum())

    def _on_stdout(self):
        data = bytes(self.process.readAllStandardOutput()).decode("utf-8", errors="replace")
        self._append_console(data)
        self._last_output_ms = self._now_ms()
        self._update_progress_from_text(data)

    def _on_stderr(self):
        data = bytes(self.process.readAllStandardError()).decode("utf-8", errors="replace")
        self._append_console(data)
        self._last_output_ms = self._now_ms()
        self._update_progress_from_text(data)

    def _on_finished(self, exit_code: int, _):
        status = "OK" if exit_code == 0 else f"Exit {exit_code}"
        self._append_console(f"\n==> Finished: {status}\n")
        if self._det_total is not None:
            self.progress.setRange(0, self._det_total)
            self.progress.setValue(self._det_total)
            self.progress.setFormat("Done")
        self.btn_stop.setEnabled(False)
        self.lbl_running.setText("Not running")
        self.statusBar().showMessage("Ready")
        self.process = None
        self.current_script = None
        self.load_sku_json()
        self.load_selected_log()

    def _on_error(self, err):
        self._append_console(f"\n==> Process error: {err}\n")
        self.btn_stop.setEnabled(False)
        self.lbl_running.setText("Not running")
        self.statusBar().showMessage("Ready")

    def closeEvent(self, event):
        if self.process and self.process.state() != QProcess.ProcessState.NotRunning:
            reply = QMessageBox.question(
                self, "Quit",
                "A script is still running. Quit anyway?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply != QMessageBox.StandardButton.Yes:
                event.ignore()
                return
            self.stop_script()
        event.accept()

def main():
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
