import sys
import csv
import json
import os
import datetime
import math
from pathlib import Path

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QLineEdit, QTableWidget, QTableWidgetItem,
    QTabWidget, QFileDialog, QMessageBox, QComboBox, QDateEdit,
    QHeaderView, QFrame, QScrollArea, QSplitter, QDoubleSpinBox,
    QDialog, QFormLayout, QDialogButtonBox, QProgressBar, QSizePolicy
)
from PyQt5.QtCore import Qt, QDate, QThread, pyqtSignal, QTimer
from PyQt5.QtGui import QFont, QColor, QPalette, QIcon

import matplotlib
matplotlib.use('Qt5Agg')
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ── Data store ────────────────────────────────────────────────────────────────
MF_DIR = Path.home() / ".mf_tracker"
MF_DIR.mkdir(parents=True, exist_ok=True)
META_FILE = MF_DIR / "meta.json"

AUTO_REFRESH_HOURS = 8


def _profile_file(profile_name: str) -> Path:
    safe = "".join(c if c.isalnum() or c in "-_ " else "_" for c in profile_name)
    return MF_DIR / f"portfolio_{safe}.json"


def list_profiles() -> list:
    meta = load_meta()
    profiles = meta.get("profiles", ["Default"])
    if not profiles:
        profiles = ["Default"]
    return profiles


def load_portfolio(profile: str) -> list:
    f = _profile_file(profile)
    # Migrate legacy single-portfolio file on first run
    legacy = MF_DIR / "portfolio.json"
    if legacy.exists() and not f.exists() and profile == "Default":
        import shutil
        shutil.copy(legacy, f)
    if f.exists():
        with open(f) as fh:
            return json.load(fh)
    return []


def save_portfolio(data: list, profile: str):
    with open(_profile_file(profile), "w") as f:
        json.dump(data, f, indent=2)


def add_profile(name: str):
    meta = load_meta()
    profiles = meta.get("profiles", ["Default"])
    if name not in profiles:
        profiles.append(name)
    meta["profiles"] = profiles
    save_meta(meta)


def remove_profile(name: str):
    meta = load_meta()
    profiles = meta.get("profiles", ["Default"])
    if name in profiles and name != "Default":
        profiles.remove(name)
    meta["profiles"] = profiles
    # Remove last_refreshed for this profile
    meta.pop(f"last_refreshed_{name}", None)
    save_meta(meta)
    # Delete data file
    f = _profile_file(name)
    if f.exists():
        f.unlink()


def load_meta() -> dict:
    if META_FILE.exists():
        with open(META_FILE) as f:
            return json.load(f)
    return {}


def save_meta(meta: dict):
    with open(META_FILE, "w") as f:
        json.dump(meta, f, indent=2)


def hours_since_last_refresh(profile: str) -> float:
    meta = load_meta()
    last = meta.get(f"last_refreshed_{profile}")
    if not last:
        return float("inf")
    delta = datetime.datetime.now() - datetime.datetime.fromisoformat(last)
    return delta.total_seconds() / 3600


def touch_last_refreshed(profile: str):
    meta = load_meta()
    meta[f"last_refreshed_{profile}"] = datetime.datetime.now().isoformat()
    save_meta(meta)


# ── NAV fetching ──────────────────────────────────────────────────────────────
def fetch_nav_history(scheme_code: str, from_date: str):
    """Fetch NAV history from MFAPI (free, no key needed)."""
    try:
        import urllib.request
        url = f"https://api.mfapi.in/mf/{scheme_code}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = json.loads(resp.read())
        data = raw.get("data", [])
        # data is list of {"date":"DD-MM-YYYY","nav":"..."}
        from_dt = datetime.datetime.strptime(from_date, "%Y-%m-%d").date()
        history = []
        for entry in data:
            try:
                dt = datetime.datetime.strptime(entry["date"], "%d-%m-%Y").date()
                if dt >= from_dt:
                    history.append({"date": dt.isoformat(), "nav": float(entry["nav"])})
            except Exception:
                continue
        history.sort(key=lambda x: x["date"])
        return history
    except Exception as e:
        return []


def search_funds(query: str):
    """Search mutual fund schemes by name."""
    try:
        import urllib.request
        url = "https://api.mfapi.in/mf/search?q=" + urllib.parse.quote(query)
        import urllib.parse
        url = "https://api.mfapi.in/mf/search?q=" + urllib.parse.quote(query)
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            results = json.loads(resp.read())
        return results[:20]
    except Exception:
        return []


def xirr(cashflows):
    """Compute XIRR given list of (date_str, amount) – negative=investment, positive=current."""
    if len(cashflows) < 2:
        return None
    dates = [datetime.datetime.strptime(c[0], "%Y-%m-%d") for c in cashflows]
    amounts = [c[1] for c in cashflows]
    def npv(rate):
        t0 = dates[0]
        return sum(a / ((1 + rate) ** ((d - t0).days / 365.0)) for d, a in zip(dates, amounts))
    lo, hi = -0.999, 100.0
    try:
        for _ in range(200):
            mid = (lo + hi) / 2
            if npv(mid) > 0:
                lo = mid
            else:
                hi = mid
            if abs(hi - lo) < 1e-7:
                break
        return round((lo + hi) / 2 * 100, 2)
    except Exception:
        return None


# ── Background worker ─────────────────────────────────────────────────────────
class NavFetcher(QThread):
    done = pyqtSignal(str, list)   # scheme_code, history

    def __init__(self, scheme_code, from_date):
        super().__init__()
        self.scheme_code = scheme_code
        self.from_date = from_date

    def run(self):
        history = fetch_nav_history(self.scheme_code, self.from_date)
        self.done.emit(self.scheme_code, history)


# ── Add Fund Dialog ───────────────────────────────────────────────────────────
class AddFundDialog(QDialog):
    def __init__(self, parent=None, fund=None):
        super().__init__(parent)
        self.edit_mode = fund is not None
        self.existing_fund = fund or {}
        self.setWindowTitle("Edit Mutual Fund" if self.edit_mode else "Add Mutual Fund")
        self.setMinimumWidth(460)
        self.setStyleSheet(parent.styleSheet() if parent else "")
        self.fund_list = []
        self._build_ui()
        if self.edit_mode:
            self._prefill(fund)

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(14)

        # Search
        lbl_search = QLabel("🔍  Search Fund Name")
        lbl_search.setFont(QFont("Segoe UI", 10, QFont.Bold))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("e.g. SBI Bluechip, HDFC Mid Cap...")
        self.search_input.setMinimumHeight(36)
        btn_search = QPushButton("Search")
        btn_search.setMinimumHeight(36)
        btn_search.clicked.connect(self._do_search)
        row = QHBoxLayout()
        row.addWidget(self.search_input)
        row.addWidget(btn_search)

        self.fund_combo = QComboBox()
        self.fund_combo.setMinimumHeight(34)
        self.fund_combo.setPlaceholderText("— select from results —")

        # Or manual scheme code
        sep = QLabel("─── or enter scheme code directly ───")
        sep.setAlignment(Qt.AlignCenter)
        sep.setStyleSheet("color: #888; font-size: 11px;")
        self.scheme_input = QLineEdit()
        self.scheme_input.setPlaceholderText("AMFI Scheme Code (e.g. 120503)")
        self.scheme_input.setMinimumHeight(34)

        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("Display name (auto-filled on search)")
        self.name_input.setMinimumHeight(34)

        form = QFormLayout()
        form.setSpacing(10)
        self.units_input = QDoubleSpinBox()
        self.units_input.setDecimals(3); self.units_input.setMaximum(999999); self.units_input.setMinimumHeight(34)
        self.nav_input = QDoubleSpinBox()
        self.nav_input.setDecimals(4); self.nav_input.setMaximum(99999); self.nav_input.setMinimumHeight(34)
        self.date_input = QDateEdit(QDate.currentDate())
        self.date_input.setCalendarPopup(True); self.date_input.setMinimumHeight(34)
        self.date_input.setDisplayFormat("dd MMM yyyy")

        form.addRow("Units Purchased:", self.units_input)
        form.addRow("Purchase NAV (₹):", self.nav_input)
        form.addRow("Purchase Date:", self.date_input)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self._accept)
        btns.rejected.connect(self.reject)

        layout.addWidget(lbl_search)
        layout.addLayout(row)
        layout.addWidget(self.fund_combo)
        layout.addWidget(sep)
        layout.addWidget(QLabel("Scheme Code:"))
        layout.addWidget(self.scheme_input)
        layout.addWidget(QLabel("Fund Display Name:"))
        layout.addWidget(self.name_input)
        layout.addLayout(form)
        layout.addWidget(btns)

        self.fund_combo.currentIndexChanged.connect(self._on_fund_select)

    def _do_search(self):
        q = self.search_input.text().strip()
        if not q:
            return
        self.fund_combo.clear()
        self.fund_combo.addItem("Searching…")
        QApplication.processEvents()
        results = search_funds(q)
        self.fund_list = results
        self.fund_combo.clear()
        if results:
            for r in results:
                self.fund_combo.addItem(r.get("schemeName", ""), r.get("schemeCode", ""))
        else:
            self.fund_combo.addItem("No results found")

    def _on_fund_select(self, idx):
        if idx < 0 or idx >= len(self.fund_list):
            return
        fund = self.fund_list[idx]
        self.scheme_input.setText(str(fund.get("schemeCode", "")))
        self.name_input.setText(fund.get("schemeName", ""))

    def _prefill(self, fund):
        self.scheme_input.setText(fund.get("scheme_code", ""))
        self.name_input.setText(fund.get("name", ""))
        self.units_input.setValue(fund.get("units", 0))
        self.nav_input.setValue(fund.get("purchase_nav", 0))
        date_str = fund.get("purchase_date", "")
        if date_str:
            qd = QDate.fromString(date_str, "yyyy-MM-dd")
            if qd.isValid():
                self.date_input.setDate(qd)
        # In edit mode, lock scheme code & name to prevent accidental change of identity
        self.scheme_input.setReadOnly(True)
        self.scheme_input.setStyleSheet("background: #0d1117; color: #8b949e; border: 1px solid #30363d; border-radius: 5px; padding: 6px 10px;")
        # Hide search section — not needed when editing
        self.search_section_visible(False)

    def search_section_visible(self, visible):
        # Hide/show search widgets (first 4 items in layout: label, search row, combo, sep + code label handled separately)
        for i in range(self.layout().count()):
            item = self.layout().itemAt(i)
            w = item.widget() if item else None
            layout = item.layout() if item else None
            if i < 5:  # search label, search row, combo, separator
                if w:
                    w.setVisible(visible)
                if layout:
                    for j in range(layout.count()):
                        sub = layout.itemAt(j)
                        if sub and sub.widget():
                            sub.widget().setVisible(visible)

    def _accept(self):
        code = self.scheme_input.text().strip() or (
            str(self.fund_list[self.fund_combo.currentIndex()].get("schemeCode", ""))
            if self.fund_list else ""
        )
        name = self.name_input.text().strip() or "Unknown Fund"
        units = self.units_input.value()
        nav = self.nav_input.value()
        date = self.date_input.date().toString("yyyy-MM-dd")

        if not code:
            QMessageBox.warning(self, "Missing", "Please provide a scheme code.")
            return
        if units <= 0 or nav <= 0:
            QMessageBox.warning(self, "Invalid", "Units and NAV must be > 0.")
            return

        # Check if purchase date changed — if so, clear nav_history so it gets re-fetched
        old_date = self.existing_fund.get("purchase_date", "")
        nav_history = self.existing_fund.get("nav_history", [])
        if date != old_date:
            nav_history = []

        self.result_fund = {
            "scheme_code": code,
            "name": name,
            "units": units,
            "purchase_nav": nav,
            "purchase_date": date,
            "nav_history": nav_history
        }
        self.accept()


# ── NAV Chart ─────────────────────────────────────────────────────────────────
COMPARE_COLORS = [
    "#58a6ff", "#3fb950", "#e3b341", "#f78166", "#bc8cff",
    "#39d353", "#ff7b72", "#79c0ff", "#d2a8ff", "#56d364"
]

class NavChart(FigureCanvas):
    def __init__(self, parent=None):
        self.fig = Figure(facecolor="#0d1117", tight_layout=True)
        self.ax = self.fig.add_subplot(111)
        super().__init__(self.fig)
        self.setParent(parent)
        self._style_ax()

        # Tooltip state
        self._tooltip_text = None
        self._crosshair_v = None
        self._crosshair_h = None
        self._tooltip_box = None
        self._mode = "single"          # "single" or "compare"
        self._plot_data = []           # list of (dates, values, label, color)

        self.mpl_connect("motion_notify_event", self._on_mouse_move)
        self.mpl_connect("axes_leave_event",    self._on_axes_leave)

    def _style_ax(self):
        self.ax.set_facecolor("#0d1117")
        self.ax.tick_params(colors="#8b949e", labelsize=8)
        for spine in self.ax.spines.values():
            spine.set_color("#30363d")
        self.ax.grid(True, color="#21262d", linewidth=0.7, linestyle="--")
        self.ax.set_xlabel("Date", color="#8b949e", fontsize=9)

    def _format_xaxis(self, dates):
        """Smart quarterly ticks — always show ~4 ticks/year, adapt for short ranges."""
        if not dates:
            return
        span_days = (dates[-1] - dates[0]).days
        if span_days <= 180:           # ≤ 6 months → monthly
            self.ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        elif span_days <= 540:         # ≤ 18 months → every 2 months
            self.ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        else:                          # > 18 months → quarterly (Mar/Jun/Sep/Dec)
            self.ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[3, 6, 9, 12]))
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter("%b'%y"))
        self.fig.autofmt_xdate(rotation=35)

    # ── Crosshair tooltip ─────────────────────────────────────────────────────
    def _nearest(self, x_num, dates, values):
        """Return index of nearest data point to mouse x position."""
        if not dates:
            return None
        diffs = [abs(mdates.date2num(d) - x_num) for d in dates]
        return diffs.index(min(diffs))

    def _on_axes_leave(self, event):
        self._clear_crosshair()
        self.draw_idle()

    def _clear_crosshair(self):
        if self._crosshair_v:
            try: self._crosshair_v.remove()
            except: pass
            self._crosshair_v = None
        if self._crosshair_h:
            try: self._crosshair_h.remove()
            except: pass
            self._crosshair_h = None
        if self._tooltip_box:
            try: self._tooltip_box.remove()
            except: pass
            self._tooltip_box = None

    def _on_mouse_move(self, event):
        if event.inaxes != self.ax or not self._plot_data:
            self._clear_crosshair()
            self.draw_idle()
            return

        self._clear_crosshair()
        x_num = event.xdata

        # Build tooltip lines — one per series
        lines = []
        snap_y = None
        for dates, values, label, color in self._plot_data:
            idx = self._nearest(x_num, dates, values)
            if idx is None:
                continue
            d = dates[idx]
            v = values[idx]
            if snap_y is None:
                snap_y = v
                snap_x = mdates.date2num(d)
            date_str = d.strftime("%d %b %Y")
            if self._mode == "single":
                lines.append(f"{date_str}    ₹{v:.2f}")
            else:
                short = label[:22] + "…" if len(label) > 22 else label
                lines.append(f"{short}: {v:.1f}  ({date_str})")

        if not lines or snap_y is None:
            self.draw_idle()
            return

        # Draw crosshair lines
        self._crosshair_v = self.ax.axvline(
            snap_x, color="#8b949e", linewidth=0.8, linestyle=":", zorder=5)
        self._crosshair_h = self.ax.axhline(
            snap_y, color="#8b949e", linewidth=0.8, linestyle=":", zorder=5)

        # Draw tooltip box — position smartly so it stays inside axes
        tooltip_txt = "\n".join(lines)
        xlim = self.ax.get_xlim()
        ylim = self.ax.get_ylim()
        x_frac = (snap_x - xlim[0]) / (xlim[1] - xlim[0])
        ha = "left" if x_frac < 0.7 else "right"
        x_off = 0.012 if ha == "left" else -0.012
        x_pos = snap_x + x_off * (xlim[1] - xlim[0])
        y_pos = ylim[0] + 0.97 * (ylim[1] - ylim[0])

        self._tooltip_box = self.ax.text(
            x_pos, y_pos, tooltip_txt,
            fontsize=8, color="#e6edf3", va="top", ha=ha, zorder=10,
            bbox=dict(boxstyle="round,pad=0.4", facecolor="#161b22",
                      edgecolor="#30363d", alpha=0.92)
        )
        self.draw_idle()

    # ── Plot helpers ──────────────────────────────────────────────────────────
    def _store_and_draw(self, data_series, mode):
        """Cache series for tooltip use and redraw."""
        self._plot_data = data_series
        self._mode = mode

    def plot(self, fund):
        self.ax.clear()
        self._style_ax()
        self._clear_crosshair()
        self._plot_data = []
        self.ax.set_ylabel("NAV (₹)", color="#8b949e", fontsize=9)

        history = fund.get("nav_history", [])
        if not history:
            self.ax.text(0.5, 0.5, "No NAV data yet.\nClick 'Refresh NAV' to load.",
                         ha="center", va="center", color="#8b949e", fontsize=11,
                         transform=self.ax.transAxes)
            self.draw()
            return

        dates = [datetime.datetime.strptime(h["date"], "%Y-%m-%d") for h in history]
        navs  = [h["nav"] for h in history]
        purchase_nav = fund["purchase_nav"]
        current_nav  = navs[-1] if navs else purchase_nav
        color = "#3fb950" if current_nav >= purchase_nav else "#f85149"

        self.ax.fill_between(dates, navs, alpha=0.15, color=color)
        self.ax.plot(dates, navs, color=color, linewidth=1.8, zorder=3)
        self.ax.axhline(purchase_nav, color="#e3b341", linewidth=1.2,
                        linestyle="--", label=f"Buy NAV ₹{purchase_nav:.2f}")

        self._format_xaxis(dates)
        self.ax.set_title(fund["name"], color="#e6edf3", fontsize=10, pad=8)
        self.ax.legend(facecolor="#161b22", edgecolor="#30363d",
                       labelcolor="#e6edf3", fontsize=8)
        self._store_and_draw([(dates, navs, fund["name"], color)], "single")
        self.draw()

    def plot_compare(self, funds):
        self.ax.clear()
        self._style_ax()
        self._clear_crosshair()
        self._plot_data = []
        self.ax.set_ylabel("Indexed Return (Base = 100)", color="#8b949e", fontsize=9)

        all_dates = []
        plotted = 0
        series = []
        for i, fund in enumerate(funds):
            history = fund.get("nav_history", [])
            if not history:
                continue
            color = COMPARE_COLORS[i % len(COMPARE_COLORS)]
            purchase_nav = fund["purchase_nav"]
            dates   = [datetime.datetime.strptime(h["date"], "%Y-%m-%d") for h in history]
            indexed = [(h["nav"] / purchase_nav) * 100 for h in history]
            short   = fund["name"][:28] + "…" if len(fund["name"]) > 28 else fund["name"]

            self.ax.plot(dates, indexed, color=color, linewidth=1.8, label=short, zorder=3)
            self.ax.fill_between(dates, indexed, 100, alpha=0.07, color=color)
            all_dates.extend(dates)
            series.append((dates, indexed, fund["name"], color))
            plotted += 1

        if plotted == 0:
            self.ax.text(0.5, 0.5, "No NAV data to compare.\nClick 'Refresh NAV' first.",
                         ha="center", va="center", color="#8b949e", fontsize=11,
                         transform=self.ax.transAxes)
            self.draw()
            return

        self.ax.axhline(100, color="#e3b341", linewidth=1.0, linestyle="--",
                        label="Buy price (100)", zorder=2)
        self._format_xaxis(sorted(all_dates))
        self.ax.set_title("Portfolio Comparison — Indexed to Purchase Price",
                          color="#e6edf3", fontsize=10, pad=8)
        self.ax.legend(facecolor="#161b22", edgecolor="#30363d",
                       labelcolor="#e6edf3", fontsize=7.5,
                       loc="upper left", framealpha=0.9)
        self._store_and_draw(series, "compare")
        self.draw()


# ── Main Window ───────────────────────────────────────────────────────────────
class MFTracker(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("📈  Mutual Fund Portfolio Tracker")
        self.setMinimumSize(1150, 700)
        # Ensure Default profile exists
        meta = load_meta()
        if "profiles" not in meta:
            meta["profiles"] = ["Default"]
            save_meta(meta)
        self.current_profile = meta.get("last_active_profile", "Default")
        if self.current_profile not in list_profiles():
            self.current_profile = list_profiles()[0]
        self.portfolio = load_portfolio(self.current_profile)
        self._apply_theme()
        self._build_ui()
        self._refresh_table()
        QTimer.singleShot(800, self._auto_refresh_if_stale)

    # ── Theme ─────────────────────────────────────────────────────────────────
    def _apply_theme(self):
        self.setStyleSheet("""
            QMainWindow, QWidget { background: #0d1117; color: #e6edf3; font-family: 'Segoe UI'; }
            QTabWidget::pane { border: 1px solid #30363d; background: #0d1117; }
            QTabBar::tab { background: #161b22; color: #8b949e; padding: 8px 20px;
                           border: 1px solid #30363d; border-bottom: none; border-radius: 4px 4px 0 0; }
            QTabBar::tab:selected { background: #0d1117; color: #58a6ff; border-bottom: 2px solid #58a6ff; }
            QTableWidget { background: #0d1117; gridline-color: #21262d;
                           border: 1px solid #30363d; border-radius: 6px; }
            QTableWidget::item { padding: 6px; }
            QTableWidget::item:selected { background: #1f6feb; color: white; }
            QHeaderView::section { background: #161b22; color: #8b949e; padding: 8px;
                                   border: none; border-bottom: 1px solid #30363d;
                                   font-weight: bold; font-size: 12px; }
            QPushButton { background: #21262d; color: #e6edf3; border: 1px solid #30363d;
                          border-radius: 6px; padding: 8px 16px; font-size: 13px; }
            QPushButton:hover { background: #30363d; border-color: #58a6ff; }
            QPushButton#primary { background: #1f6feb; border-color: #1f6feb; color: white; }
            QPushButton#primary:hover { background: #388bfd; }
            QPushButton#danger { background: #da3633; border-color: #da3633; color: white; }
            QPushButton#danger:hover { background: #f85149; }
            QLineEdit, QComboBox, QDateEdit, QDoubleSpinBox {
                background: #161b22; border: 1px solid #30363d; border-radius: 5px;
                padding: 6px 10px; color: #e6edf3; font-size: 13px; }
            QLineEdit:focus, QComboBox:focus, QDateEdit:focus { border-color: #58a6ff; }
            QLabel { color: #e6edf3; }
            QScrollBar:vertical { background: #0d1117; width: 8px; }
            QScrollBar::handle:vertical { background: #30363d; border-radius: 4px; }
            QFrame#card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; }
        """)

    # ── Profile management ────────────────────────────────────────────────────
    def _repopulate_profile_combo(self):
        self.profile_combo.blockSignals(True)
        self.profile_combo.clear()
        for p in list_profiles():
            self.profile_combo.addItem(p)
        idx = self.profile_combo.findText(self.current_profile)
        self.profile_combo.setCurrentIndex(max(idx, 0))
        self.profile_combo.blockSignals(False)

    def _on_profile_switched(self, name):
        if name == self.current_profile or not name:
            return
        self.current_profile = name
        # Remember last active profile
        meta = load_meta()
        meta["last_active_profile"] = name
        save_meta(meta)
        self.portfolio = load_portfolio(self.current_profile)
        self._refresh_table()
        self._update_cards_portfolio()
        self.chart.ax.clear()
        self.chart._plot_data = []
        self.chart.draw()
        self._set_status(f"Switched to profile: {name}")
        QTimer.singleShot(800, self._auto_refresh_if_stale)

    def _add_profile(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("New Profile")
        dlg.setMinimumWidth(300)
        dlg.setStyleSheet(self.styleSheet())
        layout = QVBoxLayout(dlg)
        layout.setSpacing(12)
        layout.addWidget(QLabel("Profile name (e.g. Spouse, Parent, Child):"))
        name_input = QLineEdit()
        name_input.setPlaceholderText("Enter name…")
        name_input.setMinimumHeight(34)
        layout.addWidget(name_input)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)
        layout.addWidget(btns)
        if dlg.exec_() == QDialog.Accepted:
            name = name_input.text().strip()
            if not name:
                return
            if name in list_profiles():
                QMessageBox.warning(self, "Exists", f"Profile '{name}' already exists.")
                return
            add_profile(name)
            self.current_profile = name
            self.portfolio = []
            meta = load_meta()
            meta["last_active_profile"] = name
            save_meta(meta)
            self._repopulate_profile_combo()
            self._refresh_table()
            self._set_status(f"Profile '{name}' created")

    def _rename_profile(self):
        old = self.current_profile
        dlg = QDialog(self)
        dlg.setWindowTitle("Rename Profile")
        dlg.setMinimumWidth(300)
        dlg.setStyleSheet(self.styleSheet())
        layout = QVBoxLayout(dlg)
        layout.setSpacing(12)
        layout.addWidget(QLabel(f"Rename '{old}' to:"))
        name_input = QLineEdit(old)
        name_input.setMinimumHeight(34)
        layout.addWidget(name_input)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)
        layout.addWidget(btns)
        if dlg.exec_() == QDialog.Accepted:
            new = name_input.text().strip()
            if not new or new == old:
                return
            if new in list_profiles():
                QMessageBox.warning(self, "Exists", f"Profile '{new}' already exists.")
                return
            # Rename = add new, copy data, remove old
            data = load_portfolio(old)
            add_profile(new)
            save_portfolio(data, new)
            # Copy refresh timestamp
            meta = load_meta()
            meta[f"last_refreshed_{new}"] = meta.pop(f"last_refreshed_{old}", None)
            meta["last_active_profile"] = new
            save_meta(meta)
            remove_profile(old)
            self.current_profile = new
            self.portfolio = data
            self._repopulate_profile_combo()
            self._refresh_table()
            self._set_status(f"Renamed to '{new}'")

    def _delete_profile(self):
        name = self.current_profile
        if name == "Default" and len(list_profiles()) == 1:
            QMessageBox.warning(self, "Cannot Delete", "You must have at least one profile.")
            return
        reply = QMessageBox.question(
            self, "Delete Profile",
            f"Delete profile '{name}' and all its fund data?\nThis cannot be undone.",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            remove_profile(name)
            profiles = list_profiles()
            self.current_profile = profiles[0]
            self.portfolio = load_portfolio(self.current_profile)
            meta = load_meta()
            meta["last_active_profile"] = self.current_profile
            save_meta(meta)
            self._repopulate_profile_combo()
            self._refresh_table()
            self._set_status(f"Deleted profile '{name}'")

    # ── UI ────────────────────────────────────────────────────────────────────
    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(16, 12, 16, 12)
        root.setSpacing(10)

        # Header
        hdr = QHBoxLayout()
        title = QLabel("📈  Mutual Fund Portfolio Tracker")
        title.setFont(QFont("Segoe UI", 17, QFont.Bold))
        title.setStyleSheet("color: #58a6ff;")

        # Profile controls
        profile_lbl = QLabel("👤  Profile:")
        profile_lbl.setStyleSheet("color: #8b949e; font-size: 12px;")
        self.profile_combo = QComboBox()
        self.profile_combo.setMinimumWidth(160)
        self.profile_combo.setMaximumWidth(220)
        self._repopulate_profile_combo()
        self.profile_combo.currentTextChanged.connect(self._on_profile_switched)

        btn_add_profile = QPushButton("＋")
        btn_add_profile.setFixedWidth(32)
        btn_add_profile.setToolTip("Add new profile")
        btn_add_profile.clicked.connect(self._add_profile)

        btn_rename_profile = QPushButton("✎")
        btn_rename_profile.setFixedWidth(32)
        btn_rename_profile.setToolTip("Rename profile")
        btn_rename_profile.clicked.connect(self._rename_profile)

        btn_del_profile = QPushButton("✕")
        btn_del_profile.setFixedWidth(32)
        btn_del_profile.setToolTip("Delete profile")
        btn_del_profile.setObjectName("danger")
        btn_del_profile.clicked.connect(self._delete_profile)

        self.status_lbl = QLabel("")
        self.status_lbl.setStyleSheet("color: #8b949e; font-size: 11px;")

        hdr.addWidget(title)
        hdr.addStretch()
        hdr.addWidget(profile_lbl)
        hdr.addWidget(self.profile_combo)
        hdr.addWidget(btn_add_profile)
        hdr.addWidget(btn_rename_profile)
        hdr.addWidget(btn_del_profile)
        hdr.addSpacing(16)
        hdr.addWidget(self.status_lbl)
        root.addLayout(hdr, stretch=0)

        # Summary cards — fixed height, no stretch
        self.summary_row = QHBoxLayout()
        self.card_invested, self.lbl_invested = self._make_card("Total Invested", "₹0", "#e6edf3")
        self.card_current,  self.lbl_current  = self._make_card("Current Value",  "₹0", "#e6edf3")
        self.card_pl,       self.lbl_pl       = self._make_card("P&L",            "₹0", "#e6edf3")
        self.card_xirr,     self.lbl_xirr     = self._make_card("Portfolio XIRR", "—",  "#e6edf3")
        for c in [self.card_invested, self.card_current, self.card_pl, self.card_xirr]:
            c.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            self.summary_row.addWidget(c)
        root.addLayout(self.summary_row, stretch=0)

        # Tabs — takes ALL remaining vertical space
        self.tabs = QTabWidget()
        self.tabs.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        root.addWidget(self.tabs, stretch=1)

        # Tab 1 – Portfolio
        portfolio_tab = QWidget()
        ptab_layout = QVBoxLayout(portfolio_tab)
        ptab_layout.setContentsMargins(0, 8, 0, 0)

        # Toolbar
        toolbar = QHBoxLayout()
        btn_add = QPushButton("＋  Add Fund")
        btn_add.setObjectName("primary")
        btn_add.clicked.connect(self._add_fund)
        btn_csv = QPushButton("📂  Import CSV")
        btn_csv.clicked.connect(self._import_csv)
        btn_refresh = QPushButton("🔄  Refresh NAV")
        btn_refresh.clicked.connect(self._refresh_all_nav)
        btn_edit = QPushButton("✏️  Edit Fund")
        btn_edit.clicked.connect(self._edit_fund)
        btn_del = QPushButton("🗑  Remove")
        btn_del.setObjectName("danger")
        btn_del.clicked.connect(self._remove_fund)
        btn_export = QPushButton("💾  Export CSV")
        btn_export.clicked.connect(self._export_csv)
        for b in [btn_add, btn_csv, btn_refresh, btn_edit, btn_del, btn_export]:
            toolbar.addWidget(b)
        toolbar.addStretch()
        ptab_layout.addLayout(toolbar)

        # Table
        self.table = QTableWidget()
        cols = ["Fund Name", "Scheme Code", "Units", "Buy NAV (₹)",
                "Buy Date", "Current NAV (₹)", "Invested (₹)", "Current (₹)", "P&L (₹)", "P&L %", "XIRR %"]
        self.table.setColumnCount(len(cols))
        self.table.setHorizontalHeaderLabels(cols)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet(self.table.styleSheet() +
            "QTableWidget { alternate-background-color: #161b22; }")
        self.table.verticalHeader().setVisible(False)
        self.table.doubleClicked.connect(lambda: self._edit_fund())
        self.table.itemSelectionChanged.connect(self._on_table_selection_changed)
        ptab_layout.addWidget(self.table)
        self.tabs.addTab(portfolio_tab, "📋  Portfolio")

        # Tab 2 – Charts
        chart_tab = QWidget()
        chart_tab.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        chart_layout = QVBoxLayout(chart_tab)
        chart_layout.setContentsMargins(8, 8, 8, 8)
        chart_layout.setSpacing(8)

        chart_ctrl = QHBoxLayout()
        chart_ctrl.setSpacing(10)

        # Mode toggle buttons
        self.btn_single = QPushButton("Single Fund")
        self.btn_single.setObjectName("primary")
        self.btn_single.setCheckable(True)
        self.btn_single.setChecked(True)
        self.btn_single.setFixedWidth(110)
        self.btn_single.clicked.connect(self._switch_to_single)

        self.btn_compare = QPushButton("Compare All")
        self.btn_compare.setCheckable(True)
        self.btn_compare.setFixedWidth(110)
        self.btn_compare.clicked.connect(self._switch_to_compare)

        self.fund_selector = QComboBox()
        self.fund_selector.setMinimumWidth(300)
        self.fund_selector.currentIndexChanged.connect(self._plot_selected)

        self.compare_hint = QLabel("All funds normalised to 100 at purchase — compare relative returns")
        self.compare_hint.setStyleSheet("color: #8b949e; font-size: 11px;")
        self.compare_hint.setVisible(False)

        chart_ctrl.addWidget(self.btn_single)
        chart_ctrl.addWidget(self.btn_compare)
        chart_ctrl.addSpacing(12)
        chart_ctrl.addWidget(self.fund_selector)
        chart_ctrl.addWidget(self.compare_hint)
        chart_ctrl.addStretch()
        chart_layout.addLayout(chart_ctrl, stretch=0)

        self.chart = NavChart()
        self.chart.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.chart.setMinimumHeight(200)
        chart_layout.addWidget(self.chart, stretch=1)
        self.tabs.addTab(chart_tab, "📈  NAV Chart")

        # Tab 3 – CSV Format Help
        self.tabs.addTab(self._build_help_tab(), "❓  CSV Format")

    # ── Summary Cards ─────────────────────────────────────────────────────────
    def _make_card(self, label, value, color):
        frame = QFrame()
        frame.setObjectName("card")
        frame.setFixedHeight(76)
        v = QVBoxLayout(frame)
        v.setContentsMargins(14, 10, 14, 10)
        lbl = QLabel(label)
        lbl.setStyleSheet("color: #8b949e; font-size: 11px;")
        val = QLabel(value)
        val.setFont(QFont("Segoe UI", 16, QFont.Bold))
        val.setStyleSheet(f"color: {color};")
        v.addWidget(lbl)
        v.addWidget(val)
        # Return both — frame goes into layout, val_label is stored for direct updates
        return frame, val

    def _update_card(self, val_label, value, color="#e6edf3"):
        val_label.setText(value)
        val_label.setStyleSheet(f"color: {color}; font-size: 16px; font-weight: bold;")

    # ── Help Tab ──────────────────────────────────────────────────────────────
    def _build_help_tab(self):
        w = QWidget()
        v = QVBoxLayout(w)
        v.setContentsMargins(20, 16, 20, 16)
        t = QLabel("""<h3 style='color:#58a6ff'>CSV Import Format</h3>
<p style='color:#8b949e'>Your CSV file should have the following columns (with header row):</p>
<pre style='background:#161b22;padding:14px;border-radius:6px;color:#3fb950;font-size:13px'>
scheme_code,name,units,purchase_nav,purchase_date
120503,SBI Bluechip Fund,100,45.23,2022-06-15
118989,HDFC Mid-Cap Opportunities,50,78.45,2023-01-10
</pre>
<ul style='color:#8b949e;line-height:1.8'>
  <li><b style='color:#e6edf3'>scheme_code</b> – AMFI scheme code (find at mfapi.in)</li>
  <li><b style='color:#e6edf3'>name</b> – Display name of the fund</li>
  <li><b style='color:#e6edf3'>units</b> – Number of units purchased</li>
  <li><b style='color:#e6edf3'>purchase_nav</b> – NAV at time of purchase</li>
  <li><b style='color:#e6edf3'>purchase_date</b> – Date in YYYY-MM-DD format</li>
</ul>
<p style='color:#8b949e'>After import, click <b style='color:#e6edf3'>🔄 Refresh NAV</b> to fetch live prices.</p>
""")
        t.setTextFormat(Qt.RichText)
        t.setWordWrap(True)
        t.setStyleSheet("font-size: 13px;")
        v.addWidget(t)
        v.addStretch()
        return w

    # ── Portfolio CRUD ────────────────────────────────────────────────────────
    def _add_fund(self):
        dlg = AddFundDialog(self)
        if dlg.exec_() == QDialog.Accepted:
            fund = dlg.result_fund
            self.portfolio.append(fund)
            save_portfolio(self.portfolio, self.current_profile)
            self._refresh_table()
            self._set_status(f"Added: {fund['name']}")

    def _edit_fund(self):
        rows = self.table.selectionModel().selectedRows()
        if not rows:
            QMessageBox.information(self, "Select", "Select a row to edit.")
            return
        idx = rows[0].row()
        existing = self.portfolio[idx]
        dlg = AddFundDialog(self, fund=existing)
        if dlg.exec_() == QDialog.Accepted:
            updated = dlg.result_fund
            refetch = len(updated["nav_history"]) == 0 and existing.get("nav_history")
            self.portfolio[idx] = updated
            save_portfolio(self.portfolio, self.current_profile)
            self._refresh_table()
            self._set_status(f"Updated: {updated['name']}")
            if refetch:
                reply = QMessageBox.question(
                    self, "Refresh NAV?",
                    "Purchase date changed — NAV history was cleared.\nFetch fresh NAV data now?",
                    QMessageBox.Yes | QMessageBox.No
                )
                if reply == QMessageBox.Yes:
                    self._refresh_all_nav()

    def _remove_fund(self):
        rows = self.table.selectionModel().selectedRows()
        if not rows:
            QMessageBox.information(self, "Select", "Select a row to remove.")
            return
        idx = rows[0].row()
        name = self.portfolio[idx]["name"]
        if QMessageBox.question(self, "Remove", f"Remove {name}?",
                                QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            self.portfolio.pop(idx)
            save_portfolio(self.portfolio, self.current_profile)
            self._refresh_table()
            self._set_status(f"Removed: {name}")

    def _import_csv(self):
        path, _ = QFileDialog.getOpenFileName(self, "Open CSV", "", "CSV Files (*.csv)")
        if not path:
            return
        count = 0
        errors = []
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, 1):
                try:
                    fund = {
                        "scheme_code": row["scheme_code"].strip(),
                        "name": row["name"].strip(),
                        "units": float(row["units"]),
                        "purchase_nav": float(row["purchase_nav"]),
                        "purchase_date": row["purchase_date"].strip(),
                        "nav_history": []
                    }
                    self.portfolio.append(fund)
                    count += 1
                except Exception as e:
                    errors.append(f"Row {i}: {e}")
        save_portfolio(self.portfolio, self.current_profile)
        if errors:
            msg += f"\n\nErrors:\n" + "\n".join(errors)
        QMessageBox.information(self, "Import Complete", msg)

    def _export_csv(self):
        path, _ = QFileDialog.getSaveFileName(self, "Save CSV", "portfolio.csv", "CSV Files (*.csv)")
        if not path:
            return
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["scheme_code", "name", "units", "purchase_nav", "purchase_date"])
            writer.writeheader()
            for fund in self.portfolio:
                writer.writerow({
                    "scheme_code": fund["scheme_code"],
                    "name": fund["name"],
                    "units": fund["units"],
                    "purchase_nav": fund["purchase_nav"],
                    "purchase_date": fund["purchase_date"],
                })
        self._set_status(f"Exported to {path}")

    # ── NAV Refresh ───────────────────────────────────────────────────────────
    def _auto_refresh_if_stale(self):
        if not self.portfolio:
            return
        hours = hours_since_last_refresh(self.current_profile)
        if hours >= AUTO_REFRESH_HOURS:
            if hours == float("inf"):
                msg = "Fetching NAV data for the first time…"
            else:
                h = int(hours)
                msg = f"NAV data is {h}h old — auto-refreshing in background…"
            self._set_status(msg, duration=0)
            self._refresh_all_nav(silent=True)

    def _refresh_all_nav(self, silent=False):
        if not self.portfolio:
            if not silent:
                QMessageBox.information(self, "Empty", "No funds in portfolio.")
            return
        if not silent:
            self._set_status("Fetching NAV data…", duration=0)
        self._pending = len(self.portfolio)
        self._refresh_profile = self.current_profile   # snapshot profile at refresh start
        for fund in self.portfolio:
            worker = NavFetcher(fund["scheme_code"], fund["purchase_date"])
            worker.done.connect(self._on_nav_fetched)
            worker.start()
            self._workers = getattr(self, "_workers", [])
            self._workers.append(worker)

    def _on_nav_fetched(self, code, history):
        for fund in self.portfolio:
            if fund["scheme_code"] == code:
                if history:
                    fund["nav_history"] = history
        self._pending = getattr(self, "_pending", 1) - 1
        if self._pending <= 0:
            save_portfolio(self.portfolio, self.current_profile)
            touch_last_refreshed(self.current_profile)
            self._refresh_table()
            self._set_status("NAV data updated ✓")
            self._populate_fund_selector()

    # ── Card update helpers ───────────────────────────────────────────────────
    def _compute_fund_stats(self, fund):
        """Return (invested, current_val, pl, pl_pct, xi) for a single fund."""
        history     = fund.get("nav_history", [])
        current_nav = history[-1]["nav"] if history else fund["purchase_nav"]
        invested    = fund["units"] * fund["purchase_nav"]
        current_val = fund["units"] * current_nav
        pl          = current_val - invested
        pl_pct      = (pl / invested * 100) if invested else 0
        cf          = [(fund["purchase_date"], -invested),
                       (datetime.date.today().isoformat(), current_val)]
        xi          = xirr(cf)
        return invested, current_val, pl, pl_pct, xi

    def _update_cards_for_fund(self, fund):
        """Show metrics for a single selected fund."""
        invested, current_val, pl, pl_pct, xi = self._compute_fund_stats(fund)
        pl_color = "#3fb950" if pl >= 0 else "#f85149"
        xi_color = "#3fb950" if xi and xi >= 0 else "#f85149"
        # Shorten name for card label
        short = fund["name"][:30] + "…" if len(fund["name"]) > 30 else fund["name"]
        self._set_card_label(self.card_invested, f"Invested · {short}")
        self._set_card_label(self.card_current,  "Current Value")
        self._set_card_label(self.card_pl,       "P&L")
        self._set_card_label(self.card_xirr,     "XIRR")
        self._update_card(self.lbl_invested, f"₹{invested:,.0f}")
        self._update_card(self.lbl_current,  f"₹{current_val:,.0f}")
        self._update_card(self.lbl_pl,       f"₹{pl:+,.0f}  ({pl_pct:+.1f}%)", pl_color)
        self._update_card(self.lbl_xirr,     f"{xi:.2f}%" if xi is not None else "—", xi_color)

    def _update_cards_portfolio(self):
        """Show aggregated portfolio metrics."""
        total_inv = total_cur = 0
        all_cf = []
        for fund in self.portfolio:
            inv, cur, _, _, _ = self._compute_fund_stats(fund)
            total_inv += inv
            total_cur += cur
            all_cf.append([(fund["purchase_date"], -inv),
                            (datetime.date.today().isoformat(), cur)])
        total_pl    = total_cur - total_inv
        pl_color    = "#3fb950" if total_pl >= 0 else "#f85149"
        merged      = sorted([item for cf in all_cf for item in cf], key=lambda x: x[0])
        xi_port     = xirr(merged) if merged else None
        xi_color    = "#3fb950" if xi_port and xi_port >= 0 else "#f85149"

        self._set_card_label(self.card_invested, "Total Invested")
        self._set_card_label(self.card_current,  "Current Value")
        self._set_card_label(self.card_pl,       "P&L")
        self._set_card_label(self.card_xirr,     "Portfolio XIRR")
        self._update_card(self.lbl_invested, f"₹{total_inv:,.0f}")
        self._update_card(self.lbl_current,  f"₹{total_cur:,.0f}")
        self._update_card(self.lbl_pl,       f"₹{total_pl:+,.0f}", pl_color)
        self._update_card(self.lbl_xirr,     f"{xi_port:.2f}%" if xi_port is not None else "—", xi_color)

    def _set_card_label(self, frame, text):
        """Update the small top label inside a card frame."""
        lbl = frame.findChild(QLabel)   # first QLabel = the title label
        if lbl:
            lbl.setText(text)

    # ── Table refresh ─────────────────────────────────────────────────────────
    def _refresh_table(self):
        self.table.setRowCount(0)
        total_inv = total_cur = 0
        all_cf = []

        for fund in self.portfolio:
            row = self.table.rowCount()
            self.table.insertRow(row)

            history = fund.get("nav_history", [])
            current_nav = history[-1]["nav"] if history else fund["purchase_nav"]
            invested = fund["units"] * fund["purchase_nav"]
            current_val = fund["units"] * current_nav
            pl = current_val - invested
            pl_pct = (pl / invested * 100) if invested else 0

            # XIRR
            cf = [(fund["purchase_date"], -invested),
                  (datetime.date.today().isoformat(), current_val)]
            xi = xirr(cf)
            all_cf.append(cf)
            total_inv += invested
            total_cur += current_val

            def cell(txt, align=Qt.AlignRight):
                item = QTableWidgetItem(str(txt))
                item.setTextAlignment(align | Qt.AlignVCenter)
                return item

            self.table.setItem(row, 0, cell(fund["name"], Qt.AlignLeft))
            self.table.setItem(row, 1, cell(fund["scheme_code"]))
            self.table.setItem(row, 2, cell(f"{fund['units']:.3f}"))
            self.table.setItem(row, 3, cell(f"{fund['purchase_nav']:.4f}"))
            self.table.setItem(row, 4, cell(fund["purchase_date"]))
            self.table.setItem(row, 5, cell(f"{current_nav:.4f}"))
            self.table.setItem(row, 6, cell(f"₹{invested:,.2f}"))
            self.table.setItem(row, 7, cell(f"₹{current_val:,.2f}"))

            pl_item = cell(f"₹{pl:,.2f}")
            pl_item.setForeground(QColor("#3fb950" if pl >= 0 else "#f85149"))
            self.table.setItem(row, 8, pl_item)

            pct_item = cell(f"{pl_pct:+.2f}%")
            pct_item.setForeground(QColor("#3fb950" if pl_pct >= 0 else "#f85149"))
            self.table.setItem(row, 9, pct_item)

            xi_item = cell(f"{xi:.2f}%" if xi is not None else "—")
            if xi is not None:
                xi_item.setForeground(QColor("#3fb950" if xi >= 0 else "#f85149"))
            self.table.setItem(row, 10, xi_item)

        # Summary cards — always show portfolio totals after a full table refresh
        self._update_cards_portfolio()
        self._populate_fund_selector()

    def _on_table_selection_changed(self):
        rows = self.table.selectionModel().selectedRows()
        if rows:
            idx = rows[0].row()
            if 0 <= idx < len(self.portfolio):
                self._update_cards_for_fund(self.portfolio[idx])
        else:
            self._update_cards_portfolio()

    def _switch_to_single(self):
        self.btn_single.setObjectName("primary")
        self.btn_single.setChecked(True)
        self.btn_compare.setObjectName("")
        self.btn_compare.setChecked(False)
        self.fund_selector.setVisible(True)
        self.compare_hint.setVisible(False)
        self.btn_single.setStyleSheet("")
        self.btn_compare.setStyleSheet("")
        self._plot_selected(self.fund_selector.currentIndex())

    def _switch_to_compare(self):
        self.btn_compare.setObjectName("primary")
        self.btn_compare.setChecked(True)
        self.btn_single.setObjectName("")
        self.btn_single.setChecked(False)
        self.fund_selector.setVisible(False)
        self.compare_hint.setVisible(True)
        self.btn_single.setStyleSheet("")
        self.btn_compare.setStyleSheet("")
        self.chart.plot_compare(self.portfolio)
        # Compare = all funds → show portfolio totals
        self._update_cards_portfolio()

    def _populate_fund_selector(self):
        self.fund_selector.blockSignals(True)
        current = self.fund_selector.currentIndex()
        self.fund_selector.clear()
        for fund in self.portfolio:
            self.fund_selector.addItem(fund["name"])
        self.fund_selector.setCurrentIndex(current if current < len(self.portfolio) else 0)
        self.fund_selector.blockSignals(False)
        # Refresh whichever mode is active
        if self.btn_compare.isChecked():
            self.chart.plot_compare(self.portfolio)
        else:
            self._plot_selected(self.fund_selector.currentIndex())

    def _plot_selected(self, idx):
        if 0 <= idx < len(self.portfolio):
            fund = self.portfolio[idx]
            self.chart.plot(fund)
            self._update_cards_for_fund(fund)

    def _set_status(self, msg, duration=5000):
        self.status_lbl.setText(msg)
        if duration > 0:
            QTimer.singleShot(duration, lambda: self.status_lbl.setText(""))


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    win = MFTracker()
    win.show()
    sys.exit(app.exec_())
