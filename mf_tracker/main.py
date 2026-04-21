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

import bisect
import matplotlib
matplotlib.use('Qt5Agg')
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker

# ── Custom Table Item for Numeric Sorting ────────────────────────────────────
class NumericTableWidgetItem(QTableWidgetItem):
    """Custom QTableWidgetItem that sorts numerically instead of alphabetically."""
    def __init__(self, text, numeric_value):
        super().__init__(text)
        self.numeric_value = numeric_value
    
    def __lt__(self, other):
        """Override less-than comparison for sorting."""
        if isinstance(other, NumericTableWidgetItem):
            return self.numeric_value < other.numeric_value
        return super().__lt__(other)

# ── Data store ────────────────────────────────────────────────────────────────
MF_DIR    = Path.home() / ".mf_tracker"
META_FILE = MF_DIR / "meta.json"
MF_DIR.mkdir(parents=True, exist_ok=True)

AUTO_REFRESH_HOURS = 8


def _profile_file(name: str) -> Path:
    safe = "".join(c if c.isalnum() or c in "-_ " else "_" for c in name)
    return MF_DIR / f"portfolio_{safe}.json"


def list_profiles() -> list:
    profiles = load_meta().get("profiles", ["Default"])
    return profiles if profiles else ["Default"]


def load_portfolio(profile: str) -> list:
    f = _profile_file(profile)
    # One-time migration from single-file v7 format
    legacy = MF_DIR / "portfolio.json"
    if legacy.exists() and not f.exists() and profile == "Default":
        import shutil; shutil.copy(legacy, f)
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


def remove_portfolio_profile(name: str):
    meta = load_meta()
    profiles = meta.get("profiles", ["Default"])
    if name in profiles and name != "Default":
        profiles.remove(name)
    meta["profiles"] = profiles
    meta.pop(f"last_refreshed_{name}", None)
    save_meta(meta)
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
    last = load_meta().get(f"last_refreshed_{profile}")
    if not last:
        return float("inf")
    return (datetime.datetime.now() - datetime.datetime.fromisoformat(last)).total_seconds() / 3600


def touch_last_refreshed(profile: str):
    meta = load_meta()
    meta[f"last_refreshed_{profile}"] = datetime.datetime.now().isoformat()
    save_meta(meta)


# ── NAV fetching ──────────────────────────────────────────────────────────────
def fetch_nav_history(scheme_code: str, from_date: str):
    """Fetch NAV history from MFAPI."""
    import threading
    import requests
    
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] Fetching {scheme_code}...")
    
    try:
        url = f"https://api.mfapi.in/mf/{scheme_code}"
        headers = {"User-Agent": "Mozilla/5.0"}
        
        # requests handles IncompleteRead automatically ✓
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        raw = response.json()
        print(f"[{thread_name}] ✓ Success")
        
        if raw.get("status") == "ERROR" or "data" not in raw:
            return [], "not_found"
        
        data = raw.get("data", [])
        if not data:
            return [], "no_data"
        
        from_dt = datetime.datetime.strptime(from_date, "%Y-%m-%d").date()
        history = []
        for entry in data:
            try:
                dt = datetime.datetime.strptime(entry["date"], "%d-%m-%Y").date()
                if dt >= from_dt:
                    history.append({"date": dt.isoformat(), "nav": float(entry["nav"])})
            except:
                continue
        history.sort(key=lambda x: x["date"])
        
        if not history:
            latest = data[0].get("date", "?")
            return [], f"date_filter|{latest}"
        
        return history, "ok"
    
    except requests.exceptions.RequestException as e:
        print(f"[{thread_name}] ✗ Error: {e}")
        return [], "network"


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


def verify_scheme(scheme_code: str, purchase_date: str):
    """Quick check — returns (ok, message) before adding a fund."""
    history, reason = fetch_nav_history(scheme_code, purchase_date)
    if reason == "ok":
        latest_nav = history[-1]["nav"]
        latest_date = history[-1]["date"]
        return True, f"✓  Found {len(history)} NAV records. Latest: ₹{latest_nav:.2f} on {latest_date}"
    elif reason == "not_found":
        return False, f"Scheme code {scheme_code} was not found on MFAPI.\nPlease check the code and try again."
    elif reason == "no_data":
        return False, f"Scheme {scheme_code} exists but has no NAV data on MFAPI.\nThis may be a very old or closed fund."
    elif reason and reason.startswith("date_filter"):
        latest = reason.split("|")[1] if "|" in reason else "unknown"
        return False, (f"Scheme {scheme_code} has NAV data but the latest entry is {latest},\n"
                       f"which is before your purchase date ({purchase_date}).\n\n"
                       f"Try setting an earlier purchase date, or this may be a wound-up fund.")
    else:
        return False, f"Could not reach MFAPI (network error).\nPlease check your internet connection and try again."
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
    done = pyqtSignal(str, list, str)   # scheme_code, history, reason

    def __init__(self, scheme_code, from_date):
        super().__init__()
        self.scheme_code = scheme_code
        self.from_date = from_date

    def run(self):
        history, reason = fetch_nav_history(self.scheme_code, self.from_date)
        self.done.emit(self.scheme_code, history, reason)


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
        nav   = self.nav_input.value()
        date  = self.date_input.date().toString("yyyy-MM-dd")

        if not code:
            QMessageBox.warning(self, "Missing", "Please provide a scheme code.")
            return
        if units <= 0 or nav <= 0:
            QMessageBox.warning(self, "Invalid", "Units and NAV must be > 0.")
            return

        # In edit mode, only verify if scheme code or date changed
        skip_verify = (
            self.edit_mode and
            code == self.existing_fund.get("scheme_code") and
            date == self.existing_fund.get("purchase_date") and
            self.existing_fund.get("nav_history")
        )

        if not skip_verify:
            # Show a "Verifying…" indicator while we check
            self.setEnabled(False)
            QApplication.setOverrideCursor(Qt.WaitCursor)
            QApplication.processEvents()
            try:
                ok, msg = verify_scheme(code, date)
            finally:
                QApplication.restoreOverrideCursor()
                self.setEnabled(True)

            if not ok:
                reply = QMessageBox.warning(
                    self, "Fund Verification Failed",
                    f"{msg}\n\nDo you still want to add this fund?\n"
                    f"(NAV chart will be empty until data is available)",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No
                )
                if reply == QMessageBox.No:
                    return
            else:
                # Show success info briefly
                QMessageBox.information(self, "Fund Verified", msg)

        # Check if purchase date changed — clear nav_history so it gets re-fetched
        old_date    = self.existing_fund.get("purchase_date", "")
        nav_history = self.existing_fund.get("nav_history", [])
        if date != old_date:
            nav_history = []

        self.result_fund = {
            "scheme_code": code,
            "name":         name,
            "units":        units,
            "purchase_nav": nav,
            "purchase_date": date,
            "nav_history":  nav_history
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
        self._mode = "single"          # "single", "compare", or "worth"
        self._plot_data = []           # list of (dates, values, label, color)
        self._ax2 = None               # secondary y-axis (ratio line)

        self.mpl_connect("motion_notify_event", self._on_mouse_move)
        self.mpl_connect("axes_leave_event",    self._on_axes_leave)

    def _clear_twin(self):
        if self._ax2 is not None:
            try:
                self._ax2.remove()
            except Exception:
                pass
            self._ax2 = None

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
        active_axes = {self.ax}
        if self._ax2 is not None:
            active_axes.add(self._ax2)
        if event.inaxes not in active_axes or not self._plot_data:
            self._clear_crosshair()
            self.draw_idle()
            return

        self._clear_crosshair()
        x_num = event.xdata

        # Worth mode: build one unified tooltip from all three series
        if self._mode == "worth" and len(self._plot_data) == 3:
            worth_dates, worth_vals, _, _ = self._plot_data[0]
            _, inv_vals,   _, _ = self._plot_data[1]
            idx = self._nearest(x_num, worth_dates, worth_vals)
            if idx is None:
                self.draw_idle()
                return
            d = worth_dates[idx]
            w = worth_vals[idx]
            i = inv_vals[idx]
            pct = (w - i) / i * 100 if i > 0 else 0.0
            snap_y  = w
            snap_x  = mdates.date2num(d)
            pct_str = f"{pct:+.1f}%"
            lines = [
                d.strftime("%d %b %Y"),
                f"Worth:    ₹{w:,.0f}",
                f"Invested: ₹{i:,.0f}",
                f"Return:   {pct_str}",
            ]
        else:
            # Build tooltip lines — one per series
            lines = []
            snap_y = None
            for dates, values, label, _ in self._plot_data:
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

    def plot(self, funds, years=None):
        """Plot single-fund NAV chart. funds is a list of portfolio entries for the same scheme."""
        if isinstance(funds, dict):
            funds = [funds]   # backward-compat for any direct callers

        self._clear_twin()
        self.ax.clear()
        self._style_ax()
        self._clear_crosshair()
        self._plot_data = []
        self.ax.set_ylabel("NAV (₹)", color="#8b949e", fontsize=9)

        # Use the entry with the most NAV history (earliest purchase date covers the widest range)
        best = max(funds, key=lambda f: len(f.get("nav_history", [])))
        history = best.get("nav_history", [])
        if years is not None:
            cutoff = (datetime.datetime.now() - datetime.timedelta(days=years * 365)).date()
            history = [h for h in history if datetime.date.fromisoformat(h["date"]) >= cutoff]
        if not history:
            self.ax.text(0.5, 0.5, "No NAV data yet.\nClick 'Refresh NAV' to load.",
                         ha="center", va="center", color="#8b949e", fontsize=11,
                         transform=self.ax.transAxes)
            self.draw()
            return

        dates = [datetime.datetime.strptime(h["date"], "%Y-%m-%d") for h in history]
        navs  = [h["nav"] for h in history]
        current_nav = navs[-1]

        # Weighted-average purchase NAV (for color decision)
        total_units    = sum(f["units"] for f in funds)
        avg_buy_nav    = sum(f["units"] * f["purchase_nav"] for f in funds) / total_units
        color = "#3fb950" if current_nav >= avg_buy_nav else "#f85149"

        self.ax.fill_between(dates, navs, alpha=0.15, color=color)
        self.ax.plot(dates, navs, color=color, linewidth=1.8, zorder=3)

        # --- SIP purchase markers ---
        SIP_COLORS = ["#e3b341", "#58a6ff", "#bc8cff", "#f78166", "#39d353",
                      "#79c0ff", "#d2a8ff", "#56d364", "#ff7b72", "#3fb950"]
        sorted_funds = sorted(funds, key=lambda f: f["purchase_date"])
        chart_start  = dates[0]
        chart_end    = dates[-1]

        for i, fund in enumerate(sorted_funds):
            pd_dt = datetime.datetime.strptime(fund["purchase_date"], "%Y-%m-%d")
            pnav  = fund["purchase_nav"]
            c     = SIP_COLORS[i % len(SIP_COLORS)]

            if len(funds) == 1:
                label = f"Buy NAV ₹{pnav:.2f}"
            else:
                label = f"SIP {i+1}  ₹{pnav:.2f}  ({fund['purchase_date']})"

            # Horizontal buy-price line (only within the visible date range)
            x0 = max(pd_dt, chart_start)
            self.ax.hlines(pnav, x0, chart_end, colors=c, linewidth=1.0,
                           linestyle="--", label=label, zorder=2, alpha=0.85)

            # Vertical marker at purchase date (if inside visible range)
            if chart_start <= pd_dt <= chart_end:
                self.ax.axvline(pd_dt, color=c, linewidth=0.8,
                                linestyle=":", alpha=0.55, zorder=2)
                # Dot on the NAV curve at the purchase date
                idx_nearest = min(range(len(dates)),
                                  key=lambda k: abs((dates[k] - pd_dt).days))
                self.ax.scatter([dates[idx_nearest]], [navs[idx_nearest]],
                                color=c, s=28, zorder=5)

        self._format_xaxis(dates)
        fund_name = funds[0]["name"]
        title = fund_name if len(funds) == 1 else f"{fund_name}  ·  {len(funds)} SIP investments"
        self.ax.set_title(title, color="#e6edf3", fontsize=10, pad=8)
        self.ax.legend(facecolor="#161b22", edgecolor="#30363d",
                       labelcolor="#e6edf3", fontsize=8)
        self._store_and_draw([(dates, navs, fund_name, color)], "single")
        self.draw()

    def plot_compare(self, funds, years=None):
        self._clear_twin()
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
            if years is not None:
                cutoff = (datetime.datetime.now() - datetime.timedelta(days=years * 365)).date()
                history = [h for h in history if datetime.date.fromisoformat(h["date"]) >= cutoff]
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

    def plot_worth(self, funds, years=None):
        self._clear_twin()
        self.ax.clear()
        self._style_ax()
        self._clear_crosshair()
        self._plot_data = []
        self.ax.set_ylabel("Portfolio Worth (₹)", color="#8b949e", fontsize=9)

        # Build per-fund lookup: sorted dates, navs, units, purchase info
        fund_data = []
        all_date_strs = set()
        for fund in funds:
            history = fund.get("nav_history", [])
            if not history:
                continue
            sorted_hist = sorted(history, key=lambda h: h["date"])
            fdates = [datetime.date.fromisoformat(h["date"]) for h in sorted_hist]
            fnavs  = [h["nav"] for h in sorted_hist]
            fund_data.append((fdates, fnavs, fund["units"],
                              fund["purchase_nav"], fund["purchase_date"]))
            all_date_strs.update(h["date"] for h in sorted_hist)

        if not fund_data:
            self.ax.text(0.5, 0.5, "No NAV data yet.\nClick 'Refresh NAV' to load.",
                         ha="center", va="center", color="#8b949e", fontsize=11,
                         transform=self.ax.transAxes)
            self.draw()
            return

        all_dates = sorted(datetime.date.fromisoformat(d) for d in all_date_strs)

        if years is not None:
            cutoff = (datetime.datetime.now() - datetime.timedelta(days=years * 365)).date()
            all_dates = [d for d in all_dates if d >= cutoff]

        if not all_dates:
            self.ax.text(0.5, 0.5, "No data in selected time range.",
                         ha="center", va="center", color="#8b949e", fontsize=11,
                         transform=self.ax.transAxes)
            self.draw()
            return

        # For each date compute total portfolio worth and cumulative invested
        worths = []
        invested_over_time = []
        for d in all_dates:
            worth = 0.0
            invested = 0.0
            for fdates, fnavs, units, purchase_nav, purchase_date_str in fund_data:
                purchase_date = datetime.date.fromisoformat(purchase_date_str)
                if d < purchase_date:
                    continue
                invested += units * purchase_nav
                idx = bisect.bisect_right(fdates, d) - 1
                if idx >= 0:
                    worth += units * fnavs[idx]
            worths.append(worth)
            invested_over_time.append(invested)

        plot_dates = [datetime.datetime(d.year, d.month, d.day) for d in all_dates]
        color = "#3fb950" if worths[-1] >= invested_over_time[-1] else "#f85149"

        self.ax.fill_between(plot_dates, worths, alpha=0.15, color=color)
        self.ax.plot(plot_dates, worths, color=color, linewidth=1.8,
                     label="Portfolio Worth", zorder=3)
        self.ax.plot(plot_dates, invested_over_time, color="#e3b341", linewidth=1.2,
                     linestyle="--", label="Invested", zorder=2)

        # Ratio line on secondary y-axis: (worth - invested) / invested * 100
        ratio_color = "#a371f7"
        ratio = [(w - i) / i * 100 if i > 0 else 0.0
                 for w, i in zip(worths, invested_over_time)]

        self._ax2 = self.ax.twinx()
        self._ax2.set_facecolor("#0d1117")
        self._ax2.tick_params(axis="y", colors="#a371f7", labelsize=8)
        for spine in self._ax2.spines.values():
            spine.set_color("#30363d")
        self._ax2.set_ylabel("Return %", color="#a371f7", fontsize=9)
        self._ax2.yaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:+.0f}%"))
        self._ax2.plot(plot_dates, ratio, color=ratio_color, linewidth=1.4,
                       linestyle=":", label="Return %", zorder=4, alpha=0.9)
        self._ax2.axhline(0, color=ratio_color, linewidth=0.6,
                          linestyle=":", alpha=0.35, zorder=1)

        def _fmt_inr(v, _):
            if v >= 1e7:   return f"₹{v/1e7:.1f}Cr"
            if v >= 1e5:   return f"₹{v/1e5:.1f}L"
            if v >= 1e3:   return f"₹{v/1e3:.0f}K"
            return f"₹{v:.0f}"

        self.ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(_fmt_inr))
        self._format_xaxis(plot_dates)
        self.ax.set_title("Total Portfolio Worth Over Time",
                          color="#e6edf3", fontsize=10, pad=8)

        # Combined legend from both axes
        lines1, labels1 = self.ax.get_legend_handles_labels()
        lines2, labels2 = self._ax2.get_legend_handles_labels()
        self.ax.legend(lines1 + lines2, labels1 + labels2,
                       facecolor="#161b22", edgecolor="#30363d",
                       labelcolor="#e6edf3", fontsize=8)
        self._store_and_draw([
            (plot_dates, worths,            "Worth",    color),
            (plot_dates, invested_over_time, "Invested", "#e3b341"),
            (plot_dates, ratio,             "Return%",  ratio_color),
        ], "worth")
        self.draw()


# ── Main Window ───────────────────────────────────────────────────────────────
class MFTracker(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("📈  Mutual Fund Portfolio Tracker")
        self.setMinimumSize(1150, 700)
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
        meta = load_meta()
        meta["last_active_profile"] = name
        save_meta(meta)
        self.portfolio = load_portfolio(self.current_profile)
        self._refresh_table()
        self._update_cards_portfolio()
        self.chart.ax.clear()
        self.chart._plot_data = []
        self.chart.draw()
        self._set_status(f"Switched to: {name}")

    def _add_profile(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("New Profile")
        dlg.setMinimumWidth(300)
        dlg.setStyleSheet(self.styleSheet())
        layout = QVBoxLayout(dlg)
        layout.setSpacing(12)
        layout.addWidget(QLabel("Profile name (e.g. Spouse, Parent, Child):"))
        inp = QLineEdit()
        inp.setPlaceholderText("Enter name…")
        inp.setMinimumHeight(34)
        layout.addWidget(inp)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)
        layout.addWidget(btns)
        if dlg.exec_() == QDialog.Accepted:
            name = inp.text().strip()
            if not name:
                return
            if name in list_profiles():
                QMessageBox.warning(self, "Exists", f"'{name}' already exists.")
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
        inp = QLineEdit(old)
        inp.setMinimumHeight(34)
        layout.addWidget(inp)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)
        layout.addWidget(btns)
        if dlg.exec_() == QDialog.Accepted:
            new = inp.text().strip()
            if not new or new == old:
                return
            if new in list_profiles():
                QMessageBox.warning(self, "Exists", f"'{new}' already exists.")
                return
            data = load_portfolio(old)
            add_profile(new)
            save_portfolio(data, new)
            meta = load_meta()
            meta[f"last_refreshed_{new}"] = meta.pop(f"last_refreshed_{old}", None)
            meta["last_active_profile"] = new
            save_meta(meta)
            remove_portfolio_profile(old)
            self.current_profile = new
            self.portfolio = data
            self._repopulate_profile_combo()
            self._refresh_table()
            self._set_status(f"Renamed to '{new}'")

    def _delete_profile(self):
        name = self.current_profile
        if len(list_profiles()) == 1:
            QMessageBox.warning(self, "Cannot Delete", "You must have at least one profile.")
            return
        if QMessageBox.question(self, "Delete Profile",
                                f"Delete '{name}' and all its fund data?\nThis cannot be undone.",
                                QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            remove_portfolio_profile(name)
            self.current_profile = list_profiles()[0]
            self.portfolio = load_portfolio(self.current_profile)
            meta = load_meta()
            meta["last_active_profile"] = self.current_profile
            save_meta(meta)
            self._repopulate_profile_combo()
            self._refresh_table()
            self._set_status(f"Deleted '{name}'")

    # ── Card context helpers ──────────────────────────────────────────────────
    def _compute_fund_stats(self, fund):
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

    def _set_card_label(self, frame, text):
        lbl = frame.findChild(QLabel)
        if lbl:
            lbl.setText(text)

    def _update_cards_for_fund(self, fund):
        invested, current_val, pl, pl_pct, xi = self._compute_fund_stats(fund)
        pl_color = "#3fb950" if pl >= 0 else "#f85149"
        xi_color = "#3fb950" if xi and xi >= 0 else "#f85149"
        short = fund["name"][:30] + "…" if len(fund["name"]) > 30 else fund["name"]
        self._set_card_label(self.card_invested, f"Invested · {short}")
        self._set_card_label(self.card_current,  "Current Value")
        self._set_card_label(self.card_pl,       "P&L")
        self._set_card_label(self.card_xirr,     "XIRR")
        self._update_card(self.lbl_invested, f"₹{invested:,.0f}")
        self._update_card(self.lbl_current,  f"₹{current_val:,.0f}")
        self._update_card(self.lbl_pl,       f"₹{pl:+,.0f}  ({pl_pct:+.1f}%)", pl_color)
        self._update_card(self.lbl_xirr,     f"{xi:.2f}%" if xi is not None else "—", xi_color)

    def _update_cards_for_consolidated(self, funds):
        """Summary cards for a single fund with multiple SIP entries."""
        total_inv = total_cur = 0
        all_cf = []
        for fund in funds:
            inv, cur, _, _, _ = self._compute_fund_stats(fund)
            total_inv += inv
            total_cur += cur
            all_cf.append([(fund["purchase_date"], -inv),
                           (datetime.date.today().isoformat(), cur)])
        total_pl = total_cur - total_inv
        pl_pct   = (total_pl / total_inv * 100) if total_inv else 0
        pl_color = "#3fb950" if total_pl >= 0 else "#f85149"
        merged   = sorted([i for cf in all_cf for i in cf], key=lambda x: x[0])
        xi       = xirr(merged) if merged else None
        xi_color = "#3fb950" if xi and xi >= 0 else "#f85149"
        short = funds[0]["name"][:30] + "…" if len(funds[0]["name"]) > 30 else funds[0]["name"]
        self._set_card_label(self.card_invested, f"Invested · {short}")
        self._set_card_label(self.card_current,  "Current Value")
        self._set_card_label(self.card_pl,        "P&L")
        self._set_card_label(self.card_xirr,      "XIRR")
        self._update_card(self.lbl_invested, f"₹{total_inv:,.0f}")
        self._update_card(self.lbl_current,  f"₹{total_cur:,.0f}")
        self._update_card(self.lbl_pl,       f"₹{total_pl:+,.0f}  ({pl_pct:+.1f}%)", pl_color)
        self._update_card(self.lbl_xirr,     f"{xi:.2f}%" if xi is not None else "—", xi_color)

    def _update_cards_portfolio(self):
        total_inv = total_cur = 0
        all_cf = []
        for fund in self.portfolio:
            inv, cur, _, _, _ = self._compute_fund_stats(fund)
            total_inv += inv
            total_cur += cur
            all_cf.append([(fund["purchase_date"], -inv),
                           (datetime.date.today().isoformat(), cur)])
        total_pl = total_cur - total_inv
        pl_color = "#3fb950" if total_pl >= 0 else "#f85149"
        merged   = sorted([i for cf in all_cf for i in cf], key=lambda x: x[0])
        xi_port  = xirr(merged) if merged else None
        xi_color = "#3fb950" if xi_port and xi_port >= 0 else "#f85149"
        self._set_card_label(self.card_invested, "Total Invested")
        self._set_card_label(self.card_current,  "Current Value")
        self._set_card_label(self.card_pl,       "P&L")
        self._set_card_label(self.card_xirr,     "Portfolio XIRR")
        self._update_card(self.lbl_invested, f"₹{total_inv:,.0f}")
        self._update_card(self.lbl_current,  f"₹{total_cur:,.0f}")
        self._update_card(self.lbl_pl,       f"₹{total_pl:+,.0f}", pl_color)
        self._update_card(self.lbl_xirr,     f"{xi_port:.2f}%" if xi_port is not None else "—", xi_color)

    # ── Sort helpers ──────────────────────────────────────────────────────────
    def _apply_sort(self, funds):
        col = self._sort_col
        if col is None:
            return list(funds)
        def sort_key(fund):
            history     = fund.get("nav_history", [])
            current_nav = history[-1]["nav"] if history else fund["purchase_nav"]
            invested    = fund["units"] * fund["purchase_nav"]
            current_val = fund["units"] * current_nav
            pl          = current_val - invested
            pl_pct      = (pl / invested * 100) if invested else 0
            xi          = xirr([(fund["purchase_date"], -invested),
                                 (datetime.date.today().isoformat(), current_val)]) or 0
            return [fund["name"].lower(), fund["scheme_code"], fund["units"],
                    fund["purchase_nav"], fund["purchase_date"], current_nav,
                    invested, current_val, pl, pl_pct, xi][col]
        return sorted(funds, key=sort_key, reverse=not self._sort_asc)

    def _update_header_indicators(self):
        cols = ["Fund Name","Scheme Code","Units","Buy NAV (₹)","Buy Date",
                "Current NAV (₹)","Invested (₹)","Current (₹)","P&L (₹)","P&L %","XIRR %"]
        for i, name in enumerate(cols):
            arrow = (" ▲" if self._sort_asc else " ▼") if i == self._sort_col else ""
            self.table.horizontalHeaderItem(i).setText(name + arrow)

    def _on_header_clicked(self, col):
        if self._sort_col == col:
            self._sort_asc = not self._sort_asc
        else:
            self._sort_col = col
            self._sort_asc = True
        self._refresh_table()
        self.table.clearSelection()
        self._update_cards_portfolio()

    def _on_table_selection_changed(self):
        rows = self.table.selectionModel().selectedRows()
        if rows:
            sorted_funds = self._apply_sort(self.portfolio)
            idx = rows[0].row()
            if 0 <= idx < len(sorted_funds):
                self._update_cards_for_fund(sorted_funds[idx])
        else:
            self._update_cards_portfolio()

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
        self.table.setSortingEnabled(False)
        self.table.doubleClicked.connect(lambda: self._edit_fund())
        self.table.itemSelectionChanged.connect(self._on_table_selection_changed)
        # Sorting
        self._sort_col = None
        self._sort_asc = True
        hdr = self.table.horizontalHeader()
        hdr.setSectionsClickable(True)
        hdr.sectionClicked.connect(self._on_header_clicked)
        hdr.setStyleSheet(
            "QHeaderView::section { background: #161b22; color: #8b949e; padding: 8px;"
            "border: none; border-bottom: 1px solid #30363d; font-weight: bold; font-size: 12px; }"
            "QHeaderView::section:hover { background: #21262d; color: #e6edf3; }"
        )
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

        # Time range dropdown
        self._chart_years = None   # None = all time
        self.year_filter = QComboBox()
        self.year_filter.addItem("All Time", None)
        for y in range(1, 6):
            self.year_filter.addItem(f"{y} Year{'s' if y > 1 else ''}", y)
        self.year_filter.setFixedWidth(110)
        self.year_filter.currentIndexChanged.connect(self._on_year_filter_changed)

        self.fund_selector = QComboBox()
        self.fund_selector.setMinimumWidth(300)
        self.fund_selector.currentIndexChanged.connect(self._plot_selected)

        self.compare_hint = QLabel("All funds normalised to 100 at purchase — compare relative returns")
        self.compare_hint.setStyleSheet("color: #8b949e; font-size: 11px;")
        self.compare_hint.setVisible(False)

        chart_ctrl.addWidget(self.btn_single)
        chart_ctrl.addWidget(self.btn_compare)
        chart_ctrl.addSpacing(16)
        chart_ctrl.addWidget(self.year_filter)
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

        # Tab 3 – Portfolio Worth
        worth_tab = QWidget()
        worth_tab.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        worth_layout = QVBoxLayout(worth_tab)
        worth_layout.setContentsMargins(8, 8, 8, 8)
        worth_layout.setSpacing(8)

        worth_ctrl = QHBoxLayout()
        worth_ctrl.setSpacing(10)

        self._worth_years = None
        self.worth_year_filter = QComboBox()
        self.worth_year_filter.addItem("All Time", None)
        for y in range(1, 6):
            self.worth_year_filter.addItem(f"{y} Year{'s' if y > 1 else ''}", y)
        self.worth_year_filter.setFixedWidth(110)
        self.worth_year_filter.currentIndexChanged.connect(self._on_worth_year_changed)

        worth_ctrl.addWidget(self.worth_year_filter)
        worth_ctrl.addStretch()
        worth_layout.addLayout(worth_ctrl, stretch=0)

        self.worth_chart = NavChart()
        self.worth_chart.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.worth_chart.setMinimumHeight(200)
        worth_layout.addWidget(self.worth_chart, stretch=1)
        self.tabs.addTab(worth_tab, "💰  Portfolio Worth")

        # Tab 4 – Gain/Loss Calculator
        self.tabs.addTab(self._build_gainloss_tab(), "📊  Gain/Loss")

        # Tab 5 – CSV Format Help
        self.tabs.addTab(self._build_help_tab(), "❓  CSV Format")

    # ── Summary Cards ─────────────────────────────────────────────────────────
    def _make_card(self, label, value, color):
        # Simple horizontal layout: label and value on same line
        frame = QFrame()
        frame.setObjectName("card")
        frame.setMinimumHeight(36)
        h = QHBoxLayout(frame)
        h.setContentsMargins(12, 6, 12, 6)
        h.setSpacing(8)
        
        lbl = QLabel(f"{label}:")
        lbl.setStyleSheet("color: #8b949e; font-size: 13px; font-weight: 500;")
        
        val = QLabel(value)
        val.setFont(QFont("Segoe UI", 14, QFont.Bold))
        val.setStyleSheet(f"color: {color};")
        
        h.addWidget(lbl)
        h.addWidget(val)
        h.addStretch()
        
        # Return both — frame goes into layout, val_label is stored for direct updates
        return frame, val

    def _update_card(self, val_label, value, color="#e6edf3"):
        val_label.setText(value)
        val_label.setStyleSheet(f"color: {color}; font-size: 14px; font-weight: bold;")

    # ── Help Tab ──────────────────────────────────────────────────────────────
    def _build_gainloss_tab(self):
        """Build the Gain/Loss Calculator tab."""
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setContentsMargins(20, 16, 20, 16)
        layout.setSpacing(12)
        
        # Title
        title = QLabel("📊  Gain/Loss Calculator")
        title.setFont(QFont("Segoe UI", 14, QFont.Bold))
        title.setStyleSheet("color: #58a6ff;")
        layout.addWidget(title)
        
        # Description
        desc = QLabel("Calculate portfolio gain/loss between two dates based on NAV values and fund quantities.")
        desc.setStyleSheet("color: #8b949e; font-size: 12px;")
        desc.setWordWrap(True)
        layout.addWidget(desc)
        
        # Date selection form - horizontal layout
        form_widget = QWidget()
        form_widget.setStyleSheet("background: #161b22; border-radius: 8px; padding: 16px;")
        form_layout = QHBoxLayout(form_widget)
        form_layout.setSpacing(20)
        form_layout.setContentsMargins(16, 16, 16, 16)
        
        # Start Date section
        start_container = QWidget()
        start_layout = QVBoxLayout(start_container)
        start_layout.setSpacing(6)
        start_layout.setContentsMargins(0, 0, 0, 0)
        start_label = QLabel("<b style='color:#e6edf3; font-size: 13px;'>Start Date:</b>")
        self.gl_start_date = QDateEdit()
        self.gl_start_date.setCalendarPopup(True)
        self.gl_start_date.setDisplayFormat("yyyy-MM-dd")
        self.gl_start_date.setDate(QDate.currentDate().addYears(-1))
        self.gl_start_date.setMinimumWidth(180)
        self.gl_start_date.setMinimumHeight(36)
        self.gl_start_date.setStyleSheet("padding: 8px 12px; font-size: 14px;")
        start_layout.addWidget(start_label)
        start_layout.addWidget(self.gl_start_date)
        
        # End Date section
        end_container = QWidget()
        end_layout = QVBoxLayout(end_container)
        end_layout.setSpacing(6)
        end_layout.setContentsMargins(0, 0, 0, 0)
        end_label = QLabel("<b style='color:#e6edf3; font-size: 13px;'>End Date:</b>")
        self.gl_end_date = QDateEdit()
        self.gl_end_date.setCalendarPopup(True)
        self.gl_end_date.setDisplayFormat("yyyy-MM-dd")
        self.gl_end_date.setDate(QDate.currentDate())
        self.gl_end_date.setMinimumWidth(180)
        self.gl_end_date.setMinimumHeight(36)
        self.gl_end_date.setStyleSheet("padding: 8px 12px; font-size: 14px;")
        end_layout.addWidget(end_label)
        end_layout.addWidget(self.gl_end_date)
        
        # Calculate button - aligned to bottom
        btn_container = QWidget()
        btn_layout = QVBoxLayout(btn_container)
        btn_layout.setContentsMargins(0, 0, 0, 0)
        btn_layout.addStretch()
        btn_calculate = QPushButton("🧮  Calculate Gain/Loss")
        btn_calculate.setObjectName("primary")
        btn_calculate.setMinimumWidth(200)
        btn_calculate.setMinimumHeight(36)
        btn_calculate.setStyleSheet("font-size: 14px; padding: 8px 20px;")
        btn_calculate.clicked.connect(self._calculate_gainloss)
        btn_layout.addWidget(btn_calculate)
        
        form_layout.addWidget(start_container)
        form_layout.addWidget(end_container)
        form_layout.addWidget(btn_container)
        form_layout.addStretch()
        
        layout.addWidget(form_widget)
        
        # Results section - expandable
        results_widget = QWidget()
        results_widget.setStyleSheet("background: #161b22; border-radius: 8px; padding: 16px;")
        results_layout = QVBoxLayout(results_widget)
        results_layout.setSpacing(12)
        results_layout.setContentsMargins(12, 12, 12, 12)
        
        # Summary cards - more compact (removed Results title to save space)
        cards_layout = QHBoxLayout()
        cards_layout.setSpacing(10)
        
        self.gl_card_start, self.gl_lbl_start = self._make_card("Start Value", "—", "#e6edf3")
        self.gl_card_end, self.gl_lbl_end = self._make_card("End Value", "—", "#e6edf3")
        self.gl_card_change, self.gl_lbl_change = self._make_card("Gain/Loss", "—", "#e6edf3")
        self.gl_card_pct, self.gl_lbl_pct = self._make_card("Change %", "—", "#e6edf3")
        
        for c in [self.gl_card_start, self.gl_card_end, self.gl_card_change, self.gl_card_pct]:
            c.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            c.setMinimumHeight(36)
            cards_layout.addWidget(c)
        
        results_layout.addLayout(cards_layout)
        
        # Detailed table - expandable to fill remaining space
        #table_label = QLabel("<b style='color:#e6edf3; font-size: 11px;'>Detailed Breakdown:</b>")
        #results_layout.addWidget(table_label)
        
        self.gl_table = QTableWidget()
        self.gl_table.setColumnCount(7)
        self.gl_table.setHorizontalHeaderLabels([
            "Fund Name", "Units", "Start NAV (₹)", "End NAV (₹)",
            "Start Value (₹)", "End Value (₹)", "Gain/Loss (₹)"
        ])
        
        # Configure table headers and sorting - make headers very visible
        header = self.gl_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        for i in range(1, 7):
            header.setSectionResizeMode(i, QHeaderView.ResizeToContents)
        header.setVisible(True)
        header.setStretchLastSection(False)
        header.setMinimumSectionSize(100)
        header.setDefaultSectionSize(150)
        
        # Apply header styling with proper text display
        self.gl_table.setStyleSheet(
            self.gl_table.styleSheet() +
            """
            QHeaderView::section {
                background-color: #1f6feb;
                color: #ffffff;
                padding: 12px 10px;
                border: none;
                border-right: 1px solid #0d1117;
                border-bottom: 3px solid #58a6ff;
                font-weight: bold;
                font-size: 13px;
                min-height: 40px;
                max-height: 40px;
            }
            QHeaderView::section:hover {
                background-color: #388bfd;
            }
            """
        )
        
        # Enable sorting
        self.gl_table.setSortingEnabled(True)
        
        # Configure table behavior
        self.gl_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.gl_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.gl_table.setAlternatingRowColors(True)
        self.gl_table.verticalHeader().setVisible(False)
        
        # Table styling
        self.gl_table.setStyleSheet(
            "QTableWidget { "
            "alternate-background-color: #161b22; "
            "background-color: #0d1117; "
            "gridline-color: #30363d; "
            "color: #e6edf3; "
            "}"
            "QTableWidget::item { padding: 8px; }"
            "QTableWidget::item:selected { background-color: #1f6feb; }"
        )
        
        # Remove minimum height constraint and let it expand
        self.gl_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        
        results_layout.addWidget(self.gl_table, 1)  # stretch factor of 1 to expand
        layout.addWidget(results_widget, 1)  # stretch factor of 1 to expand
        
        return w
    
    def _calculate_gainloss(self):
        """Calculate gain/loss for the selected date range."""
        if not self.portfolio:
            QMessageBox.information(self, "Empty Portfolio", "No funds in portfolio to calculate.")
            return
        
        start_date = self.gl_start_date.date().toString("yyyy-MM-dd")
        end_date = self.gl_end_date.date().toString("yyyy-MM-dd")
        
        if start_date >= end_date:
            QMessageBox.warning(self, "Invalid Range", "End date must be after start date.")
            return
        
        # Calculate for each fund
        results = []
        total_start_value = 0.0
        total_end_value = 0.0
        
        for fund in self.portfolio:
            history = fund.get("nav_history", [])
            if not history:
                continue
            
            # Sort history by date
            sorted_hist = sorted(history, key=lambda h: h["date"])
            dates = [h["date"] for h in sorted_hist]
            navs = [h["nav"] for h in sorted_hist]
            
            # Find NAV for start date (use nearest available)
            start_nav = None
            for i, d in enumerate(dates):
                if d >= start_date:
                    start_nav = navs[i]
                    break
            if start_nav is None and dates:
                # Use last available if all dates are before start_date
                start_nav = navs[-1]
            
            # Find NAV for end date (use nearest available)
            end_nav = None
            for i in range(len(dates) - 1, -1, -1):
                if dates[i] <= end_date:
                    end_nav = navs[i]
                    break
            if end_nav is None and dates:
                # Use first available if all dates are after end_date
                end_nav = navs[0]
            
            if start_nav is not None and end_nav is not None:
                units = fund["units"]
                start_value = units * start_nav
                end_value = units * end_nav
                gain_loss = end_value - start_value
                
                results.append({
                    "name": fund["name"],
                    "units": units,
                    "start_nav": start_nav,
                    "end_nav": end_nav,
                    "start_value": start_value,
                    "end_value": end_value,
                    "gain_loss": gain_loss
                })
                
                total_start_value += start_value
                total_end_value += end_value
        
        if not results:
            QMessageBox.information(self, "No Data",
                "No NAV data available for the selected date range.\n"
                "Please refresh NAV data first.")
            return
        
        # Update summary cards
        total_gain_loss = total_end_value - total_start_value
        pct_change = (total_gain_loss / total_start_value * 100) if total_start_value > 0 else 0.0
        
        self._update_card(self.gl_lbl_start, f"₹{total_start_value:,.2f}", "#e6edf3")
        self._update_card(self.gl_lbl_end, f"₹{total_end_value:,.2f}", "#e6edf3")
        
        gain_color = "#3fb950" if total_gain_loss >= 0 else "#f85149"
        self._update_card(self.gl_lbl_change,
            f"{'₹' if total_gain_loss >= 0 else '-₹'}{abs(total_gain_loss):,.2f}",
            gain_color)
        self._update_card(self.gl_lbl_pct,
            f"{'+' if pct_change >= 0 else ''}{pct_change:.2f}%",
            gain_color)
        
        # Update table with proper alignment and sorting support
        self.gl_table.setSortingEnabled(False)  # Disable sorting while populating
        self.gl_table.setRowCount(len(results))
        
        for row, r in enumerate(results):
            # Fund name - left aligned (text sorting)
            name_item = QTableWidgetItem(r["name"])
            name_item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
            self.gl_table.setItem(row, 0, name_item)
            
            # Units - right aligned (numeric sorting)
            units_item = NumericTableWidgetItem(f"{r['units']:.4f}", r['units'])
            units_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.gl_table.setItem(row, 1, units_item)
            
            # Start NAV - right aligned (numeric sorting)
            start_nav_item = NumericTableWidgetItem(f"₹{r['start_nav']:.2f}", r['start_nav'])
            start_nav_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.gl_table.setItem(row, 2, start_nav_item)
            
            # End NAV - right aligned (numeric sorting)
            end_nav_item = NumericTableWidgetItem(f"₹{r['end_nav']:.2f}", r['end_nav'])
            end_nav_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.gl_table.setItem(row, 3, end_nav_item)
            
            # Start Value - right aligned (numeric sorting)
            start_val_item = NumericTableWidgetItem(f"₹{r['start_value']:,.2f}", r['start_value'])
            start_val_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.gl_table.setItem(row, 4, start_val_item)
            
            # End Value - right aligned (numeric sorting)
            end_val_item = NumericTableWidgetItem(f"₹{r['end_value']:,.2f}", r['end_value'])
            end_val_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.gl_table.setItem(row, 5, end_val_item)
            
            # Gain/Loss - right aligned with color (numeric sorting)
            gl_item = NumericTableWidgetItem(
                f"{'₹' if r['gain_loss'] >= 0 else '-₹'}{abs(r['gain_loss']):,.2f}",
                r['gain_loss']
            )
            gl_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            gl_color = "#3fb950" if r['gain_loss'] >= 0 else "#f85149"
            gl_item.setForeground(QColor(gl_color))
            self.gl_table.setItem(row, 6, gl_item)
        
        self.gl_table.setSortingEnabled(True)  # Re-enable sorting after populating
        
        self._set_status(f"Gain/Loss calculated for {len(results)} fund(s) ✓")

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
        table_row = rows[0].row()
        # Get the sorted funds to map table row back to portfolio index
        sorted_funds = self._apply_sort(self.portfolio)
        selected_fund = sorted_funds[table_row]
        # Find the index of this fund in the original portfolio
        portfolio_idx = None
        for i, fund in enumerate(self.portfolio):
            if fund is selected_fund:
                portfolio_idx = i
                break
        if portfolio_idx is None:
            QMessageBox.warning(self, "Error", "Could not find selected fund in portfolio.")
            return
        existing = self.portfolio[portfolio_idx]
        dlg = AddFundDialog(self, fund=existing)
        if dlg.exec_() == QDialog.Accepted:
            updated = dlg.result_fund
            refetch = len(updated["nav_history"]) == 0 and existing.get("nav_history")
            self.portfolio[portfolio_idx] = updated
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
        table_row = rows[0].row()
        # Get the sorted funds to map table row back to portfolio index
        sorted_funds = self._apply_sort(self.portfolio)
        selected_fund = sorted_funds[table_row]
        # Find the index of this fund in the original portfolio
        portfolio_idx = None
        for i, fund in enumerate(self.portfolio):
            if fund is selected_fund:
                portfolio_idx = i
                break
        if portfolio_idx is None:
            QMessageBox.warning(self, "Error", "Could not find selected fund in portfolio.")
            return
        name = self.portfolio[portfolio_idx]["name"]
        if QMessageBox.question(self, "Remove", f"Remove {name}?",
                                QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            self.portfolio.pop(portfolio_idx)
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
        self._refresh_table()
        msg = f"Imported {count} fund(s)."
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

        # One fetch per unique scheme_code, using the earliest purchase date so the
        # history covers all SIP entries of the same fund.
        unique: dict = {}
        for fund in self.portfolio:
            code = fund["scheme_code"]
            date = fund["purchase_date"]
            if code not in unique or date < unique[code]:
                unique[code] = date

        self._pending = len(unique)
        for code, from_date in unique.items():
            worker = NavFetcher(code, from_date)
            worker.done.connect(self._on_nav_fetched)
            worker.start()
            self._workers = getattr(self, "_workers", [])
            self._workers.append(worker)

    def _on_nav_fetched(self, code, history, reason):
        fund_name = next((f["name"] for f in self.portfolio if f["scheme_code"] == code), code)
        matched = [f for f in self.portfolio if f["scheme_code"] == code]
        if history:
            for fund in matched:
                fund["nav_history"] = history
        else:
            # Track one failure per scheme_code (not once per SIP entry)
            self._nav_failures = getattr(self, "_nav_failures", [])
            self._nav_failures.append((fund_name, code, reason))

        self._pending = getattr(self, "_pending", 1) - 1
        if self._pending <= 0:
            save_portfolio(self.portfolio, self.current_profile)
            touch_last_refreshed(self.current_profile)
            self._refresh_table()

            # Show failure summary if any funds had issues
            failures = getattr(self, "_nav_failures", [])
            self._nav_failures = []   # reset for next refresh
            if failures:
                lines = []
                for fname, fcode, r in failures:
                    if r == "not_found":
                        reason_txt = "Scheme code not found on MFAPI"
                    elif r == "no_data":
                        reason_txt = "No NAV data available (closed/old fund)"
                    elif r and r.startswith("date_filter"):
                        latest = r.split("|")[1] if "|" in r else "unknown date"
                        reason_txt = f"All NAV data predates purchase date (latest on MFAPI: {latest})"
                    elif r == "network":
                        reason_txt = "Network error — could not reach MFAPI"
                    else:
                        reason_txt = "Unknown error"
                    lines.append(f"• {fname} ({fcode})\n  → {reason_txt}")

                ok_count = len(self.portfolio) - len(failures)
                msg = (f"NAV updated for {ok_count} fund(s).\n\n"
                       f"⚠️  {len(failures)} fund(s) could not be updated:\n\n" +
                       "\n\n".join(lines))
                QMessageBox.warning(self, "NAV Refresh — Partial Results", msg)
                self._set_status(f"NAV updated — {len(failures)} fund(s) had issues ⚠️")
            else:
                self._set_status("NAV data updated ✓")

            self._populate_fund_selector()

    # ── Table refresh ─────────────────────────────────────────────────────────
    def _refresh_table(self):
        self.table.setRowCount(0)

        for fund in self._apply_sort(self.portfolio):
            row = self.table.rowCount()
            self.table.insertRow(row)

            history = fund.get("nav_history", [])
            current_nav = history[-1]["nav"] if history else fund["purchase_nav"]
            invested = fund["units"] * fund["purchase_nav"]
            current_val = fund["units"] * current_nav
            pl = current_val - invested
            pl_pct = (pl / invested * 100) if invested else 0
            xi = xirr([(fund["purchase_date"], -invested),
                        (datetime.date.today().isoformat(), current_val)])

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

        self._update_header_indicators()
        self._update_cards_portfolio()
        self._populate_fund_selector()

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
        self.chart.plot_compare(self.portfolio, years=self._chart_years)
        self._update_cards_portfolio()

    def _on_year_filter_changed(self, index):
        self._chart_years = self.year_filter.itemData(index)
        self._replot_current()

    def _on_worth_year_changed(self, index):
        self._worth_years = self.worth_year_filter.itemData(index)
        self._plot_worth()

    def _plot_worth(self):
        self.worth_chart.plot_worth(self.portfolio, years=self._worth_years)

    def _replot_current(self):
        if self.btn_compare.isChecked():
            self.chart.plot_compare(self.portfolio, years=self._chart_years)
        else:
            self._plot_selected(self.fund_selector.currentIndex())

    def _populate_fund_selector(self):
        self.fund_selector.blockSignals(True)
        # Remember current selection by scheme_code so we can restore it
        prev_code = self.fund_selector.currentData()
        self.fund_selector.clear()

        # One entry per unique scheme_code; label shows SIP count when > 1
        seen = []
        for fund in self.portfolio:
            code = fund["scheme_code"]
            if code not in seen:
                seen.append(code)
        for code in seen:
            entries = [f for f in self.portfolio if f["scheme_code"] == code]
            name    = entries[0]["name"]
            label   = f"{name}  ({len(entries)} SIPs)" if len(entries) > 1 else name
            self.fund_selector.addItem(label, code)   # store scheme_code as item data

        # Restore previous selection (by code), else default to first item
        restore_idx = 0
        if prev_code:
            for i in range(self.fund_selector.count()):
                if self.fund_selector.itemData(i) == prev_code:
                    restore_idx = i
                    break
        self.fund_selector.setCurrentIndex(restore_idx)
        self.fund_selector.blockSignals(False)

        if self.btn_compare.isChecked():
            self.chart.plot_compare(self.portfolio, years=self._chart_years)
        else:
            self._plot_selected(self.fund_selector.currentIndex())
        self._plot_worth()

    def _plot_selected(self, idx):
        if idx < 0 or self.fund_selector.count() == 0:
            return
        code = self.fund_selector.itemData(idx)
        if not code:
            return
        entries = [f for f in self.portfolio if f["scheme_code"] == code]
        if not entries:
            return
        self.chart.plot(entries, years=self._chart_years)
        if len(entries) == 1:
            self._update_cards_for_fund(entries[0])
        else:
            self._update_cards_for_consolidated(entries)

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