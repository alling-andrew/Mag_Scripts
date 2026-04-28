#!/usr/bin/env python3
"""
Magnetometer + GNSS logger
Writes: combined_log_<stamp>.csv

Columns
-------
timestamp_unix_gnss  – GNSS-derived UTC unix time (float, nanosecond precision)
checksum_ok          – NMEA XOR checksum passed (bool)
sensor_type          – PSEND field [2]
sensor_index         – PSEND field [3]
values               – raw payload fields [4:] joined with '|'
value_x / value_y / value_z – parsed from sensor_type == "4"
lat / lon            – decimal degrees (WGS-84)
height_m             – ellipsoidal height (metres)
fixType              – u-blox fix type (0=no fix, 3=3-D, 4=GNSS+DR …)
numSV                – satellites used
hAcc_m / vAcc_m      – horizontal / vertical accuracy estimate (metres)
carrSoln             – carrier-phase solution (0=none, 1=float, 2=fixed)
"""

import csv
import calendar
import datetime as dt
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path

import serial
import matplotlib.pyplot as plt
from pyubx2 import UBXReader, UBX_PROTOCOL
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box as rich_box

# ─────────────────────── CONFIG ───────────────────────

GNSS_PORT   = "/dev/ttyACM0"
SENSOR_PORT = "/dev/ttyUSB0"
BAUDRATE    = 115200

GNSS_TIMEOUT_S    = 1.0
SENSOR_TIMEOUT_S  = 0.5
RECONNECT_PAUSE_S = 2.0

LOG_DIR = Path.home() / "Documents/Mag_Scripts/Mag_Data"

# magnetic_anomaly.py is expected in the same directory as this script
ANOMALY_SCRIPT = Path(__file__).parent / "magnetic_anomaly.py"

# Matplotlib vector plot rate
PLOT_HZ     = 10
PLOT_PERIOD = 1.0 / PLOT_HZ
MAX_POINTS  = 350

# Rich status panel refresh rate
STATUS_HZ     = 2
STATUS_PERIOD = 1.0 / STATUS_HZ

# TMI sparkline width (characters)
SPARK_LEN = 40

# CSV flush interval (seconds)
FLUSH_INTERVAL_S = 5.0

# ─────────────────────── SHARED STATE ─────────────────

GNSS_LOCK  = threading.Lock()
GNSS_STATE: dict = {}

# Counters and live values — written by main loop only (GIL-safe for
# single int/float assignments), read by the status panel builder.
_status: dict = {
    "lines_ok":   0,
    "lines_bad":  0,
    "tmi_latest": None,
    "tmi_spark":  deque(maxlen=SPARK_LEN),
    "start_time": None,
    "csv_path":   "",
    "sensor_ok":  False,
}

# Small scrolling event log shown at the bottom of the panel
_log_ring: deque[str] = deque(maxlen=6)
_log_lock  = threading.Lock()


def _log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%H:%M:%S")
    with _log_lock:
        _log_ring.append(f"[dim]{ts}[/dim]  {msg}")


# ─────────────────────── GNSS THREAD ──────────────────

def gnss_thread() -> None:
    while True:
        try:
            with serial.Serial(GNSS_PORT, BAUDRATE, timeout=GNSS_TIMEOUT_S) as ser:
                ubr = UBXReader(ser, protfilter=UBX_PROTOCOL)
                _log("GNSS connected")
                while True:
                    _, msg = ubr.read()
                    if msg is None:
                        continue
                    if msg.identity != "NAV-PVT":
                        continue
                    if not (msg.validDate and msg.validTime and msg.fullyResolved):
                        continue

                    dt_utc    = dt.datetime(msg.year, msg.month, msg.day,
                                            msg.hour, msg.min, msg.second,
                                            tzinfo=dt.timezone.utc)
                    unix_time = calendar.timegm(dt_utc.timetuple()) + msg.nano * 1e-9

                    with GNSS_LOCK:
                        GNSS_STATE.update({
                            "unix_time": unix_time,
                            "lat":       msg.lat,
                            "lon":       msg.lon,
                            "height":    msg.height / 1000.0,
                            "fixType":   msg.fixType,
                            "numSV":     msg.numSV,
                            "hAcc":      msg.hAcc / 1000.0,
                            "vAcc":      msg.vAcc / 1000.0,
                            "carrSoln":  msg.carrSoln,
                        })

        except serial.SerialException as exc:
            with GNSS_LOCK:
                GNSS_STATE.clear()
            _log(f"[yellow]GNSS port error: {exc} — retrying[/yellow]")
        except Exception as exc:
            _log(f"[red]GNSS error: {exc}[/red]")

        time.sleep(RECONNECT_PAUSE_S)


# ─────────────────────── PSEND PARSER ─────────────────

def _xor_checksum(payload: str) -> int:
    c = 0
    for ch in payload:
        c ^= ord(ch)
    return c


def parse_psend(line: str) -> dict | None:
    line = line.strip()
    if not line.startswith("$PSEND"):
        return None
    if "*" not in line:
        return None

    with GNSS_LOCK:
        if not GNSS_STATE:
            return None
        gnss = GNSS_STATE.copy()

    body, chk_str = line.split("*", 1)
    payload = body[1:]

    try:
        checksum_ok = int(chk_str[:2], 16) == _xor_checksum(payload)
    except ValueError:
        checksum_ok = False

    parts        = payload.split(",")
    sensor_type  = parts[2] if len(parts) > 2 else ""
    sensor_index = parts[3] if len(parts) > 3 else ""

    value_x = value_y = value_z = None
    value_total: list[str] = []

    if sensor_type == "4":
        dirs    = [p.split(";") for p in parts[4:]]
        value_x = dirs[0][0] if len(dirs) > 0 and dirs[0] else None
        value_y = dirs[1][0] if len(dirs) > 1 and dirs[1] else None
        value_z = dirs[2][0] if len(dirs) > 2 and dirs[2] else None
    else:
        value_total = parts[4:]

    return dict(
        ts           = gnss["unix_time"],
        checksum_ok  = checksum_ok,
        sensor_type  = sensor_type,
        sensor_index = sensor_index,
        value_total  = value_total,
        value_x      = value_x,
        value_y      = value_y,
        value_z      = value_z,
        gnss         = gnss,
    )


# ─────────────────────── RICH STATUS PANEL ────────────

_FIX_LABEL  = {0: "no fix", 1: "dead reck.", 2: "2-D", 3: "3-D", 4: "GNSS+DR", 5: "time only"}
_CARR_LABEL = {0: "–", 1: "float", 2: "fixed ✓"}
_SPARK_CHARS = " ▁▂▃▄▅▆▇█"


def _sparkline(buf: deque) -> str:
    if len(buf) < 2:
        return "–"
    lo, hi = min(buf), max(buf)
    span = hi - lo or 1.0
    return "".join(_SPARK_CHARS[int((v - lo) / span * 8)] for v in buf)


def _build_panel() -> Panel:
    # ── GNSS table ──────────────────────────────────────
    with GNSS_LOCK:
        g = GNSS_STATE.copy()

    gt = Table(box=rich_box.SIMPLE, show_header=False, padding=(0, 1))
    gt.add_column(style="dim", min_width=10)
    gt.add_column()

    if not g:
        gt.add_row("Status", Text("waiting for NAV-PVT …", style="yellow"))
    else:
        fix  = g.get("fixType", 0)
        carr = g.get("carrSoln", 0)
        fix_col  = "green" if fix >= 3 else ("yellow" if fix > 0 else "red")
        carr_col = "cyan"  if carr == 2 else "white"

        ts = dt.datetime.utcfromtimestamp(g["unix_time"]).strftime("%H:%M:%S.%f")[:-3]
        gt.add_row("UTC",     ts)
        gt.add_row("Fix",     Text(_FIX_LABEL.get(fix, str(fix)), style=fix_col))
        gt.add_row("Carrier", Text(_CARR_LABEL.get(carr, str(carr)), style=carr_col))
        gt.add_row("Sats",    str(g.get("numSV", "–")))
        gt.add_row("Lat",     f"{g.get('lat', 0):.6f}°")
        gt.add_row("Lon",     f"{g.get('lon', 0):.6f}°")
        gt.add_row("Height",  f"{g.get('height', 0):.1f} m")
        gt.add_row("hAcc",    f"{g.get('hAcc', 0):.3f} m")
        gt.add_row("vAcc",    f"{g.get('vAcc', 0):.3f} m")

    # ── Sensor / TMI table ──────────────────────────────
    tmi    = _status["tmi_latest"]
    ok     = _status["lines_ok"]
    bad    = _status["lines_bad"]
    total  = ok + bad
    pct_bad = 100 * bad / total if total else 0.0
    start  = _status["start_time"] or time.perf_counter()
    elapsed = time.perf_counter() - start
    rate   = ok / elapsed if elapsed > 0 else 0.0

    st = Table(box=rich_box.SIMPLE, show_header=False, padding=(0, 1))
    st.add_column(style="dim", min_width=10)
    st.add_column()

    sen_txt = Text("● connected", style="green") if _status["sensor_ok"] \
              else Text("● waiting…",  style="yellow")
    st.add_row("Sensor",    sen_txt)
    st.add_row("TMI",       Text(f"{tmi:.2f} nT" if tmi is not None else "–",
                                 style="bright_white"))
    st.add_row("Spark",     Text(_sparkline(_status["tmi_spark"]), style="cyan"))
    st.add_row("Rows OK",   f"{ok:,}")
    st.add_row("Bad cksum", f"{bad:,}  ({pct_bad:.1f}%)")
    st.add_row("Rate",      f"{rate:.1f} rows/s")
    m, s   = divmod(int(elapsed), 60)
    h, m   = divmod(m, 60)
    st.add_row("Elapsed",   f"{h:02d}:{m:02d}:{s:02d}")
    st.add_row("Log",       Text(str(_status["csv_path"]), style="dim"))

    # ── Event log ───────────────────────────────────────
    with _log_lock:
        log_lines = list(_log_ring)
    log_text = Text.from_markup(
        "\n".join(log_lines) if log_lines else "[dim]no events yet[/dim]"
    )

    # ── Layout ──────────────────────────────────────────
    top = Layout()
    top.split_row(
        Layout(Panel(gt, title="[bold blue]GNSS / NAV-PVT[/bold blue]",
                     border_style="blue"), name="gnss"),
        Layout(Panel(st, title="[bold magenta]Magnetometer[/bold magenta]",
                     border_style="magenta"), name="sensor"),
    )
    body = Layout()
    body.split_column(
        Layout(top, name="top", ratio=5),
        Layout(Panel(log_text, title="[bold]Events[/bold]", border_style="dim"),
               name="log", ratio=2),
    )

    now_str = dt.datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
    return Panel(body,
                 title=f"[bold cyan]mag.py[/bold cyan]  [dim]{now_str}[/dim]",
                 border_style="cyan")


# ─────────────────────── POST-RUN PROMPT ──────────────

def _prompt_anomaly(csv_path: Path) -> None:
    """
    Called in the main thread after the read loop exits.
    Offers to run magnetic_anomaly.py on the just-written file.
    """
    console = Console()
    console.print()
    console.rule("[bold cyan]Session complete[/bold cyan]")
    console.print(f"  Rows logged : [bold]{_status['lines_ok']:,}[/bold]")
    console.print(f"  File        : [cyan]{csv_path}[/cyan]")
    console.print()

    if _status["lines_ok"] == 0:
        console.print("[yellow]No valid rows recorded — skipping anomaly processing.[/yellow]")
        return

    if not ANOMALY_SCRIPT.exists():
        console.print(
            f"[yellow]magnetic_anomaly.py not found at {ANOMALY_SCRIPT}[/yellow]\n"
            f"[dim]Run manually:  python magnetic_anomaly.py {csv_path}[/dim]"
        )
        return

    try:
        answer = console.input(
            "[bold]Run magnetic_anomaly.py on this file now? [Y/n] → [/bold]"
        ).strip().lower()
    except (EOFError, KeyboardInterrupt):
        answer = "n"

    if answer in ("", "y", "yes"):
        console.print(f"\n[green]Launching magnetic_anomaly.py …[/green]\n")
        subprocess.run(
            [sys.executable, str(ANOMALY_SCRIPT), str(csv_path)],
            check=False,
        )
    else:
        console.print("[dim]Skipped.  Run manually when ready:[/dim]")
        console.print(f"  python magnetic_anomaly.py {csv_path}")


# ─────────────────────── PLOT HELPER ──────────────────

def _update_plot(fig, ax, line_x, line_y, line_z,
                 idx_vec, x_buf, y_buf, z_buf) -> None:
    if len(idx_vec) < 2:
        plt.pause(0.001)
        return
    xs = list(idx_vec)
    line_x.set_data(xs, list(x_buf))
    line_y.set_data(xs, list(y_buf))
    line_z.set_data(xs, list(z_buf))
    ax.set_xlim(xs[0], xs[-1])
    ymin = min(min(x_buf), min(y_buf), min(z_buf))
    ymax = max(max(x_buf), max(y_buf), max(z_buf))
    if ymin == ymax:
        ymin -= 1; ymax += 1
    ax.set_ylim(ymin, ymax)
    fig.canvas.draw_idle()
    fig.canvas.flush_events()
    plt.pause(0.001)


# ─────────────────────── MAIN ─────────────────────────

CSV_HEADER = [
    "timestamp_unix_gnss", "checksum_ok", "sensor_type", "sensor_index",
    "values", "value_x", "value_y", "value_z",
    "lat", "lon", "height_m", "fixType", "numSV", "hAcc_m", "vAcc_m", "carrSoln",
]


def main() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp    = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = LOG_DIR / f"combined_log_{stamp}.csv"

    _status["start_time"] = time.perf_counter()
    _status["csv_path"]   = str(csv_path)

    threading.Thread(target=gnss_thread, daemon=True).start()

    # ── Matplotlib vector plot ───────────────────────────
    idx_vec: deque[int]   = deque(maxlen=MAX_POINTS)
    x_buf:   deque[float] = deque(maxlen=MAX_POINTS)
    y_buf:   deque[float] = deque(maxlen=MAX_POINTS)
    z_buf:   deque[float] = deque(maxlen=MAX_POINTS)
    sample_vec = 0

    plt.ion()
    fig, ax = plt.subplots()
    (line_x,) = ax.plot([], [], label="Bx")
    (line_y,) = ax.plot([], [], label="By")
    (line_z,) = ax.plot([], [], label="Bz")
    ax.set_title("Live Magnetic Vector")
    ax.set_ylabel("nT")
    ax.legend()

    last_plot  = time.perf_counter()
    last_flush = time.perf_counter()

    with csv_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)

        with Live(_build_panel(), refresh_per_second=STATUS_HZ,
                  screen=False, console=Console()) as live:

            last_status = time.perf_counter()

            while True:
                # ── Sensor connection ────────────────────
                try:
                    sensor_ser = serial.Serial(
                        SENSOR_PORT, BAUDRATE, timeout=SENSOR_TIMEOUT_S
                    )
                    _status["sensor_ok"] = True
                    _log("Sensor connected")
                except serial.SerialException as exc:
                    _status["sensor_ok"] = False
                    _log(f"[yellow]Sensor error: {exc} — retrying[/yellow]")
                    time.sleep(RECONNECT_PAUSE_S)
                    continue

                try:
                    while True:
                        try:
                            raw = sensor_ser.readline()
                        except serial.SerialException as exc:
                            _status["sensor_ok"] = False
                            _log(f"[yellow]Read error: {exc} — reconnecting[/yellow]")
                            break

                        now = time.perf_counter()

                        # Refresh status panel
                        if now - last_status >= STATUS_PERIOD:
                            live.update(_build_panel())
                            last_status = now

                        if not raw:
                            _update_plot(fig, ax, line_x, line_y, line_z,
                                         idx_vec, x_buf, y_buf, z_buf)
                            last_plot = now
                            continue

                        line   = raw.decode(errors="ignore").strip()
                        parsed = parse_psend(line)
                        if parsed is None:
                            continue

                        # Counters
                        if parsed["checksum_ok"]:
                            _status["lines_ok"] += 1
                        else:
                            _status["lines_bad"] += 1

                        # CSV write
                        g = parsed["gnss"]
                        writer.writerow([
                            parsed["ts"],
                            parsed["checksum_ok"],
                            parsed["sensor_type"],
                            parsed["sensor_index"],
                            "|".join(parsed["value_total"]),
                            parsed["value_x"],
                            parsed["value_y"],
                            parsed["value_z"],
                            g.get("lat"),  g.get("lon"),    g.get("height"),
                            g.get("fixType"), g.get("numSV"),
                            g.get("hAcc"), g.get("vAcc"),   g.get("carrSoln"),
                        ])

                        # TMI + plot buffers
                        if parsed["sensor_type"] == "4":
                            try:
                                vx = float(parsed["value_x"])
                                vy = float(parsed["value_y"])
                                vz = float(parsed["value_z"])
                            except (TypeError, ValueError):
                                pass
                            else:
                                tmi = (vx**2 + vy**2 + vz**2) ** 0.5
                                _status["tmi_latest"] = tmi
                                _status["tmi_spark"].append(tmi)
                                sample_vec += 1
                                idx_vec.append(sample_vec)
                                x_buf.append(vx)
                                y_buf.append(vy)
                                z_buf.append(vz)

                        # Rate-limited plot update
                        if now - last_plot >= PLOT_PERIOD:
                            _update_plot(fig, ax, line_x, line_y, line_z,
                                         idx_vec, x_buf, y_buf, z_buf)
                            last_plot = now

                        # Time-based flush
                        if now - last_flush >= FLUSH_INTERVAL_S:
                            f.flush()
                            last_flush = now

                except KeyboardInterrupt:
                    _log("Ctrl-C — shutting down")
                    live.update(_build_panel())
                    f.flush()
                    break

                finally:
                    _status["sensor_ok"] = False
                    sensor_ser.close()

    # Live context has exited — terminal is restored, safe to prompt
    _prompt_anomaly(csv_path)


if __name__ == "__main__":
    main()
