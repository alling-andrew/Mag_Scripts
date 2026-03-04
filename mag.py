#!/usr/bin/env python3

import csv
import calendar
import datetime as dt
import threading
import time
from collections import deque
from pathlib import Path

import serial
import matplotlib.pyplot as plt
from pyubx2 import UBXReader, UBX_PROTOCOL

# ================= CONFIG =================

GNSS_PORT = "/dev/ttyACM0"
SENSOR_PORT = "/dev/ttyUSB0"

BAUDRATE = 115200

READ_TIMEOUT_S = 0.1

LOG_DIR = Path.home() / "Documents/Mag_Scripts/Mag_Data"

PLOT_HZ = 10
PLOT_PERIOD = 1.0 / PLOT_HZ
MAX_POINTS = 350

FLUSH_EVERY = 200

# ================= GLOBAL GNSS STATE =================

GNSS_UNIX_TIME = None
GNSS_STATE = {}

GNSS_LOCK = threading.Lock()

# ================= GNSS THREAD =================

def gnss_thread():

    global GNSS_UNIX_TIME, GNSS_STATE

    ser = serial.Serial(GNSS_PORT, BAUDRATE, timeout=1)
    ubr = UBXReader(ser, protfilter=UBX_PROTOCOL)

    while True:

        _, msg = ubr.read()

        if msg and msg.identity == "NAV-PVT":

            if msg.validDate and msg.validTime and msg.fullyResolved:

                dt_utc = dt.datetime(
                    msg.year,
                    msg.month,
                    msg.day,
                    msg.hour,
                    msg.min,
                    msg.second,
                    tzinfo=dt.timezone.utc
                )

                epoch_sec = calendar.timegm(dt_utc.timetuple())
                unix_time = epoch_sec + msg.nano * 1e-9

                with GNSS_LOCK:
                    GNSS_UNIX_TIME = unix_time
                    GNSS_STATE.update({
                        "lat": msg.lat,
                        "lon": msg.lon,
                        "height": msg.height / 1000.0,
                        "fixType": msg.fixType,
                        "numSV": msg.numSV,
                        "hAcc": msg.hAcc / 1000.0,
                        "vAcc": msg.vAcc / 1000.0,
                        "carrSoln": msg.carrSoln
                    })


# ================= PSEND PARSER =================

def xor_checksum(payload):
    c = 0
    for ch in payload:
        c ^= ord(ch)
    return c


def parse_psend(line):

    line = line.strip()

    if not line.startswith("$PSEND"):
        return None

    recv_ts = None

    with GNSS_LOCK:
        recv_ts = GNSS_UNIX_TIME

    if recv_ts is None:
        return None

    if "*" not in line:
        return None

    body, chk_str = line.split("*", 1)
    payload = body[1:]

    try:
        given = int(chk_str[:2], 16)
        calc = xor_checksum(payload)
        checksum_ok = (given == calc)
    except:
        checksum_ok = False

    parts = payload.split(",")

    sensor_type = parts[2] if len(parts) > 2 else ""
    sensor_index = parts[3] if len(parts) > 3 else ""

    value_x = value_y = value_z = None
    value_total = []

    if sensor_type == "4":

        dirs = [p.split(";") for p in parts[4:]]

        if len(dirs) > 0 and len(dirs[0]) > 0:
            value_x = dirs[0][0]

        if len(dirs) > 1 and len(dirs[1]) > 0:
            value_y = dirs[1][0]

        if len(dirs) > 2 and len(dirs[2]) > 0:
            value_z = dirs[2][0]

    else:
        value_total = parts[4:]

    return dict(
        ts=recv_ts,
        checksum_ok=checksum_ok,
        sensor_type=sensor_type,
        sensor_index=sensor_index,
        value_total=value_total,
        value_x=value_x,
        value_y=value_y,
        value_z=value_z,
        raw=line
    )


# ================= MAIN =================

def main():

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_path = LOG_DIR / f"combined_log_{stamp}.csv"

    header = [
        "timestamp_unix_gnss",
        "checksum_ok",
        "sensor_type",
        "sensor_index",
        "values",
        "lat",
        "lon",
        "height_m",
        "fixType",
        "numSV",
        "hAcc_m",
        "vAcc_m",
        "carrSoln"
    ]

    print("[INFO] Starting GNSS thread...")
    threading.Thread(target=gnss_thread, daemon=True).start()

    print(f"[INFO] Logging → {csv_path}")

    sensor_ser = serial.Serial(
        SENSOR_PORT,
        BAUDRATE,
        timeout=READ_TIMEOUT_S
    )

    # Plot buffers
    idx_vec = deque(maxlen=MAX_POINTS)
    x_buf = deque(maxlen=MAX_POINTS)
    y_buf = deque(maxlen=MAX_POINTS)
    z_buf = deque(maxlen=MAX_POINTS)

    sample_vec = 0

    plt.ion()

    fig, ax = plt.subplots()
    line_x, = ax.plot([], [], label="X")
    line_y, = ax.plot([], [], label="Y")
    line_z, = ax.plot([], [], label="Z")

    ax.set_title("Live Magnetic Vector")
    ax.legend()

    last_plot = time.perf_counter()

    lines_ok = 0
    lines_bad = 0

    with csv_path.open("w", newline="") as f:

        writer = csv.writer(f)
        writer.writerow(header)

        try:

            while True:

                raw = sensor_ser.readline()

                if not raw:
                    plt.pause(0.001)
                    continue

                line = raw.decode(errors="ignore").strip()

                parsed = parse_psend(line)

                if not parsed:
                    continue

                if parsed["checksum_ok"]:
                    lines_ok += 1
                else:
                    lines_bad += 1

                with GNSS_LOCK:
                    gnss_snapshot = GNSS_STATE.copy()

                writer.writerow([
                    parsed["ts"],
                    parsed["checksum_ok"],
                    parsed["sensor_type"],
                    parsed["sensor_index"],
                    ";".join(parsed["value_total"]),
                    gnss_snapshot.get("lat"),
                    gnss_snapshot.get("lon"),
                    gnss_snapshot.get("height"),
                    gnss_snapshot.get("fixType"),
                    gnss_snapshot.get("numSV"),
                    gnss_snapshot.get("hAcc"),
                    gnss_snapshot.get("vAcc"),
                    gnss_snapshot.get("carrSoln"),
                ])

                if parsed["sensor_type"] == "4":

                    vx = float(parsed["value_x"]) if parsed["value_x"] else None
                    vy = float(parsed["value_y"]) if parsed["value_y"] else None
                    vz = float(parsed["value_z"]) if parsed["value_z"] else None

                    if vx is not None and vy is not None and vz is not None:

                        sample_vec += 1

                        idx_vec.append(sample_vec)
                        x_buf.append(vx)
                        y_buf.append(vy)
                        z_buf.append(vz)

                now = time.perf_counter()

                if now - last_plot >= PLOT_PERIOD:

                    last_plot = now

                    if len(idx_vec) > 2:

                        xs = list(idx_vec)

                        line_x.set_data(xs, list(x_buf))
                        line_y.set_data(xs, list(y_buf))
                        line_z.set_data(xs, list(z_buf))

                        ax.set_xlim(xs[0], xs[-1])

                        ymin = min(min(x_buf), min(y_buf), min(z_buf))
                        ymax = max(max(x_buf), max(y_buf), max(z_buf))

                        if ymin == ymax:
                            ymin -= 1
                            ymax += 1

                        ax.set_ylim(ymin, ymax)

                        fig.canvas.draw_idle()
                        fig.canvas.flush_events()

                    plt.pause(0.001)

                if (lines_ok + lines_bad) % FLUSH_EVERY == 0:
                    f.flush()

        except KeyboardInterrupt:
            print("\nStopping...")

        finally:
            sensor_ser.close()


if __name__ == "__main__":
    main()
