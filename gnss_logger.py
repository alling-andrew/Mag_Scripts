#!/usr/bin/env python3

"""
gnss_logger.py

- Reads UBX-NAV-PVT messages from u-blox GNSS over USB
- Logs GNSS data to CSV using UNIX epoch timestamps (time.time())
- Appends safely (does NOT overwrite on restart)
- Designed for offline merge with high-rate sensor logs (e.g. magnetometer)

Requires:
  pip install pyserial pyubx2
"""

import csv
import time
from pathlib import Path

import serial
from pyubx2 import UBXReader, UBX_PROTOCOL
from datetime import datetime, timezone

# -------------------- Config --------------------
PORT = "/dev/ttyACM0"     # USB GNSS device
BAUDRATE = 115200         # Ignored for USB, required by pyserial
READ_TIMEOUT_S = 1.0

LOG_DIR = Path.home() / "Documents/Mag_Scripts"
CSV_NAME = "gnss_log_experiments.csv"   # fixed name → append-safe


# -------------------- Main --------------------
def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = LOG_DIR / CSV_NAME

    file_exists = csv_path.exists()

    print(f"[INFO] Logging GNSS to: {csv_path}")
    print("[INFO] Press Ctrl+C to stop.\n")

    ser = serial.Serial(
        port=PORT,
        baudrate=BAUDRATE,
        timeout=READ_TIMEOUT_S
    )

    ubr = UBXReader(
        ser,
        protfilter=UBX_PROTOCOL
    )

    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # Write header only if file is new
        if not file_exists:
            writer.writerow([
                "timestamp_unix",
                "lat_deg",
                "lon_deg",
                "height_m",
                "fixType",
                "numSV",
                "hAcc_m",
                "vAcc_m",
                "carrSoln"
            ])
            f.flush()

        try:
            while True:
                raw, msg = ubr.read()
                if msg is None:
                    continue

                if msg.identity == "NAV-PVT":
                    ts = time.time()   # shared clock with magnetometer

                    writer.writerow([
                        ts,
                        msg.lat,                     # degrees * 1e-7 already scaled
                        msg.lon,
                        msg.height / 1000.0,         # mm → m
                        msg.fixType,
                        msg.numSV,
                        msg.hAcc / 1000.0,           # mm → m
                        msg.vAcc / 1000.0,           # mm → m
                        msg.carrSoln                 # 0=None, 1=Float, 2=Fixed
                    ])

                    f.flush()  # GNSS is low-rate → flushing is cheap

                    print(
                        f"[GNSS] t={ts:.3f} "
                        f"lat={msg.lat:.7f} lon={msg.lon:.7f} "
                        f"fix={msg.fixType} sv={msg.numSV}"
                    )

        except KeyboardInterrupt:
            print("\n[INFO] GNSS logging stopped.")

        finally:
            ser.close()


if __name__ == "__main__":
    main()

