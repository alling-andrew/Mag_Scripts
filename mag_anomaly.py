import pandas as pd
import numpy as np
from scipy.signal import butter, filtfilt
from scipy.interpolate import griddata
from scipy.ndimage import maximum_filter, minimum_filter, label
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import math
import sys
import os

# ----------------------------
# Config
# ----------------------------
FILES = ["combined_log_1.csv", "combined_log_2.csv"]

GRID_RES       = 300
FS             = 10
CUTOFF_HZ      = 0.5
FILTER_ORDER   = 3
ANOMALY_SIGMA  = 2.5
NEIGHBORHOOD   = 15
CONTOUR_LEVELS = 30
FIGSIZE        = (14, 10)
DPI            = 150

MIN_FIX_TYPE = 3
MIN_SV       = 4

CMAP = {"TMI": "RdBu_r", "Bx": "PuOr", "By": "PRGn", "Bz": "RdYlBu"}

# ----------------------------
# Load
# ----------------------------
def load(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)

    df = df[df["sensor_type"] == "4"]
    df = df[df["checksum_ok"]  == True]
    if "fixType" in df.columns:
        df = df[df["fixType"] >= MIN_FIX_TYPE]
    if "numSV" in df.columns:
        df = df[df["numSV"] >= MIN_SV]

    df = df.rename(columns={"lat": "Latitude", "lon": "Longitude", "height_m": "Altitude"})
    df["Timestamp"] = pd.to_datetime(df["timestamp_unix_gnss"], unit="s", utc=True)
    df["Bx"]  = pd.to_numeric(df["value_x"], errors="coerce")
    df["By"]  = pd.to_numeric(df["value_y"], errors="coerce")
    df["Bz"]  = pd.to_numeric(df["value_z"], errors="coerce")
    df["TMI"] = np.sqrt(df["Bx"]**2 + df["By"]**2 + df["Bz"]**2)

    df = df.dropna(subset=["Timestamp", "Latitude", "Longitude", "TMI", "Bx", "By", "Bz"])
    df = df.sort_values("Timestamp").reset_index(drop=True)

    return df[["Timestamp", "Latitude", "Longitude", "Altitude", "TMI", "Bx", "By", "Bz"]]

# ----------------------------
# Signal processing
# ----------------------------
def lowpass(data: np.ndarray) -> np.ndarray:
    b, a = butter(FILTER_ORDER, CUTOFF_HZ / (0.5 * FS), btype="low")
    return filtfilt(b, a, data)


def make_grid(x, y, z):
    xi = np.linspace(x.min(), x.max(), GRID_RES)
    yi = np.linspace(y.min(), y.max(), GRID_RES)
    Xi, Yi = np.meshgrid(xi, yi)
    Zi = griddata((x, y), z, (Xi, Yi), method="linear")
    return xi, yi, Xi, Yi, Zi


def find_anomalies(Zi):
    valid = np.where(np.isfinite(Zi), Zi, np.nan)
    mean, std = np.nanmean(valid), np.nanstd(valid)
    size = 2 * NEIGHBORHOOD + 1
    lmax = maximum_filter(np.nan_to_num(valid, nan=-np.inf), size=size)
    lmin = minimum_filter(np.nan_to_num(valid, nan=+np.inf), size=size)

    def blobs(mask, want_max):
        lbl, n = label(mask)
        pts = []
        for i in range(1, n + 1):
            ys, xs = np.where(lbl == i)
            idx = np.argmax(valid[ys, xs]) if want_max else np.argmin(valid[ys, xs])
            pts.append((ys[idx], xs[idx]))
        return pts

    return (blobs((valid == lmax) & (valid > mean + ANOMALY_SIGMA * std), True),
            blobs((valid == lmin) & (valid < mean - ANOMALY_SIGMA * std), False))

# ----------------------------
# Plotting
# ----------------------------
def plot_channel(ax, Xi, Yi, Zi, xi, yi, peaks, troughs,
                 x_raw, y_raw, title, cmap, cbar_label):
    vmax = np.nanpercentile(np.abs(Zi), 98) or 1
    norm = mcolors.TwoSlopeNorm(vmin=-vmax, vcenter=0, vmax=vmax)
    cf = ax.contourf(Xi, Yi, Zi, levels=CONTOUR_LEVELS, cmap=cmap, norm=norm)
    ax.contour(Xi, Yi, Zi, levels=CONTOUR_LEVELS, colors="k", linewidths=0.2, alpha=0.25)
    plt.colorbar(cf, ax=ax, pad=0.02, fraction=0.046).set_label(cbar_label, fontsize=9)
    ax.plot(x_raw, y_raw, "w-", lw=0.3, alpha=0.3)
    for r, c in peaks:
        ax.plot(xi[c], yi[r], "r^", ms=6, markeredgecolor="white", markeredgewidth=0.6, zorder=5)
        ax.annotate(f"+{Zi[r,c]:.0f}", (xi[c], yi[r]), xytext=(4,3),
                    textcoords="offset points", fontsize=6, color="darkred", fontweight="bold")
    for r, c in troughs:
        ax.plot(xi[c], yi[r], "bv", ms=6, markeredgecolor="white", markeredgewidth=0.6, zorder=5)
        ax.annotate(f"{Zi[r,c]:.0f}", (xi[c], yi[r]), xytext=(4,3),
                    textcoords="offset points", fontsize=6, color="darkblue", fontweight="bold")
    ax.set_title(title, fontsize=10)
    ax.set_xlabel("Longitude (°)", fontsize=8)
    ax.set_ylabel("Latitude (°)",  fontsize=8)
    ax.tick_params(labelsize=7)
    ax.set_aspect("equal", adjustable="box")

# ----------------------------
# Main
# ----------------------------
def process(filepath: str) -> pd.DataFrame:
    name = os.path.splitext(os.path.basename(filepath))[0]
    print(f"\n=== {filepath} ===")

    df = load(filepath)
    print(f"  {len(df):,} valid rows")

    x, y     = df["Longitude"].values, df["Latitude"].values
    channels = {ch: df[ch].values for ch in ("TMI", "Bx", "By", "Bz")}

    results = {}
    for ch, raw in channels.items():
        filt  = lowpass(raw)
        bg    = np.median(filt)
        anom  = filt - bg
        xi, yi, Xi, Yi, Zi = make_grid(x, y, anom)
        peaks, troughs      = find_anomalies(Zi)
        print(f"  [{ch}] bg={bg:.2f} nT  "
              f"range=[{anom.min():.2f}, {anom.max():.2f}] nT  "
              f"peaks={len(peaks)}  troughs={len(troughs)}")
        results[ch] = dict(xi=xi, yi=yi, Xi=Xi, Yi=Yi, Zi=Zi,
                           peaks=peaks, troughs=troughs, bg=bg)

    fig, axes = plt.subplots(2, 2, figsize=FIGSIZE, dpi=DPI)
    for idx, (ch, r) in enumerate(results.items()):
        plot_channel(
            axes[idx//2][idx%2], r["Xi"], r["Yi"], r["Zi"], r["xi"], r["yi"],
            r["peaks"], r["troughs"], x, y,
            title     = f"{ch}  (bg {r['bg']:.1f} nT)",
            cmap      = CMAP.get(ch, "RdBu_r"),
            cbar_label = f"{ch} anomaly (nT)",
        )

    fig.suptitle(f"{name}  ·  LPF {CUTOFF_HZ} Hz  ·  {ANOMALY_SIGMA}σ  ·  {len(df):,} pts",
                 fontsize=12)
    plt.tight_layout()
    out = f"{name}_anomaly_map.png"
    plt.savefig(out, dpi=DPI, bbox_inches="tight")
    print(f"  Saved: {out}")
    plt.show()

    # Anomaly table
    rows = []
    for ch, r in results.items():
        for rc in r["peaks"]:
            rows.append({"ch": ch, "type": "peak",
                         "lat": r["yi"][rc[0]], "lon": r["xi"][rc[1]],
                         "nT": round(r["Zi"][rc], 2)})
        for rc in r["troughs"]:
            rows.append({"ch": ch, "type": "trough",
                         "lat": r["yi"][rc[0]], "lon": r["xi"][rc[1]],
                         "nT": round(r["Zi"][rc], 2)})
    if rows:
        print(pd.DataFrame(rows).sort_values("nT", ascending=False).to_string(index=False))

    return df


if __name__ == "__main__":
    for f in (sys.argv[1:] or FILES):
        if os.path.exists(f):
            process(f)
        else:
            print(f"Not found: {f}")
