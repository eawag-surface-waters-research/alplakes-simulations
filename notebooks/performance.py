#!/usr/bin/env python3
"""
performance.py — score a wave-model run against wave-buoy observations.

Given a CSV of buoy wave data and a simulation directory (an existing model run),
this script:
  1. extracts the model time series at the grid cell nearest the buoy,
  2. computes error metrics (bias, RMSE, MAE, scatter index, correlation;
     circular statistics for direction),
  3. plots model vs. observed time series and scatter, saving a figure and a
     metrics CSV next to the input (or in --output).

The buoy CSV is expected in the same format as wave_buoy.csv:

    Time,Wave Height (m),Wave Period (s),Wave Direction (deg)
    2026-03-24 08:40:00,0.07,1.5,109.71
    ...

The CSV carries no position, so the buoy location is given by --lat/--lon
(defaults to the LeXPLORE platform on Lake Geneva — verify per deployment).

The simulation directory is a run folder under runs/ holding the SWAN output
NetCDF(s) (output_*.nc or output.nc) with HS / TM01 / PDIR on a lat/lon grid.

Examples
--------
    python performance.py ../wave_buoy.csv \
        ../runs/delftwaves_swanv4151_swan_geneva_20260324_20260329_1 \
        --lat 46.5000 --lon 6.6670
"""
import os
import glob
import argparse

import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt

# Default buoy position: LeXPLORE platform, Lake Geneva. Override with --lat/--lon.
DEFAULT_LAT, DEFAULT_LON = 46.5000, 6.6670

# Buoy CSV column -> (model output variable, axis label, is_circular)
VARIABLES = [
    ("Wave Height (m)",      "HS",   "Significant wave height H$_s$ (m)", False),
    ("Wave Period (s)",      "TM01", "Mean wave period T$_{m01}$ (s)",    False),
    ("Wave Direction (deg)", "PDIR", "Wave direction (deg)",              True),
]

# SWAN here runs `SET CARTESIAN`, so PDIR is the direction the waves travel TO,
# measured counter-clockwise from East (the positive x-axis). Buoys usually
# report the nautical "coming-from" direction (clockwise from North). These map
# as: nautical_from = (270 - cartesian_to) mod 360 — verified against the
# LeXPLORE record (drops the direction RMSE from ~117 deg to ~77 deg). Each entry
# converts the model's Cartesian PDIR into the named buoy convention.
DIR_CONVENTIONS = {
    "nautical-from": lambda c: (270.0 - c) % 360.0,   # clockwise from N, coming-from
    "nautical-to":   lambda c: (90.0 - c) % 360.0,    # clockwise from N, travelling-to
    "cartesian-to":  lambda c: c % 360.0,             # CCW from E, travelling-to (no change)
}


def read_buoy(csv_path):
    """Load the buoy CSV, parse timestamps, drop duplicates, sort by time."""
    df = pd.read_csv(csv_path)
    if "Time" not in df.columns:
        raise ValueError("Buoy CSV must have a 'Time' column; got {}".format(list(df.columns)))
    df["Time"] = pd.to_datetime(df["Time"])
    df = df.dropna(subset=["Time"]).drop_duplicates(subset=["Time"]).sort_values("Time")
    return df.set_index("Time")


def load_model_dataset(sim_dir):
    """Open and time-concatenate the SWAN output_*.nc files in a run directory."""
    files = sorted(glob.glob(os.path.join(sim_dir, "output_*.nc")))
    if not files:
        single = os.path.join(sim_dir, "output.nc")
        files = [single] if os.path.isfile(single) else []
    if not files:
        raise FileNotFoundError("No output NetCDF found in {}".format(sim_dir))
    parts = [xr.open_dataset(f) for f in files]
    ds = xr.concat(parts, dim="time") if len(parts) > 1 else parts[0]
    return ds.sortby("time")


def nearest_water_cell(ds, blat, blon, ref_var="HS"):
    """Index (eta, xi) of the lake cell nearest the buoy (cells dry for the whole run are skipped)."""
    lat = ds["lat"].values
    lon = ds["lon"].values
    wet = np.isfinite(ds[ref_var].values).any(axis=0)  # (eta, xi) cells active at some point
    if not wet.any():
        raise ValueError("Model output has no wet cells in {}".format(ref_var))
    dist = (lat - blat) ** 2 + (lon - blon) ** 2
    dist = np.where(wet, dist, np.inf)
    eta, xi = np.unravel_index(np.argmin(dist), dist.shape)
    return int(eta), int(xi), float(lat[eta, xi]), float(lon[eta, xi])


def model_series_at_cell(ds, eta, xi, dir_convention="nautical-from"):
    """DataFrame of the model variables at one grid cell, indexed by time.

    PDIR is converted from SWAN's Cartesian travelling-to convention into the
    buoy's convention (dir_convention) so directions are directly comparable.
    """
    times = pd.to_datetime(ds["time"].values)
    data = {mvar: ds[mvar].values[:, eta, xi] for _, mvar, _, _ in VARIABLES if mvar in ds}
    df = pd.DataFrame(data, index=times)
    if "PDIR" in df.columns:
        df["PDIR"] = DIR_CONVENTIONS[dir_convention](df["PDIR"])
    return df


def interp_to_times(src_index, src_values, target_index, circular=False):
    """Linearly interpolate a series onto target timestamps; circular via unit vectors."""
    xs = src_index.view("int64").astype(float)
    xt = target_index.view("int64").astype(float)
    sv = np.asarray(src_values, dtype=float)
    finite = np.isfinite(sv)
    if finite.sum() < 2:
        return np.full(len(xt), np.nan)
    xs, sv = xs[finite], sv[finite]
    if circular:
        rad = np.deg2rad(sv)
        s = np.interp(xt, xs, np.sin(rad), left=np.nan, right=np.nan)
        c = np.interp(xt, xs, np.cos(rad), left=np.nan, right=np.nan)
        return np.rad2deg(np.arctan2(s, c)) % 360.0
    return np.interp(xt, xs, sv, left=np.nan, right=np.nan)


def angular_diff(model, obs):
    """Signed smallest difference model-obs in degrees, wrapped to [-180, 180]."""
    return (np.asarray(model) - np.asarray(obs) + 180.0) % 360.0 - 180.0


def metrics(model, obs, circular=False):
    """Bias, RMSE, MAE, scatter index, correlation and sample count for a paired series."""
    model = np.asarray(model, dtype=float)
    obs = np.asarray(obs, dtype=float)
    mask = np.isfinite(model) & np.isfinite(obs)
    model, obs = model[mask], obs[mask]
    n = int(mask.sum())
    if n == 0:
        return {"N": 0, "bias": np.nan, "rmse": np.nan, "mae": np.nan, "si": np.nan, "r": np.nan}
    if circular:
        d = angular_diff(model, obs)
        bias = float(np.rad2deg(np.arctan2(np.mean(np.sin(np.deg2rad(d))),
                                           np.mean(np.cos(np.deg2rad(d))))))
        rmse = float(np.sqrt(np.mean(d ** 2)))
        mae = float(np.mean(np.abs(d)))
        si, r = np.nan, np.nan
    else:
        diff = model - obs
        bias = float(np.mean(diff))
        rmse = float(np.sqrt(np.mean(diff ** 2)))
        mae = float(np.mean(np.abs(diff)))
        si = float(rmse / np.mean(obs)) if np.mean(obs) != 0 else np.nan
        r = float(np.corrcoef(model, obs)[0, 1]) if n > 1 else np.nan
    return {"N": n, "bias": bias, "rmse": rmse, "mae": mae, "si": si, "r": r}


def compare(obs_df, model_df):
    """Pair model and observations on the model timestamps; return metrics + paired frame."""
    rows = []
    paired = pd.DataFrame(index=model_df.index)
    for obs_col, mvar, _, circular in VARIABLES:
        if obs_col not in obs_df.columns or mvar not in model_df.columns:
            continue
        obs_on_model = interp_to_times(obs_df.index.values, obs_df[obs_col].values,
                                       model_df.index.values, circular=circular)
        paired["{}_model".format(mvar)] = model_df[mvar].values
        paired["{}_obs".format(mvar)] = obs_on_model
        m = metrics(model_df[mvar].values, obs_on_model, circular=circular)
        m.update({"variable": mvar, "obs_column": obs_col, "circular": circular})
        rows.append(m)
    metrics_df = pd.DataFrame(rows).set_index("variable")
    return metrics_df, paired


def plot_performance(obs_df, model_df, paired, metrics_df, info, out_png):
    """Time-series (left) and scatter (right) for each variable; metrics annotated."""
    present = [(o, m, lbl, circ) for (o, m, lbl, circ) in VARIABLES
               if m in model_df.columns and o in obs_df.columns]
    n = len(present)
    fig, axes = plt.subplots(n, 2, figsize=(14, 3.4 * n),
                             gridspec_kw={"width_ratios": [2.4, 1]})
    if n == 1:
        axes = axes.reshape(1, 2)

    for row, (obs_col, mvar, label, circular) in enumerate(present):
        ts_ax, sc_ax = axes[row, 0], axes[row, 1]
        mo = paired["{}_model".format(mvar)]
        ob = paired["{}_obs".format(mvar)]

        # Time series: observations (raw) and model
        ts_ax.plot(obs_df.index, obs_df[obs_col], ".", ms=3, color="0.5",
                   alpha=0.6, label="buoy")
        ts_ax.plot(model_df.index, model_df[mvar], "-", lw=1.4, color="tab:blue",
                   label="model")
        ts_ax.set_ylabel(label)
        ts_ax.grid(alpha=0.3)
        if row == 0:
            ts_ax.legend(loc="upper right", fontsize=8)

        # Scatter: model vs observed (paired on model times)
        sc_ax.plot(ob, mo, ".", ms=4, color="tab:blue", alpha=0.6)
        finite = np.isfinite(mo) & np.isfinite(ob)
        if finite.any():
            lo = float(np.nanmin([ob[finite].min(), mo[finite].min()]))
            hi = float(np.nanmax([ob[finite].max(), mo[finite].max()]))
            sc_ax.plot([lo, hi], [lo, hi], "k--", lw=1, alpha=0.7)
            sc_ax.set_xlim(lo, hi)
            sc_ax.set_ylim(lo, hi)
        sc_ax.set_xlabel("observed")
        sc_ax.set_ylabel("model")
        sc_ax.set_aspect("equal", adjustable="box")
        sc_ax.grid(alpha=0.3)

        m = metrics_df.loc[mvar]
        unit = "deg" if circular else ""
        txt = "N={N:.0f}\nbias={bias:.3f}{u}\nRMSE={rmse:.3f}{u}\nMAE={mae:.3f}{u}".format(
            N=m["N"], bias=m["bias"], rmse=m["rmse"], mae=m["mae"], u=unit)
        if not circular:
            txt += "\nSI={si:.2f}  R={r:.2f}".format(si=m["si"], r=m["r"])
        sc_ax.text(0.04, 0.96, txt, transform=sc_ax.transAxes, va="top", ha="left",
                   fontsize=8, bbox=dict(boxstyle="round", fc="white", alpha=0.8))

    title = ("{label}  |  cell ({eta},{xi}) at {clat:.4f},{clon:.4f}  "
             "(buoy {blat:.4f},{blon:.4f}, {dkm:.2f} km)").format(**info)
    fig.suptitle(title, fontsize=11)
    fig.tight_layout(rect=[0, 0, 1, 0.98])
    fig.savefig(out_png, dpi=150)
    print("Saved figure: {}".format(out_png))


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dp = np.radians(lat2 - lat1)
    dl = np.radians(lon2 - lon1)
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return float(2 * r * np.arcsin(np.sqrt(a)))


def main():
    parser = argparse.ArgumentParser(description="Score a wave-model run against buoy data.")
    parser.add_argument("csv", help="Path to the buoy wave CSV (wave_buoy.csv format).")
    parser.add_argument("sim_dir", help="Path to the simulation run directory (holds output_*.nc).")
    parser.add_argument("--lat", type=float, default=DEFAULT_LAT, help="Buoy latitude (deg).")
    parser.add_argument("--lon", type=float, default=DEFAULT_LON, help="Buoy longitude (deg).")
    parser.add_argument("--dir-convention", choices=list(DIR_CONVENTIONS), default="nautical-from",
                        help="Direction convention the buoy reports; the model PDIR is converted "
                             "to match it (default: nautical-from = coming-from, CW from North).")
    parser.add_argument("--output", "-o", default=None,
                        help="Output directory for the figure and metrics CSV (default: CSV's folder).")
    args = parser.parse_args()

    if not os.path.isdir(args.sim_dir):
        raise FileNotFoundError("Simulation directory not found: {}".format(args.sim_dir))

    obs_df = read_buoy(args.csv)
    if obs_df.empty:
        raise ValueError("No rows in buoy CSV {}".format(args.csv))
    print("Buoy record: {} -> {}  ({} rows)".format(
        obs_df.index.min(), obs_df.index.max(), len(obs_df)))

    ds = load_model_dataset(args.sim_dir)
    label = ds.attrs.get("lake", os.path.basename(os.path.normpath(args.sim_dir)))
    eta, xi, clat, clon = nearest_water_cell(ds, args.lat, args.lon)
    dkm = haversine_km(args.lat, args.lon, clat, clon)
    print("Model span:  {} -> {}".format(
        pd.to_datetime(ds["time"].values[0]), pd.to_datetime(ds["time"].values[-1])))
    print("Nearest wet cell ({},{}) at {:.4f},{:.4f} — {:.2f} km from buoy".format(
        eta, xi, clat, clon, dkm))

    model_df = model_series_at_cell(ds, eta, xi, dir_convention=args.dir_convention)
    metrics_df, paired = compare(obs_df, model_df)
    print("\nModel PDIR converted from SWAN Cartesian (travelling-to) to buoy "
          "convention '{}'.".format(args.dir_convention))
    print("\nPerformance metrics:")
    print(metrics_df.to_string(float_format=lambda v: "{:.3f}".format(v)))

    # If a large direction bias remains, the buoy may use a different convention;
    # try --dir-convention nautical-to / cartesian-to.
    if "PDIR" in metrics_df.index and abs(metrics_df.loc["PDIR", "bias"]) > 45:
        print("\nNote: direction bias still exceeds 45 deg — the buoy may use a "
              "different convention; try --dir-convention nautical-to or cartesian-to.")

    out_dir = args.output or os.path.dirname(os.path.abspath(args.csv))
    os.makedirs(out_dir, exist_ok=True)
    tag = str(label).replace("/", "_")
    out_png = os.path.join(out_dir, "performance_{}.png".format(tag))
    out_csv = os.path.join(out_dir, "performance_{}.csv".format(tag))

    info = {"label": label, "eta": eta, "xi": xi, "clat": clat, "clon": clon,
            "blat": args.lat, "blon": args.lon, "dkm": dkm}
    plot_performance(obs_df, model_df, paired, metrics_df, info, out_png)
    metrics_df.to_csv(out_csv)
    print("Saved metrics: {}".format(out_csv))


if __name__ == "__main__":
    main()
