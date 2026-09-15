#!/usr/bin/env python3
"""
performance.py — score a wave-model run against wave-buoy observations.

Given buoy wave observations and a simulation directory (an existing model run),
this script:
  1. extracts the model time series at the grid cell nearest the buoy,
  2. computes error metrics (bias, RMSE, MAE, scatter index, correlation;
     circular statistics for direction),
  3. plots model vs. observed time series and scatter, saving a figure, a
     metrics CSV, and a merged timeseries CSV (the buoy record with the model
     values appended) next to the input (or in --output).

Observations can be given in either form:

  * a CSV in the wave_buoy.csv format (the "Wave Period" column is the buoy's
    peak period Tp):

        Time,Wave Height (m),Wave Period (s),Wave Direction (deg)
        2026-03-24 08:40:00,0.07,1.5,109.71

  * a LeXPLORE Level1 NetCDF file, a directory of them, or a glob
    (L1_WaveBuoy_v2_*.nc). These carry hs, tp, te, wd, mwd, hmax plus
    <var>_qual flags, which are applied along with physical bounds.

The LeXPLORE buoy is a NexSens CB-450 carrying a Seaview SVS-603HR wave sensor.
It reports the peak (dominant) period Tp, the energy period Te, the dominant
direction and the mean direction — so the SWAN counterparts are RTP, TMM10,
PDIR and DIR respectively. The sensor resolves periods of 1.5 s and longer
only, so period and direction pairs in calm conditions are excluded with
--min-hs (default 0.10 m).

The observations carry no position, so the buoy location is given by --lat/--lon
(defaults to the LeXPLORE platform on Lake Geneva — verify per deployment).

The simulation directory is a run folder under runs/ holding the SWAN output
NetCDF(s) (output_*.nc or output.nc) on a lat/lon grid. Variables the run did
not output are skipped.

Examples
--------
    python performance.py ~/git/lexplore/wave-buoy/data/Level1 \
        ../runs/delftwaves_swanv4151_swan_geneva_20260226_20260409_2

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

# Buoy gaps longer than this are not bridged when interpolating onto model times
# (the record is hourly or 20-min; outages run from days to months).
MAX_GAP = "3h"

# Buoy column -> (model output variable, axis label, is_circular). Pairings follow
# what the SVS-603HR reports (see module docstring). Entries whose model variable
# is missing from the run are skipped.
VARIABLES = [
    ("Wave Height (m)",           "HS",    "Significant wave height H$_s$ (m)", False),
    ("Peak Period (s)",           "RTP",   "Peak wave period T$_p$ (s)",        False),
    ("Energy Period (s)",         "TMM10", "Energy period T$_e$ (s)",           False),
    ("Wave Direction (deg)",      "PDIR",  "Peak wave direction (deg)",         True),
    ("Mean Wave Direction (deg)", "DIR",   "Mean wave direction (deg)",         True),
]

# Deliberately mismatched pairing (buoy Tp vs model mean period) kept as a
# diagnostic, enabled with --include-tm01.
TM01_DIAGNOSTIC = ("Peak Period (s)", "TM01", "Mean period T$_{m01}$ vs buoy T$_p$ (s)", False)

# Level1 NetCDF variable -> buoy column name, and physical bounds (from the
# wave-buoy repo's quality_assurance.json) applied on top of the *_qual flags.
LEVEL1_COLUMNS = {
    "hs":   "Wave Height (m)",
    "tp":   "Peak Period (s)",
    "te":   "Energy Period (s)",
    "wd":   "Wave Direction (deg)",
    "mwd":  "Mean Wave Direction (deg)",
    "hmax": "Hmax (m)",
}
LEVEL1_BOUNDS = {"hs": (0, 5), "tp": (0, 40), "te": (0, 40), "wd": (0, 360),
                 "mwd": (0, 360), "hmax": (0, 5)}

# Older CSV exports name the peak period column generically.
CSV_RENAMES = {"Wave Period (s)": "Peak Period (s)"}

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


def read_buoy(path):
    """Load buoy observations (CSV, or Level1 NetCDF file/dir/glob) indexed by time."""
    if path.lower().endswith(".csv"):
        df = read_buoy_csv(path)
    else:
        df = read_buoy_level1(path)
    df = df[~df.index.isna()]
    df = df[~df.index.duplicated(keep="first")].sort_index()
    df.index.name = "Time"
    return df


def read_buoy_csv(csv_path):
    """The wave_buoy.csv export: parse timestamps and rename legacy columns."""
    df = pd.read_csv(csv_path)
    if "Time" not in df.columns:
        raise ValueError("Buoy CSV must have a 'Time' column; got {}".format(list(df.columns)))
    df["Time"] = pd.to_datetime(df["Time"])
    return df.set_index("Time").rename(columns=CSV_RENAMES)


def read_buoy_level1(path):
    """LeXPLORE Level1 NetCDF(s): apply *_qual flags and physical bounds, rename columns."""
    if os.path.isdir(path):
        files = sorted(glob.glob(os.path.join(path, "L1_WaveBuoy_*.nc")))
    elif os.path.isfile(path):
        files = [path]
    else:
        files = sorted(glob.glob(path))
    if not files:
        raise FileNotFoundError("No Level1 NetCDF found at {}".format(path))
    parts = [xr.open_dataset(f) for f in files]
    ds = xr.concat(parts, dim="time") if len(parts) > 1 else parts[0]
    ds = ds.sortby("time")

    data = {}
    for var, col in LEVEL1_COLUMNS.items():
        if var not in ds:
            continue
        values = ds[var].values.astype(float)
        qual = "{}_qual".format(var)
        if qual in ds:
            values = np.where(ds[qual].values == 0, values, np.nan)
        lo, hi = LEVEL1_BOUNDS[var]
        values = np.where((values >= lo) & (values <= hi), values, np.nan)
        data[col] = values
    return pd.DataFrame(data, index=pd.to_datetime(ds["time"].values))


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


def model_series_at_cell(ds, eta, xi, variables, dir_convention="nautical-from"):
    """DataFrame of the model variables at one grid cell, indexed by time.

    Directions (PDIR, DIR) are converted from SWAN's Cartesian travelling-to
    convention into the buoy's convention (dir_convention) so they are directly
    comparable.
    """
    times = pd.to_datetime(ds["time"].values)
    data = {mvar: ds[mvar].values[:, eta, xi] for _, mvar, _, _ in variables if mvar in ds}
    df = pd.DataFrame(data, index=times)
    for _, mvar, _, circular in variables:
        if circular and mvar in df.columns:
            df[mvar] = DIR_CONVENTIONS[dir_convention](df[mvar])
    return df


def interp_to_times(src_index, src_values, target_index, circular=False, max_gap=MAX_GAP):
    """Linearly interpolate a series onto target timestamps; circular via unit vectors.

    Targets that fall inside a gap in the source longer than max_gap (a buoy
    outage, or the model span beyond the record) are returned as NaN rather
    than bridged by interpolation.
    """
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
        out = np.rad2deg(np.arctan2(s, c)) % 360.0
    else:
        out = np.interp(xt, xs, sv, left=np.nan, right=np.nan)
    if max_gap is not None:
        hi = np.clip(np.searchsorted(xs, xt), 1, len(xs) - 1)
        gap = xs[hi] - xs[hi - 1]
        out = np.where(gap > pd.Timedelta(max_gap).value, np.nan, out)
    return out


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


def present_variables(obs_df, model_df, variables):
    """The pairings both the observations and the model actually provide."""
    return [(o, m, lbl, circ) for (o, m, lbl, circ) in variables
            if m in model_df.columns and o in obs_df.columns]


def compare(obs_df, model_df, variables, min_hs=0.0):
    """Pair model and observations on the model timestamps; return metrics + paired frame.

    Period and direction pairs where the buoy Hs is below min_hs are excluded:
    the SVS-603 cannot resolve periods under 1.5 s and direction is undefined in
    calm water. Hs itself is always scored on every pair. N_total is the number
    of valid pairs before that filter, N the number scored.
    """
    rows = []
    paired = pd.DataFrame(index=model_df.index)
    hs_col = "Wave Height (m)"
    calm = np.zeros(len(model_df), dtype=bool)
    if min_hs > 0 and hs_col in obs_df.columns:
        hs_on_model = interp_to_times(obs_df.index.values, obs_df[hs_col].values,
                                      model_df.index.values)
        calm = np.isfinite(hs_on_model) & (hs_on_model < min_hs)
    for obs_col, mvar, _, circular in present_variables(obs_df, model_df, variables):
        obs_on_model = interp_to_times(obs_df.index.values, obs_df[obs_col].values,
                                       model_df.index.values, circular=circular)
        model_vals = model_df[mvar].values.astype(float)
        n_total = int((np.isfinite(model_vals) & np.isfinite(obs_on_model)).sum())
        if mvar != "HS":
            obs_on_model = np.where(calm, np.nan, obs_on_model)
        paired["{}_model".format(mvar)] = model_vals
        paired["{}_obs".format(mvar)] = obs_on_model
        m = metrics(model_vals, obs_on_model, circular=circular)
        m.update({"variable": mvar, "obs_column": obs_col, "circular": circular,
                  "N_total": n_total, "min_hs": min_hs if mvar != "HS" else 0.0})
        rows.append(m)
    if not rows:
        raise ValueError("No variable is present in both the observations and the model output")
    metrics_df = pd.DataFrame(rows).set_index("variable")
    return metrics_df, paired


# Meteorological seasons, labelled by the year the season falls in (DJF -> year of Jan/Feb).
SEASONS = {12: "DJF", 1: "DJF", 2: "DJF", 3: "MAM", 4: "MAM", 5: "MAM",
           6: "JJA", 7: "JJA", 8: "JJA", 9: "SON", 10: "SON", 11: "SON"}


def period_labels(index, split):
    """Group label per timestamp for --split month / season / year."""
    if split == "month":
        return index.strftime("%Y-%m")
    if split == "year":
        return index.strftime("%Y")
    if split == "season":
        year = index.year + (index.month == 12)
        return pd.Index(["{}-{}".format(y, SEASONS[m]) for y, m in zip(year, index.month)])
    raise ValueError("Unknown split {}".format(split))


def metrics_by_period(paired, variables, split):
    """Metrics per calendar period (from the paired frame compare() produced)."""
    labels = period_labels(paired.index, split)
    rows = []
    for label in pd.unique(labels):
        sel = labels == label
        for _, mvar, _, circular in variables:
            mcol, ocol = "{}_model".format(mvar), "{}_obs".format(mvar)
            if mcol not in paired.columns:
                continue
            m = metrics(paired[mcol].values[sel], paired[ocol].values[sel], circular=circular)
            m.update({"period": label, "variable": mvar})
            rows.append(m)
    return pd.DataFrame(rows).set_index(["period", "variable"])


def merge_model_onto_buoy(obs_df, model_df, variables):
    """The buoy record with the model values interpolated onto the buoy timestamps.

    One row per buoy timestamp (read_buoy already drops duplicates), buoy columns
    untouched, one "Model ..." column added per variable the model provides. Buoy
    times outside the model span get NaN — interp_to_times does not extrapolate.
    """
    merged = obs_df.copy()
    lookup = {obs_col: (mvar, circ) for obs_col, mvar, _, circ in variables}
    # Walk the buoy's own column order so the "Model ..." block mirrors it.
    for obs_col in list(merged.columns):
        mvar, circular = lookup.get(obs_col, (None, False))
        if mvar is None or mvar not in model_df.columns:
            continue
        merged["Model {}".format(obs_col)] = interp_to_times(
            model_df.index.values, model_df[mvar].values,
            merged.index.values, circular=circular)
    merged.index.name = "Time"
    if not merged.index.is_unique:
        raise ValueError("Buoy timestamps are not unique — merged export would have duplicate rows")
    return merged


def plot_performance(obs_df, model_df, paired, metrics_df, info, out_png, variables):
    """Time-series (left) and scatter (right) for each variable; metrics annotated."""
    present = present_variables(obs_df, model_df, variables)
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
        n_txt = "N={:.0f}".format(m["N"])
        if m["N"] != m["N_total"]:
            n_txt += " of {:.0f} (H$_s$$\\geq$ {:.2f} m)".format(m["N_total"], m["min_hs"])
        txt = "{n}\nbias={bias:.3f}{u}\nRMSE={rmse:.3f}{u}\nMAE={mae:.3f}{u}".format(
            n=n_txt, bias=m["bias"], rmse=m["rmse"], mae=m["mae"], u=unit)
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
    parser.add_argument("obs", help="Buoy observations: wave_buoy.csv-format CSV, or a Level1 "
                                    "NetCDF file / directory / glob (L1_WaveBuoy_v2_*.nc).")
    parser.add_argument("sim_dir", help="Path to the simulation run directory (holds output_*.nc).")
    parser.add_argument("--lat", type=float, default=DEFAULT_LAT, help="Buoy latitude (deg).")
    parser.add_argument("--lon", type=float, default=DEFAULT_LON, help="Buoy longitude (deg).")
    parser.add_argument("--dir-convention", choices=list(DIR_CONVENTIONS), default="nautical-from",
                        help="Direction convention the buoy reports; the model directions are "
                             "converted to match it (default: nautical-from = coming-from, CW from North).")
    parser.add_argument("--min-hs", type=float, default=0.10,
                        help="Exclude period/direction pairs where the buoy Hs is below this (m); "
                             "the SVS-603 resolves periods >= 1.5 s only. 0 disables (default 0.10).")
    parser.add_argument("--include-tm01", action="store_true",
                        help="Also score the model mean period TM01 against the buoy peak period "
                             "(a deliberately mismatched pairing, kept as a diagnostic).")
    parser.add_argument("--start", default=None,
                        help="Restrict the comparison to model times from this date (e.g. 2026-02-26).")
    parser.add_argument("--end", default=None,
                        help="Restrict the comparison to model times up to this date (inclusive).")
    parser.add_argument("--split", choices=["month", "season", "year"], default=None,
                        help="Also write metrics per calendar period (performance_<lake>_by_<split>.csv).")
    parser.add_argument("--output", "-o", default=None,
                        help="Output directory for the figure and CSVs (default: the observations' folder).")
    args = parser.parse_args()

    if not os.path.isdir(args.sim_dir):
        raise FileNotFoundError("Simulation directory not found: {}".format(args.sim_dir))

    variables = list(VARIABLES) + ([TM01_DIAGNOSTIC] if args.include_tm01 else [])

    obs_df = read_buoy(args.obs)
    if obs_df.empty:
        raise ValueError("No rows in buoy observations {}".format(args.obs))
    print("Buoy record: {} -> {}  ({} rows; columns: {})".format(
        obs_df.index.min(), obs_df.index.max(), len(obs_df), ", ".join(obs_df.columns)))

    ds = load_model_dataset(args.sim_dir)
    if args.start or args.end:
        ds = ds.sel(time=slice(args.start, args.end))
        if ds.sizes["time"] == 0:
            raise ValueError("No model output between {} and {}".format(args.start, args.end))
    label = ds.attrs.get("lake", os.path.basename(os.path.normpath(args.sim_dir)))
    eta, xi, clat, clon = nearest_water_cell(ds, args.lat, args.lon)
    dkm = haversine_km(args.lat, args.lon, clat, clon)
    print("Model span:  {} -> {}  (variables: {})".format(
        pd.to_datetime(ds["time"].values[0]), pd.to_datetime(ds["time"].values[-1]),
        ", ".join(ds.data_vars)))
    print("Nearest wet cell ({},{}) at {:.4f},{:.4f} — {:.2f} km from buoy".format(
        eta, xi, clat, clon, dkm))

    # Keep only the observations overlapping the run (a Level1 directory holds the
    # whole deployment), with a day either side so interpolation reaches the ends.
    t0, t1 = pd.to_datetime(ds["time"].values[[0, -1]])
    obs_df = obs_df.loc[t0 - pd.Timedelta(days=1): t1 + pd.Timedelta(days=1)]
    if obs_df.empty:
        raise ValueError("Buoy observations do not overlap the model span {} -> {}".format(t0, t1))

    model_df = model_series_at_cell(ds, eta, xi, variables, dir_convention=args.dir_convention)
    metrics_df, paired = compare(obs_df, model_df, variables, min_hs=args.min_hs)
    print("\nModel directions converted from SWAN Cartesian (travelling-to) to buoy "
          "convention '{}'.".format(args.dir_convention))
    if args.min_hs > 0:
        print("Period/direction pairs with buoy Hs < {} m excluded.".format(args.min_hs))
    print("\nPerformance metrics:")
    print(metrics_df.to_string(float_format=lambda v: "{:.3f}".format(v)))

    # If a large direction bias remains, the buoy may use a different convention;
    # try --dir-convention nautical-to / cartesian-to.
    if "PDIR" in metrics_df.index and abs(metrics_df.loc["PDIR", "bias"]) > 45:
        print("\nNote: direction bias still exceeds 45 deg — the buoy may use a "
              "different convention; try --dir-convention nautical-to or cartesian-to.")

    obs_path = os.path.abspath(args.obs)
    out_dir = args.output or (obs_path if os.path.isdir(obs_path) else os.path.dirname(obs_path))
    os.makedirs(out_dir, exist_ok=True)
    tag = str(label).replace("/", "_")
    out_png = os.path.join(out_dir, "performance_{}.png".format(tag))
    out_csv = os.path.join(out_dir, "performance_{}.csv".format(tag))
    out_ts = os.path.join(out_dir, "performance_{}_timeseries.csv".format(tag))

    info = {"label": label, "eta": eta, "xi": xi, "clat": clat, "clon": clon,
            "blat": args.lat, "blon": args.lon, "dkm": dkm}
    plot_performance(obs_df, model_df, paired, metrics_df, info, out_png, variables)
    metrics_df.to_csv(out_csv)
    print("Saved metrics: {}".format(out_csv))
    if args.split:
        by_period = metrics_by_period(paired, variables, args.split)
        out_by = os.path.join(out_dir, "performance_{}_by_{}.csv".format(tag, args.split))
        by_period.to_csv(out_by)
        print("\nMetrics by {}:".format(args.split))
        print(by_period.to_string(float_format=lambda v: "{:.3f}".format(v)))
        print("Saved: {}".format(out_by))
    merge_model_onto_buoy(obs_df, model_df, variables).to_csv(out_ts)
    print("Saved timeseries: {}".format(out_ts))


if __name__ == "__main__":
    main()
