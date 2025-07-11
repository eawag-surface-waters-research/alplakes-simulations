import os
import shutil
import netCDF4
import pylake
import argparse
import numpy as np
import xarray as xr
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta, SU
import functions

import matplotlib.pyplot as plt


def verify_simulation_delft3d_flow(folder):
    print("Verify simulation results")
    file = os.path.join(folder, "trim-Simulation_Web.nc")
    if not os.path.isfile(file):
        raise ValueError("Unable to locate simulation results file trim-Simulation_Web.nc in {}".format(folder))
    with netCDF4.Dataset(file) as nc:
        for i in range(1, len(nc.variables["time"])):
            x = np.array(nc.variables["R1"][i, 0, :, :])
            x[x < 0] = np.nan
            if np.nanmin(x) == np.nanmean(x) == np.nanmax(x) and not np.all(x[~np.isnan(x)] == 4):
                raise ValueError("Simulation fails with all same values ({}degC) at {}"
                                 .format(np.nanmean(x),
                                         functions.convert_from_unit(nc.variables["time"][:][i],
                                                                     nc.variables["time"].units)))
            elif np.nanmin(x) < -5:
                raise ValueError("Simulation contains unrealistic temperature value ({}degC)".format(np.nanmin(x)))
            elif np.nanmax(x) > 40:
                raise ValueError("Simulation contains unrealistic temperature value ({}degC)".format(np.nanmax(x)))


def split_by_week_delft3d_flow(folder, skip=False):
    print("Splitting simulation results into weekly files")
    file = os.path.join(folder, "trim-Simulation_Web.nc")
    if not os.path.isfile(file):
        raise ValueError("Unable to locate simulation results file trim-Simulation_Web.nc in {}".format(folder))
    new_folder = os.path.join(folder, "postprocess")
    if not os.path.exists(new_folder):
        print("Creating {}".format(new_folder))
        os.makedirs(new_folder)
    print("Opening file {}".format(file))
    with netCDF4.Dataset(file, "r") as nc:
        time = np.array(nc.variables["time"][:])
        time_unit = nc.variables["time"].units
        min_time = functions.convert_from_unit(np.min(time), time_unit)
        max_time = functions.convert_from_unit(np.max(time), time_unit)
        start_time = min_time + relativedelta(weekday=SU(-1))
        end_time = start_time + timedelta(days=7)
        while start_time < max_time:
            if skip and start_time < datetime.strptime(skip, "%Y%m%d"):
                print("Skipping {}".format(start_time.strftime('%Y%m%d')))
                start_time = start_time + timedelta(days=7)
                end_time = end_time + timedelta(days=7)
                continue

            idx = np.where(np.logical_and(time >= functions.convert_to_unit(start_time, time_unit),
                                          time < functions.convert_to_unit(end_time, time_unit)))
            s = np.min(idx)
            e = np.max(idx) + 1
            final_file_name = os.path.join(new_folder, "{}.nc".format(start_time.strftime('%Y%m%d')))
            print("Outputting data to {}".format(final_file_name))
            with netCDF4.Dataset(final_file_name, "w") as dst:
                # Copy Attributes
                dst.setncatts(nc.__dict__)
                # Copy Dimensions
                for name, dimension in nc.dimensions.items():
                    dst.createDimension(name, (len(dimension) if not dimension.isunlimited() else None))
                # Copy Variables
                for name, variable in nc.variables.items():
                    x = dst.createVariable(name, variable.datatype, variable.dimensions)
                    if "time" in list(variable.dimensions):
                        if list(variable.dimensions)[0] != "time":
                            raise ValueError("Code only works with time as first dimension.")
                        if len(variable.dimensions) > 1:
                            dst[name][:] = nc[name][s:e, :]
                        else:
                            dst[name][:] = nc[name][s:e]
                    else:
                        dst[name][:] = nc[name][:]
                    dst[name].setncatts(nc[name].__dict__)
            start_time = start_time + timedelta(days=7)
            end_time = end_time + timedelta(days=7)


def calculate_variables_delft3d_flow(folder):
    print("Calculating variables.")
    for file in os.listdir(os.path.join(folder, "postprocess")):
        print("Processing: {}".format(file))
        try:
            functions.thermocline(os.path.join(folder, "postprocess", file))
        except Exception as e:
            print(e)
            print("Failed to calculate thermocline.")


def process_output_mitgcm(folder, skip, origin=datetime(2008, 6, 1), nodata=-999.0):
    output_files = []
    for thread in [f for f in os.listdir(os.path.join(folder, "run")) if os.path.basename(f).startswith("thread_")]:
        files = [os.path.join(folder, "run", thread, f) for f in os.listdir(os.path.join(folder, "run", thread)) if f.startswith("output.")]
        files.sort()
        output_files.append(files)

    grid = functions.get_mitgcm_grid(os.path.join(folder, "grid"))
    z_faces = -np.concatenate(([0], np.cumsum(grid.dz.flatten())))
    depth = -(z_faces[:-1] + z_faces[1:]) / 2

    ds = xr.open_mfdataset(output_files[0])
    full_time = ds["T"].values

    general_attributes = {
        "MITgcm_version": ds.attrs.get('MITgcm_version')
    }

    dimensions = {
        'time': {'dim_name': 'time', 'dim_size': None},
        'depth': {'dim_name': 'depth', 'dim_size': len(depth)},
        'X': {'dim_name': 'X', 'dim_size': grid.lat_grid.shape[1]},
        'Y': {'dim_name': 'Y', 'dim_size': grid.lat_grid.shape[0]}
    }
    variables = {
        'time': {'var_name': 'time', 'dim': ('time',), 'unit': 'seconds since 1970-01-01 00:00:00',
                 'long_name': 'time'},
        'depth': {'var_name': 'depth', 'dim': ('depth',), 'unit': 'm', 'long_name': 'Depth below surface'},
        'lat': {'var_name': 'lat', 'dim': ('Y', 'X',), 'unit': '', 'long_name': 'Latitude'},
        'lng': {'var_name': 'lng', 'dim': ('Y', 'X',), 'unit': '', 'long_name': 'Longitude'},
        't': {'var_name': 't', 'dim': ('time', 'depth', 'Y', 'X',), 'unit': '°C', 'long_name': 'Temperature'},
        'u': {'var_name': 'u', 'dim': ('time', 'depth', 'Y', 'X',), 'unit': 'm/s', 'long_name': 'Eastward velocity'},
        'v': {'var_name': 'v', 'dim': ('time', 'depth', 'Y', 'X',), 'unit': 'm/s', 'long_name': 'Northward velocity'},
        'w': {'var_name': 'w', 'dim': ('time', 'depth', 'Y', 'X',), 'unit': 'm/s', 'long_name': 'Vertical velocity'},
        'thermocline': {'var_name': 'thermocline', 'dim': ('time', 'Y', 'X',), 'unit': 'm', 'long_name': 'Thermocline calculated using PyLake'},
    }

    week_groups = {}
    for i, dt in enumerate(full_time):
        sunday = dt.astype('M8[ms]').astype(datetime) + relativedelta(weekday=SU(-1))
        if sunday.strftime("%Y%m%d") in week_groups:
            week_groups[sunday.strftime("%Y%m%d")].append(i)
        else:
            week_groups[sunday.strftime("%Y%m%d")] = [i]

    output_folder = os.path.join(folder, "postprocess")
    os.makedirs(output_folder, exist_ok=True)

    for week_start in sorted(week_groups):
        if skip and datetime.strptime(week_start, "%Y%m%d") < datetime.strptime(skip, "%Y%m%d"):
            continue
        indices = week_groups[week_start]
        print(f"Exporting data for week starting {week_start}")
        time = [(full_time[i] - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, 's') for i in range(indices[0], indices[-1] + 1)]
        with netCDF4.Dataset(os.path.join(output_folder, week_start + ".nc"), "w") as dst:
            for key in general_attributes:
                setattr(dst, key, general_attributes[key])
            for key, values in dimensions.items():
                dst.createDimension(values['dim_name'], values['dim_size'])
            for key, values in variables.items():
                variables[key]["nc"] = dst.createVariable(values["var_name"], np.float64, values["dim"], fill_value=nodata)
                variables[key]["nc"].units = values["unit"]
                variables[key]["nc"].long_name = values["long_name"]

            variables["time"]["nc"][:] = time
            variables["depth"]["nc"][:] = depth
            variables["lat"]["nc"][:] = grid.lat_grid
            variables["lng"]["nc"][:] = grid.lon_grid

            for f in output_files:
                print("  Reading {}".format(os.path.basename(os.path.dirname(f[0]))))
                if len(f) > 1:
                    with xr.open_mfdataset(f) as ds:
                        x = np.array(ds["X"].values)
                        y = ds["Y"].values
                        t = ds["THETA"].isel(T=slice(indices[0], indices[-1] + 1)).values
                        w = ds["WVEL"].isel(T=slice(indices[0], indices[-1] + 1)).values
                        uvel = ds["UVEL"].isel(T=slice(indices[0], indices[-1] + 1)).values
                        vvel = ds["VVEL"].isel(T=slice(indices[0], indices[-1] + 1)).values
                else:
                    with netCDF4.Dataset(f[0], "r") as nc:
                        x = np.array(nc.variables["X"][:])
                        y = nc.variables["Y"][:]
                        t = nc.variables["THETA"][indices[0]:indices[-1] + 1, :]
                        w = nc.variables["WVEL"][indices[0]:indices[-1] + 1, :]
                        uvel = nc.variables["UVEL"][indices[0]:indices[-1] + 1, :]
                        vvel = nc.variables["VVEL"][indices[0]:indices[-1] + 1, :]

                uvel = (uvel[..., :-1] + uvel[..., 1:]) / 2  # Get cell center
                vvel = (vvel[..., :-1, :] + vvel[..., 1:, :]) / 2 # Get cell center
                if "rotation" in grid.parameters:
                    print("    Rotating u,v by {}°".format(-grid.parameters["rotation"]))
                    theta_rad = np.deg2rad(-grid.parameters["rotation"])
                    u = uvel * np.cos(theta_rad) - vvel * np.sin(theta_rad)
                    v = uvel * np.sin(theta_rad) + vvel * np.cos(theta_rad)
                else:
                    u = uvel
                    v = vvel

                mask = (t == 0.0) | np.isnan(t)
                t[mask] = nodata
                w[mask] = nodata
                u[mask] = nodata
                v[mask] = nodata

                failed = np.all(t == nodata, axis=(1, 2, 3))
                if np.any(failed):
                    failed_index = np.argmax(failed)
                    print("Simulation failed at time: {}, index: {}".format(time[failed_index], failed_index))
                    #os.remove(os.path.join(output_folder, week_start + ".nc"))
                    #raise ValueError("Simulation failed at time: {}, index: {}".format(time[failed_index], failed_index))

                data = {"t": t, "w": w, "u": u, "v": v}

                for key, values in data.items():
                    variables[key]["nc"][:, :, int(y[0] - 1):int(y[-1]), int(x[0] - 1): int(x[-1])] = values

                print("    Computing thermocline.")
                dt = np.reshape(t, [t.shape[0], t.shape[1], t.shape[2] * t.shape[3]])
                dt[dt == -999] = np.nan
                array = xr.DataArray(
                    data=dt,
                    dims=["time", "depth", "data"],
                    coords=dict(
                        time=("time", time),
                        depth=("depth", depth),
                        data=("data", np.arange(dt.shape[2]))
                    )
                )
                therm, index = pylake.thermocline(array)
                therm = np.array(therm)
                therm = np.reshape(therm, [therm.shape[0], t.shape[2], t.shape[3]])
                therm[therm == np.nanmax(therm)] = np.nan
                therm[therm < 0] = np.nan
                therm[therm > np.nanmax(depth)] = np.nan
                therm[np.isnan(therm)] = nodata
                variables["thermocline"]["nc"][:, int(y[0] - 1):int(y[-1]), int(x[0] - 1): int(x[-1])] = therm

    pickups = list(set([f.split(".")[1] for f in os.listdir(os.path.join(folder, "run")) if "pickup.00" in f]))
    for pickup in pickups:
        with open(os.path.join(folder, "run", "pickup.{}.meta".format(pickup)), "r") as file:
            lines = file.readlines()
        name = False
        for i, line in enumerate(lines):
            if line.strip().startswith("timeStepNumber"):
                lines[i] = " timeStepNumber = [          0 ];\n"
            if line.strip().startswith("timeInterval"):
                dt = origin + timedelta(seconds=float(line.split("=")[1].split("[")[1].split("]")[0].strip()))
                if dt.weekday() != 6 or dt.hour != 0 or dt.minute != 0:
                    raise ValueError("Pickup file produced for incorrect date")
                name = dt.strftime("%Y%m%d")
                lines[i] = " timeInterval = [  0.0 ];\n"
        if name:
            shutil.copy(os.path.join(folder, "run", "pickup.{}.data".format(pickup)),
                        os.path.join(folder, "run", "pickup.{}.data".format(name)))
            with open(os.path.join(folder, "run", "pickup.{}.meta".format(name)), "w") as file:
                file.writelines(lines)


def main(folder, docker, skip=False):
    if docker in ["eawag/delft3d-flow:6.03.00.62434", "eawag/delft3d-flow:6.02.10.142612"]:
        verify_simulation_delft3d_flow(folder)
        split_by_week_delft3d_flow(folder, skip)
        calculate_variables_delft3d_flow(folder)
    elif "mitgcm" in docker:
        process_output_mitgcm(folder, skip)
    else:
        raise ValueError("Postprocessing not defined for docker image {}".format(docker))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder', '-f', help="Simulation folder", type=str)
    parser.add_argument('--docker', '-d', help="Docker image e.g. eawag/delft3d-flow:6.02.10.142612", type=str, default="eawag/delft3d-flow:6.02.10.142612")
    parser.add_argument('--skip', '-s', help="Don't process weeks before %Y%m%d", type=str, default=False)
    args = parser.parse_args()
    main(vars(args)["folder"], vars(args)["docker"], skip=vars(args)["skip"])
