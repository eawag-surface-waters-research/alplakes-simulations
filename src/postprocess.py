import os
import netCDF4
import argparse
import numpy as np
import xarray as xr
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta, SU
import functions


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


def process_output_mitgcm(folder, skip):
    output_files = []
    for root, dirs, files in os.walk(folder):
        if os.path.basename(root).startswith("thread_"):
            for file in files:
                if file.startswith("output."):
                    output_files.append(os.path.join(root, file))
    output_files.sort()

    grid = functions.get_mitgcm_grid(os.path.join(folder, "grid"))

    ds = xr.open_mfdataset(output_files[0])
    week_groups = {}
    for i, dt in enumerate(ds["T"].values):
        sunday = dt.astype('M8[ms]').astype(datetime) + relativedelta(weekday=SU(-1))
        if sunday.date() in week_groups:
            week_groups[sunday.date()].append(i)
        else:
            week_groups[sunday.date()] = [i]

    for week_start in sorted(week_groups):
        if skip and datetime.strptime(week_start, "%Y-%m-%d") < datetime.strptime(skip, "%Y%m%d"):
            continue
        indices = week_groups[week_start]
        print(f"Week starting {week_start}: Start index = {indices[0]}, Stop index = {indices[-1]}")




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
