import os
import shutil
import netCDF4
import sys
import pylake
import xarray
import argparse
import numpy as np
from datetime import timedelta
from dateutil.relativedelta import relativedelta, SU
import functions


def split_by_week(folder):
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


def calculate_variables(folder):
    print("Calculating variables.")
    for file in os.listdir(folder):
        print("Processing: {}".format(file))
        try:
            functions.thermocline(os.path.join(folder, file))
        except Exception as e:
            print(e)
            print("Failed to calculate thermocline.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder', '-f', help="Simulation folder", type=str)
    args = parser.parse_args()
    split_by_week(vars(args)["folder"])
    calculate_variables(vars(args)["folder"])
