import re
import os
import sys
import netCDF4
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timezone, timedelta


def extract_data_from_input_file(file_path):
    time_pattern = r"TIME = (.+) hours since (.+)"
    body = False
    timestamps, data, grid = [], [], []
    with open(file_path, 'r') as f:
        for line in f:
            if line.startswith('TIME'):
                body = True
                match = re.match(time_pattern, line)
                hours_since = float(match.group(1))
                start_date = datetime.strptime(match.group(2), "%Y-%m-%d %H:%M:%S %z")
                timestamp = start_date + timedelta(hours=hours_since)
                timestamps.append(timestamp)
                if len(grid) > 0:
                    data.append(np.array(grid))
                grid = []
            elif body:
                grid_row = [float(value) for value in line.split()]
                grid.append(grid_row)

    # Append the last set of grid data
    if len(grid) > 0:
        data.append(np.array(grid))

    return timestamps, data


def get_closest_index(value, array):
    array = np.asarray(array)
    sorted_array = np.sort(array)
    if len(array) == 0:
        raise ValueError("Array must be longer than len(0) to find index of value")
    elif len(array) == 1:
        return 0
    if value > (2 * sorted_array[-1] - sorted_array[-2]):
        raise ValueError("Value {} greater than max available ({})".format(value, sorted_array[-1]))
    elif value < (2 * sorted_array[0] - sorted_array[-1]):
        raise ValueError("Value {} less than min available ({})".format(value, sorted_array[0]))
    return (np.abs(array - value)).argmin()


def extract_data_from_output_file(file_path, variable, pattern, depth=1):
    with netCDF4.Dataset(file_path) as nc:
        times = np.array(nc.variables["time"][:])
        if str(pattern[-2]) == "nan":
            depth_index = get_closest_index(depth, np.array(nc.variables["ZK_LYR"][:]) * -1)
            pattern[-2] = depth_index
        data = np.array(nc.variables[variable][pattern])
        data[data == -999] = np.nan
        timestamps = [datetime.utcfromtimestamp(t + (datetime(2008, 3, 1).replace(tzinfo=timezone.utc) - datetime(1970, 1, 1).replace(tzinfo=timezone.utc)).total_seconds()).replace(tzinfo=timezone.utc) for t in times]
        return timestamps, data


def plot_input_heatmaps(inputs, folder):
    out_folder = os.path.join(folder, "plots", "inputs")
    os.makedirs(out_folder, exist_ok=True)
    for i in range(len(inputs[0]["timestamps"])):
        out_file = os.path.join(out_folder, "{}.png".format(inputs[0]["timestamps"][i]))
        if not os.path.exists(out_file):
            fig = plt.figure(figsize=(18, 8))
            fig.suptitle(inputs[0]["timestamps"][i])
            for j in range(len(inputs)):
                plt.subplot(3, 3, j + 1)
                plt.imshow(inputs[j]["data"][i], cmap='coolwarm', interpolation='nearest')
                plt.title(inputs[j]["file"].split(".")[0])
                plt.colorbar()
            plt.tight_layout()
            plt.savefig(out_file)
            plt.close()


def plot_input_linegraph(inputs):
    fig = plt.figure(figsize=(18, 8))
    for i in range(len(inputs)):
        data = np.array(inputs[i]["data"])
        min_value = np.nanmin(data)
        dims = data.shape
        y = int(dims[1]/2)
        x = int(dims[2]/2)
        timestamps = np.array(inputs[i]["timestamps"])
        plt.subplot(3, 3, i + 1)
        plt.plot(timestamps, data[:, y, x])
        plt.title(inputs[i]["file"].split(".")[0])
        nan_indices = np.where(np.isnan(data[:, y, x]))[0]
        nan_timestamps = timestamps[nan_indices]
        nan_values = np.full(len(nan_indices), min_value)
        plt.scatter(nan_timestamps, nan_values, color='red', label='NaNs')
    plt.tight_layout()
    plt.show()


def plot_output_heatmaps(inputs, folder):
    out_folder = os.path.join(folder, "plots", "outputs")
    os.makedirs(out_folder, exist_ok=True)
    for i in range(len(inputs[0]["timestamps"])):
        out_file = os.path.join(out_folder, "{}.png".format(inputs[0]["timestamps"][i]))
        if not os.path.exists(out_file):
            fig = plt.figure(figsize=(18, 8))
            fig.suptitle(inputs[0]["timestamps"][i])
            for j in range(len(inputs)):
                plt.subplot(1, 3, j + 1)
                plt.imshow(inputs[j]["data"][i], cmap='coolwarm', interpolation='nearest')
                plt.title(inputs[j]["name"])
                plt.colorbar()
            plt.tight_layout()
            plt.savefig(out_file)
            plt.close()


def plot_output_linegraph(inputs):
    fig = plt.figure(figsize=(18, 8))
    for i in range(len(inputs)):
        data = np.array(inputs[i]["data"])
        min_value = np.nanmin(data)
        dims = data.shape
        y = int(dims[1]/2)
        x = int(dims[2]/2)
        timestamps = np.array(inputs[i]["timestamps"])
        plt.subplot(1, 3, i + 1)
        plt.plot(timestamps, data[:, y, x])
        plt.title(inputs[i]["name"].split(".")[0])
        nan_indices = np.where(np.isnan(data[:, y, x]))[0]
        nan_timestamps = timestamps[nan_indices]
        print(np.array(nan_timestamps))
        nan_values = np.full(len(nan_indices), min_value)
        plt.scatter(nan_timestamps, nan_values, color='red', label='NaNs')
    plt.tight_layout()
    plt.show()


def plot_delft3d_files(run):
    folder = os.path.join("runs", run)
    input_files = [{"file": "CloudCoverage.amc"},
             {"file": "Pressure.amp"},
             {"file": "RelativeHumidity.amr"},
             {"file": "ShortwaveFlux.ams"},
             {"file": "Temperature.amt"},
             {"file": "WindU.amu"},
             {"file": "WindV.amv"}]cd gi
    for f in input_files:
        timestamps, data = extract_data_from_input_file(os.path.join(folder, f["file"]))
        f["timestamps"] = timestamps
        f["data"] = data
    #plot_input_heatmaps(input_files, folder)
    plot_input_linegraph(input_files)

    output_file = os.path.join(folder, "trim-Simulation_Web.nc")
    if os.path.exists(output_file):
        parameters = [{"name": "Temperature", "variable": "R1", "pattern": [slice(None), 0, np.nan, slice(None)]},
                      {"name": "Velocity U", "variable": "U1", "pattern": [slice(None), np.nan, slice(None)]},
                      {"name": "Velocity V", "variable": "V1", "pattern": [slice(None), np.nan, slice(None)]},
                      ]
        for p in parameters:
            timestamps, data = extract_data_from_output_file(output_file, p["variable"], p["pattern"])
            p["timestamps"] = timestamps
            p["data"] = data
        plot_output_heatmaps(parameters, folder)
        plot_output_linegraph(parameters)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise ValueError("Usage: python plot.py lake_key")
    else:
        plot_delft3d_files(sys.argv[1])