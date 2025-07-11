import re
import os
import sys
import netCDF4
import argparse
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timezone, timedelta
from functions import get_mitgcm_grid

def extract_data_from_input_file(file_path, slice):
    time_pattern = r"TIME = (.+) hours since (.+)"
    body = False
    slice_index = False
    timestamps, data, grid = [], [], []
    with open(file_path, 'r') as f:
        for line in f:
            if line.startswith("x_llcenter"):
                x_llcenter = float(line.split("=")[1].strip())
            elif line.startswith("y_llcenter"):
                y_llcenter = float(line.split("=")[1].strip())
            if line.startswith("dx"):
                dx = float(line.split("=")[1].strip())
            elif line.startswith("dy"):
                dy = float(line.split("=")[1].strip())
            elif line.startswith("n_rows"):
                n_rows = float(line.split("=")[1].strip())
            elif line.startswith('TIME'):
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

    if len(grid) > 0:
        data.append(np.array(grid))

    if slice:
        x, y = slice.split(",")
        xi = int((float(x) - x_llcenter)/dx)
        yi = int(n_rows) - int((float(y) - y_llcenter)/dy)
        slice_index = {"x": xi, "y": yi}

    return timestamps, data, slice_index


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


def plot_input_linegraph(inputs, nan_value=-999.0):
    fig = plt.figure(figsize=(18, 8))
    for i in range(len(inputs)):
        data = np.array(inputs[i]["data"])
        timestamps = np.array(inputs[i]["timestamps"])
        plt.subplot(3, 3, i + 1)
        data_mean = np.nanmean(data, axis=(1, 2))
        data_max = np.nanmax(data, axis=(1, 2))
        data_min = np.nanmin(data, axis=(1, 2))
        plt.fill_between(timestamps, data_max, data_min, color='skyblue', alpha=0.4)
        plt.plot(timestamps, data_mean)
        if inputs[i]["slice_index"]:
            plt.plot(timestamps, data[:, inputs[i]["slice_index"]["y"], inputs[i]["slice_index"]["x"]], color="r", linewidth=0.5)
        plt.xticks(rotation=45)
        plt.title(inputs[i]["file"].split(".")[0])
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
        nan_values = np.full(len(nan_indices), min_value)
        plt.scatter(nan_timestamps, nan_values, color='red', label='NaNs')
    plt.tight_layout()
    plt.show()


def extract_data_inputs_delft3dflow(folder, slice):
    input_files = [{"file": "CloudCoverage.amc"},
                   {"file": "Pressure.amp"},
                   {"file": "RelativeHumidity.amr"},
                   {"file": "ShortwaveFlux.ams"},
                   {"file": "Temperature.amt"},
                   {"file": "WindU.amu"},
                   {"file": "WindV.amv"},
                   {"file": "Secchi.scc"}]
    for f in input_files:
        timestamps, data, slice_index = extract_data_from_input_file(os.path.join(folder, f["file"]), slice)
        f["timestamps"] = timestamps
        f["data"] = data
        f["slice_index"] = slice_index
    return input_files


def extract_mitgcm_binary_file(file, shape):
    with open(file, 'rb') as fid:
        binary_data = np.fromfile(fid, dtype='>f8')
    t = int(len(binary_data) / (shape[0] * shape[1]))
    data = binary_data.reshape((t, shape[0], shape[1]))
    data = data[:216, ::-1, :]
    return data


def extract_parameters_from_file(file_path, parameters):
    out = {}
    with open(file_path, 'r') as f:
        for line in f:
            for p in parameters:
                if line.strip().startswith(p):
                    out[p] = line.split("=")[1].strip()
    return out

def extract_data_inputs_mitgcm(folder, slice):
    binary_data = os.path.join(folder, "binary_data")
    input_files = []
    slice_index = False
    grid = get_mitgcm_grid(os.path.join(folder, "grid"))
    shape = grid.lat_grid.shape
    s = extract_parameters_from_file(os.path.join(folder, "run_config/data"), ["startTime"])["startTime"]
    e = extract_parameters_from_file(os.path.join(folder, "run_config/data"), ["endTime"])["endTime"]
    origin = extract_parameters_from_file(os.path.join(folder, "run_config/data.cal"), ["startDate_1"])["startDate_1"]
    start = datetime.strptime(origin, "%Y%m%d") + timedelta(seconds=float(s))
    timesteps = int((float(e) - float(s))/3600)
    timestamps = [start + timedelta(hours=i) for i in range(timesteps)]
    for file in os.listdir(binary_data):
        if "bathy" in file:
            continue
        data = extract_mitgcm_binary_file(os.path.join(binary_data, file), shape)
        input_files.append({"file": file, "timestamps": timestamps, "data": data, "slice_index": slice_index})
    return input_files


def extract_data_outputs_delft3dflow(folder):
    output_file = os.path.join(folder, "trim-Simulation_Web.nc")
    if not os.path.exists(output_file):
        raise ValueError("No output file located.")
    parameters = [{"name": "Temperature", "variable": "R1", "pattern": [slice(None), 0, np.nan, slice(None)]},
                  {"name": "Velocity U", "variable": "U1", "pattern": [slice(None), np.nan, slice(None)]},
                  {"name": "Velocity V", "variable": "V1", "pattern": [slice(None), np.nan, slice(None)]},
                  ]
    for p in parameters:
        timestamps, data = extract_data_from_output_file(output_file, p["variable"], p["pattern"])
        p["timestamps"] = timestamps
        p["data"] = data
    return parameters


def main(folder, slice=False, heatmaps=False):
    path = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "runs", folder)
    if not os.path.exists(path):
        raise ValueError("{} not found.".format(folder))

    if "_delft3dflow_" in folder:
        extract_inputs = extract_data_inputs_delft3dflow
        extract_outputs = extract_data_outputs_delft3dflow

    elif "_mitgcm_" in folder:
        extract_inputs = extract_data_inputs_mitgcm
        #extract_outputs = extract_data_outputs_mitgcm
    else:
        raise ValueError("Unable to recognise model type.")

    input_files = extract_inputs(path, slice)
    plot_input_linegraph(input_files)
    if heatmaps:
        print("Saving to file meteorological forcing data as heatmaps")
        plot_input_heatmaps(input_files, path)

    output_files = extract_outputs(path)
    plot_output_linegraph(output_files)
    if heatmaps:
        print("Saving to file results data as heatmaps")
        plot_output_heatmaps(output_files, path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder', '-f', help="Model folder in the /run directory", type=str)
    parser.add_argument('--heatmaps', '-m', help="Save input & output heatmaps to files", action='store_true')
    parser.add_argument('--slice', '-s', help='Location for input and output sampling in model units', type=str, default=False)
    args = vars(parser.parse_args())
    main(args["folder"], args["slice"], args["heatmaps"])