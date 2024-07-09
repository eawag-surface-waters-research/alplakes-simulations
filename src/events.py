import os
import json
import netCDF4
import argparse
import xarray as xr
import numpy as np
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta, SU
import matplotlib.pyplot as plt
import scipy.ndimage
from scipy.cluster.vq import kmeans, vq
from functions import get_closest_index, convert_from_unit


def upwelling(folder, parameters):
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".nc")]
    files.sort()
    events = []
    event = False
    for file in files:
        with netCDF4.Dataset(file) as nc:
            depth_index = get_closest_index(parameters["depth"], np.array(nc.variables["ZK_LYR"][:]) * -1)
            time = [convert_from_unit(t, nc.variables["time"].units) for t in nc.variables["time"][:]]
            for time_index in range(len(time)):
                values = np.array(nc.variables["R1"][time_index, 0, depth_index, :]).flatten()
                mask = values != -999
                data = values[mask]
                k = 2
                centroids, _ = kmeans(data, k)
                if len(centroids) == 2:
                    diff = float(abs(centroids[1] - centroids[0]))
                    if diff > parameters["centroid_difference"]:
                        if event == False:
                            event = {
                                "start": time[time_index].strftime('%Y%m%d%H%M'),
                                "peak": time[time_index].strftime('%Y%m%d%H%M'),
                                "end": time[time_index].strftime('%Y%m%d%H%M'),
                                "max_centroid": diff
                            }
                        else:
                            event["end"] = time[time_index].strftime('%Y%m%d%H%M')
                            if diff > event["max_centroid"]:
                                event["peak"] = time[time_index].strftime('%Y%m%d%H%M')
                                event["max_centroid"] = diff

                        # Plot results
                        cluster_labels, _ = vq(data, centroids)
                        plot_values = np.array(nc.variables["R1"][time_index, 0, depth_index, :])
                        plot_values[plot_values == -999] = np.nan
                        plt.imshow(plot_values, cmap='seismic')
                        plt.colorbar(label="Temperature (°C)")
                        plt.title("Upwelling {}".format(time[time_index]))
                        plt.xlabel("Centroid difference: {}°C".format(round(diff, 1)))
                        plt.tight_layout()
                        out = np.zeros(len(values))
                        out[:] = np.nan
                        out[mask] = cluster_labels
                        out = out.reshape(plot_values.shape)
                        plt.contour(list(range(out.shape[1])), list(range(out.shape[0])), out, levels=[0, 1], colors='k',
                                    linewidths=1, linestyles='dashed')
                        os.makedirs(os.path.join(folder, "upwelling"), exist_ok=True)
                        plt.savefig(os.path.join(folder, "upwelling/upwelling_{}".format(time[time_index].strftime('%Y%m%d%H%M'))), bbox_inches='tight')
                        plt.close()
                    elif event != False:
                        events.append(event)
                        event = False
                elif event != False:
                    events.append(event)
                    event = False
    if event != False:
        events.append(event)
    return events


def localised_currents(folder, parameters):
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".nc")]
    files.sort()
    events = []
    event = False
    structure = np.ones((3, 3))
    for file in files:
        with netCDF4.Dataset(file) as nc:
            depth_index = get_closest_index(parameters["depth"], np.array(nc.variables["ZK_LYR"][:]) * -1)
            time = [convert_from_unit(t, nc.variables["time"].units) for t in nc.variables["time"][:]]
            area = np.count_nonzero(np.array(nc.variables["U1"][0, depth_index, :]) != -999)
            minCells = int(area * parameters["minArea"])
            maxCells = int(area * parameters["maxArea"])
            for time_index in range(len(time)):
                u = np.array(nc.variables["U1"][time_index, depth_index, :])
                v = np.array(nc.variables["V1"][time_index, depth_index, :])
                u[u == -999] = np.nan
                v[v == -999] = np.nan
                raw_values = (u ** 2 + v ** 2) ** 0.5
                values = raw_values.copy()
                values[np.isnan(values)] = -999
                values[values < parameters["threshold"]] = -999
                labeled_array, num_features = scipy.ndimage.label(values != -999, structure=structure)
                cluster_sizes = np.bincount(labeled_array.ravel())[1:]
                if len([c for c in cluster_sizes if c >= minCells and c <= maxCells]) > 0:
                    if event == False:
                        event = {
                            "start": time[time_index].strftime('%Y%m%d%H%M'),
                            "end": time[time_index].strftime('%Y%m%d%H%M')
                        }
                    else:
                        event["end"] = time[time_index].strftime('%Y%m%d%H%M')

                    data = labeled_array.copy()
                    for i, c in enumerate(cluster_sizes):
                        if c >= minCells and c <= maxCells:
                            data[data == i + 1] = 1
                        else:
                            data[data == i + 1] = 0
                    plt.imshow(raw_values, cmap='viridis', interpolation='nearest')
                    plt.colorbar(label="Velocity (m/s)")
                    plt.title("Localised currents {}".format(time[time_index]))
                    plt.tight_layout()
                    plt.contour(list(range(data.shape[1])), list(range(data.shape[0])), data, levels=[0, 1], colors='r',
                                linewidths=1, linestyles='dashed')
                    os.makedirs(os.path.join(folder, "localisedCurrents"), exist_ok=True)
                    plt.savefig(os.path.join(folder, "localisedCurrents/localisedCurrents_{}".format(time[time_index].strftime('%Y%m%d%H%M'))), bbox_inches='tight')
                    plt.close()
                elif event != False:
                    events.append(event)
                    event = False
    if event != False:
        events.append(event)
    return events


def main(folder, docker):
    event_functions = {
        "upwelling": upwelling,
        "localisedCurrents": localised_currents
    }
    with open(os.path.join(folder, "properties.json"), 'r') as f:
        properties = json.load(f)
    if "events" not in properties:
        print("No event definitions included in properties.json")
        return
    events = {}
    if docker in ["eawag/delft3d-flow:6.03.00.62434", "eawag/delft3d-flow:6.02.10.142612"]:
        for event_definition in properties["events"]:
            events[event_definition["type"]] = event_functions[event_definition["type"]](folder, event_definition["parameters"])
    else:
        raise ValueError("Postprocessing not defined for docker image {}".format(docker))
    with open(os.path.join(folder, "events.json"), 'w') as f:
        json.dump(events, f, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder', '-f', help="Simulation folder", type=str)
    parser.add_argument('--docker', '-d', help="Docker image e.g. eawag/delft3d-flow:6.02.10.142612", type=str, default="eawag/delft3d-flow:6.02.10.142612")
    args = parser.parse_args()
    main(vars(args)["folder"], vars(args)["docker"])
