import os
import json
import netCDF4
import argparse
import xarray as xr
import numpy as np
from datetime import timedelta, datetime, timezone
from dateutil.relativedelta import relativedelta, SU
import matplotlib.pyplot as plt
import scipy.ndimage
from scipy.cluster.vq import kmeans, vq
from functions import get_closest_index, convert_from_unit


def upwelling(folder, parameters):
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".nc")]
    files.sort()
    events = []
    event = None
    for file in files:
        with netCDF4.Dataset(file) as nc:
            depth_index = get_closest_index(parameters["depth"], np.array(nc.variables["ZK_LYR"][:]) * -1)
            time = [convert_from_unit(t, nc.variables["time"].units).replace(tzinfo=timezone.utc) for t in nc.variables["time"][:]]
            for time_index in range(len(time)):
                values = np.array(nc.variables["R1"][time_index, 0, depth_index, :]).flatten()
                mask = values != -999
                data = values[mask]
                k = 2
                centroids, _ = kmeans(data, k)
                new = True
                if len(centroids) == 2:
                    diff = float(abs(centroids[1] - centroids[0]))
                    if diff > parameters["centroid_difference"]:
                        if event is None:
                            if len(events) > 0:
                                merge_time = datetime.fromisoformat(events[-1]["end"]) + timedelta(hours=parameters["merge"])
                                if time[time_index] <= merge_time:
                                    event = events[-1]
                                    events = events[:-1]
                                    new = False
                        else:
                            new = False
                        if new:
                            event = {
                                "type": "upwelling",
                                "description": parameters["description"],
                                "start": time[time_index].isoformat(),
                                "end": time[time_index].isoformat(),
                                "properties": {"peak": time[time_index].isoformat(),
                                               "max_centroid": diff},
                                "parameters": {
                                    "depth": parameters["depth"],
                                    "centroid_difference": parameters["centroid_difference"],
                                }
                            }
                        else:
                            event["end"] = time[time_index].isoformat()
                            if diff > event["properties"]["max_centroid"]:
                                event["properties"]["peak"] = time[time_index].isoformat()
                                event["properties"]["max_centroid"] = diff

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
                        os.makedirs(os.path.join(folder, "events"), exist_ok=True)
                        plt.savefig(os.path.join(folder, "events/upwelling_{}".format(time[time_index].isoformat())), bbox_inches='tight')
                        plt.close()
                    elif event is not None:
                        events.append(event)
                        event = None
                elif event is not None:
                    events.append(event)
                    event = None
    if event is not None:
        events.append(event)
    return events


def localised_currents(folder, parameters):
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".nc")]
    files.sort()
    events = []
    event = None
    structure = np.ones((3, 3))
    for file in files:
        with netCDF4.Dataset(file) as nc:
            depth_index = get_closest_index(parameters["depth"], np.array(nc.variables["ZK_LYR"][:]) * -1)
            time = [convert_from_unit(t, nc.variables["time"].units).replace(tzinfo=timezone.utc) for t in nc.variables["time"][:]]
            area = np.count_nonzero(np.array(nc.variables["U1"][0, depth_index, :]) != -999)
            minCells = int(area * (parameters["min_area"] / parameters["total_area"]))
            maxCells = int(area * (parameters["max_area"] / parameters["total_area"]))
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
                new = True
                if len([c for c in cluster_sizes if c >= minCells and c <= maxCells]) > 0:
                    if event is None:
                        if len(events) > 0:
                            merge_time = datetime.fromisoformat(events[-1]["end"]) + timedelta(
                                hours=parameters["merge"])
                            if time[time_index] <= merge_time:
                                event = events[-1]
                                events = events[:-1]
                                new = False
                    else:
                        new = False
                    if new:
                        event = {
                            "type": "localisedCurrents",
                            "description": parameters["description"],
                            "start": time[time_index].isoformat(),
                            "end": time[time_index].isoformat(),
                            "properties": {},
                            "parameters": {
                                "depth": parameters["depth"],
                                "threshold": parameters["threshold"],
                                "min_area": parameters["min_area"],
                                "max_area": parameters["max_area"],
                                "total_area": parameters["total_area"]
                            }
                        }
                    else:
                        event["end"] = time[time_index].isoformat()

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
                    os.makedirs(os.path.join(folder, "events"), exist_ok=True)
                    plt.savefig(os.path.join(folder, "events/localisedCurrents_{}".format(time[time_index].isoformat())), bbox_inches='tight')
                    plt.close()
                elif event is not None:
                    events.append(event)
                    event = None
    if event is not None:
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
    events = []
    if docker in ["eawag/delft3d-flow:6.03.00.62434", "eawag/delft3d-flow:6.02.10.142612"]:
        for event_definition in properties["events"]:
            events.extend(event_functions[event_definition["type"]](folder, event_definition["parameters"]))
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
