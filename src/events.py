import os
import json
import netCDF4
import argparse
import xarray as xr
import numpy as np
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta, SU
import matplotlib.pyplot as plt
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


def threshold_detection(folder, parameters):
    return []
    print("Detecting if thresholds are exceeded")
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".nc")]
    files.sort()
    for file in files:
        with xr.open_dataset(file, chunks={'time': 10}) as da:
            if parameters["parameter"] == "temperature":
                data = da["R1"]
                data.where(data != -999, other=np.nan)
                dims = ['LSTSCI', 'KMAXOUT_RESTR', 'M', 'N']
            elif parameters["parameter"] == "velocity":
                u = da["U1"].rename({"MC": "M"})
                v = da["V1"].rename({"NC": "N"})
                u = u.where(u != -999.0, other=np.nan)
                v = v.where(v != -999.0, other=np.nan)
                data = (u**2 + v**2)**0.5
                dims = ['KMAXOUT_RESTR', 'M', 'N']
            if parameters["greaterthan"]:
                exceeds_threshold = data > parameters["value"]
            else:
                exceeds_threshold = data < parameters["value"]
            exceeds_at_timestep = exceeds_threshold.any(dim=dims)
    return []


def main(folder, docker):
    event_functions = {
        "upwelling": upwelling,
        "threshold_detection": threshold_detection
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
