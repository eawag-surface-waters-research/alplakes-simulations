import os
import json
import netCDF4
import argparse
import numpy as np
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta, SU
import functions


def upwelling(folder, centroid_difference, depth):
    print("Detecting upwelling events")
    events = []
    print(centroid_difference, depth)
    return events


def threshold_detection(folder, parameter, value, greaterthan, depth):
    print("Detecting if thresholds are exceeded")
    print(parameter, value, greaterthan, depth)
    return []


def main(folder, docker):
    with open(os.path.join(folder, "properties.json"), 'r') as f:
        properties = json.load(f)
    if "events" not in properties:
        print("No event definitions included in properties.json")
        return
    events = []
    if docker in ["eawag/delft3d-flow:6.03.00.62434", "eawag/delft3d-flow:6.02.10.142612"]:
        for event_definition in properties["events"]:
            if event_definition["type"] == "upwelling":
                events = events + upwelling(folder, event_definition["centroid_difference"], event_definition["depth"])
            if event_definition["type"] == "threshold_detection":
                events = events + threshold_detection(folder,
                                                      event_definition["parameter"],
                                                      event_definition["value"],
                                                      event_definition["greaterthan"],
                                                      event_definition["depth"])
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
