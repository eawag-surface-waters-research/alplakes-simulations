import os
import json
import shutil
import argparse
from datetime import datetime, timedelta

import functions
from main import main
from postprocess import split_by_week, calculate_variables


def backfill(params):
    default_params = {
        "bucket": "alplakes-eawag",
        "upload": False,
        "restart": False,
        "run": False,
        "api": "http://eaw-alplakes2:8000",
        "log": False
    }
    params.update(default_params)
    if params["start"]:
        start = datetime.strptime(params["start"], '%Y%m%d')
    else:
        start = functions.closest_sunday(datetime.strptime(params["profile"].replace(".txt", ""), '%Y%m%d'))
    if params["end"]:
        end = datetime.strptime(params["end"], '%Y%m%d')
    else:
        end = functions.sunday_before(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0))

    with open('credentials.json') as f:
        creds = json.load(f)

    ice = functions.get_ice()

    model, lake = params["model"].split("/")
    api_server_folder = "/nfsmount/filesystem/media/simulations/{}/results_backfill/{}".format(model, lake)

    for week_start in functions.iterate_weeks(start, end):
        if functions.check_ice(lake, week_start, ice):
            print("Skipping {} due to ice.".format(week_start))
            params["profile"] = "default.txt"
            continue
        params["start"] = week_start.strftime("%Y%m%d")
        params["end"] = (week_start + timedelta(days=8)).strftime("%Y%m%d")
        params["today"] = datetime.now().strftime("%Y%m%d")
        restart = (week_start + timedelta(days=7)).strftime("%Y%m%d")
        simulation_dir = main(params)
        simulation_dir = os.path.abspath(simulation_dir)
        functions.run_simulation(params["bucket"], model, lake, restart, params["docker"], simulation_dir, params["cores"], creds)
        split_by_week(simulation_dir)
        calculate_variables(simulation_dir)
        functions.upload_results(simulation_dir, api_server_folder, creds)
        shutil.rmtree(simulation_dir)
        params["profile"] = False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', '-s', help="Start date e.g. 20221901", type=str, default=False)
    parser.add_argument('--end', '-e', help="End date e.g. 20221905", type=str, default=False)
    parser.add_argument('--model', '-m', help="Model name e.g. delft3d-flow/greifensee", type=str)
    parser.add_argument('--docker', '-d', help="Docker image e.g. eawag/delft3d-flow:5.01.00.2163", type=str, )
    parser.add_argument('--profile', '-p', help="Name of profile to start from should be in {lake}/profiles.", type=str)
    parser.add_argument('--cores', '-c', help="Number of cores for simulation", type=int, default=5)
    args = parser.parse_args()
    backfill(vars(args))
