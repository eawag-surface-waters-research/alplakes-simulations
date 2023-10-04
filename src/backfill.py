import os
import shutil
import argparse
from datetime import datetime, timedelta

import functions
from main import main
import postprocess


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
        simulation_dir_docker = simulation_dir
        if params["filesystem"]:
            simulation_dir_docker = os.path.join(params["filesystem"], "runs", simulation_dir.split("/runs/")[1])

        functions.run_simulation(params["bucket"], model, lake, restart, params["docker"], simulation_dir,
                                 simulation_dir_docker, params["cores"], params["awsid"], params["awskey"])
        postprocess.main(simulation_dir, params["docker"])
        functions.upload_results(simulation_dir, api_server_folder, params["apiserver"], params["apiuser"],
                                 params["apipassword"])
        shutil.rmtree(simulation_dir)
        params["profile"] = False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', '-s', help="Start date e.g. 20221901", type=str, default=False)
    parser.add_argument('--end', '-e', help="End date e.g. 20221905", type=str, default=False)
    parser.add_argument('--model', '-m', help="Model name e.g. delft3d-flow/greifensee", type=str)
    parser.add_argument('--docker', '-d', help="Docker image e.g. eawag/delft3d-flow:5.01.00.2163",
                        type=str, default="eawag/delft3d-flow:6.02.10.142612")
    parser.add_argument('--profile', '-p', help="Name of profile to start from should be in {lake}/profiles.", type=str)
    parser.add_argument('--cores', '-c', help="Number of cores for simulation", type=str, default=5)
    parser.add_argument('--awsid', '-i', help="AWS ID", type=str, default=False)
    parser.add_argument('--awskey', '-k', help="AWS KEY", type=str, default=False)
    parser.add_argument('--apiuser', '-u', help="API username", type=str, default="alplakes")
    parser.add_argument('--apiserver', '-v', help="API server-name", type=str, default="eaw-alplakes2")
    parser.add_argument('--apipassword', '-w', help="API password", type=str, default=False)
    parser.add_argument('--filesystem', '-f', help="Local filesystem when run in docker container", type=str, default=False)
    args = parser.parse_args()
    backfill(vars(args))
