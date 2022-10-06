import os
import yaml
import traceback
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


def verify_args(file):
    if not os.path.isfile(file):
        raise Exception("File doesn't exist: {}".format(file))
    if ".yaml" not in file:
        raise Exception("File must be a .yaml file.")

    required = [{"name": "start_date", "default": False, "type": valid_date},
                {"name": "end_date", "default": False, "type": valid_date},
                {"name": "model", "default": False, "type": valid_string},
                {"name": "log_name", "default": "log", "type": valid_string},
                {"name": "log_path", "default": "", "type": valid_path},
                {"name": "setup", "default": False, "type": valid_string},
                {"name": "simulation_folder", "default": False, "type": valid_path},
                {"name": "weather_data", "default": False, "type": valid_path},
                {"name": "restart_files", "default": False, "type": valid_path},
                ]
    try:
        with open(file, "r") as f:
            parameters = yaml.load(f, Loader=yaml.FullLoader)
    except Exception as e:
        print(e)
        raise Exception("Failed to parse input yaml file.")

    for i in range(len(required)):
        key = required[i]["name"]
        parameters[key] = required[i]["type"](key, parameters, required[i]["default"])

    return parameters


def valid_date(key, parameters, default):
    if key not in parameters:
        if default == False:
            raise Exception("A valid key: {} format YYYYMMDD must be provided.".format(key))
        else:
            return default
    try:
        return datetime.strptime(parameters[key], '%Y%m%d')
    except:
        raise Exception("A valid key: {} format YYYYMMDD must be provided.".format(key))


def valid_string(key, parameters, default):
    if key not in parameters:
        if default == False:
            raise Exception("A valid key: {} format string must be provided.".format(key))
        else:
            return default
    if isinstance(parameters[key], str):
        return parameters[key]
    else:
        raise Exception("A valid key: {} format string must be provided.".format(key))


def valid_path(key, parameters, default):
    if key not in parameters:
        if default == False:
            raise Exception("A valid key: {} format path must be provided.".format(key))
        else:
            return default
    if os.path.isdir(parameters[key]):
        return parameters[key]
    else:
        raise Exception("A valid key: {} format path must be provided.".format(key))


def error(string):
    print('\033[91m' + string + '\033[0m')


class log(object):
    def __init__(self, name, path=""):
        self.name = name + datetime.now().strftime("_%Y%m%d_%H%M%S") + ".txt"
        self.path = os.path.join(path, self.name)
        self.stage = 1

    def log(self, string, indent=0):
        out = datetime.now().strftime("%H:%M:%S.%f") + (" " * 3 * (indent + 1)) + string
        print(out)
        with open(self.path, "a") as file:
            file.write(out + "\n")

    def initialise(self, string):
        out = "****** " + string + " ******"
        print('\033[1m' + out + '\033[0m')
        with open(self.path, "a") as file:
            file.write(out + "\n")

    def begin_stage(self, string):
        self.newline()
        out = datetime.now().strftime("%H:%M:%S.%f") + "   Stage {}: ".format(self.stage) + string
        self.stage = self.stage + 1
        print('\033[95m' + out + '\033[0m')
        with open(self.path, "a") as file:
            file.write(out + "\n")
        return self.stage - 1

    def end_stage(self, stage):
        out = datetime.now().strftime("%H:%M:%S.%f") + "   Stage {}: Completed.".format(stage)
        print('\033[92m' + out + '\033[0m')
        with open(self.path, "a") as file:
            file.write(out + "\n")

    def warning(self, string, indent=0):
        out = datetime.now().strftime("%H:%M:%S.%f") + (" " * 3 * (indent + 1)) + "WARNING: " + string
        print('\033[93m' + out + '\033[0m')
        with open(self.path, "a") as file:
            file.write(out + "\n")

    def error(self, stage):
        out = datetime.now().strftime("%H:%M:%S.%f") + "   ERROR: Script failed on stage {}".format(stage)
        print('\033[91m' + out + '\033[0m')
        with open(self.path, "a") as file:
            file.write(out + "\n")
            file.write("\n")
            traceback.print_exc(file=file)

    def end(self, string):
        out = "****** " + string + " ******"
        print('\033[92m' + out + '\033[0m')
        with open(self.path, "a") as file:
            file.write(out + "\n")

    def subprocess(self, process, error=""):
        failed = False
        while True:
            output = process.stdout.readline()
            out = output.strip()
            print(out)
            if error != "" and error in out:
                failed = True
            with open(self.path, "a") as file:
                file.write(out + "\n")
            return_code = process.poll()
            if return_code is not None:
                for output in process.stdout.readlines():
                    out = output.strip()
                    print(out)
                    with open(self.path, "a") as file:
                        file.write(out + "\n")
                break
        return failed

    def newline(self):
        print("")
        with open(self.path, "a") as file:
            file.write("\n")


def list_local_cosmo_files(folder, start_date, end_date, template="cosmo2_epfl_lakes_"):
    cosmo_files = []
    cosmo_dates = []
    cosmo_dates_str = []
    for path, subdirs, files in os.walk(folder):
        for name in files:
            if template in name:
                try:
                    date = datetime.strptime(name.split(".")[0].split("_")[-1], '%Y%m%d')
                    if start_date <= date <= end_date:
                        cosmo_files.append(os.path.join(path, name))
                        cosmo_dates.append(date)
                        cosmo_dates_str.append(date.strftime("%Y%m%d"))
                except:
                    raise ValueError("Failed to parse filename {}".format(name))

    for day in np.arange(start_date, end_date, timedelta(days=1)).astype(datetime):
        if day.strftime("%Y%m%d") not in cosmo_dates_str:
            raise ValueError("COSMO data does not cover full simulation period.")

    df = pd.DataFrame(list(zip(cosmo_dates, cosmo_files)), columns=['dates', 'files'])
    df = df.sort_values(by=['dates'])
    return list(df["files"])


def latlng_to_ch1900(lat, lng):
    lat = lat * 3600
    lng = lng * 3600
    lat_aux = (lat - 169028.66) / 10000
    lng_aux = (lng - 26782.5) / 10000
    x = 2600072.37 + 211455.93 * lng_aux - 10938.51 * lng_aux * lat_aux - 0.36 * lng_aux * lat_aux ** 2 - 44.54 * lng_aux ** 3 - 2000000
    y = 1200147.07 + 308807.95 * lat_aux + 3745.25 * lng_aux ** 2 + 76.63 * lat_aux ** 2 - 194.56 * lng_aux ** 2 * lat_aux + 119.79 * lat_aux ** 3 - 1000000
    return x, y