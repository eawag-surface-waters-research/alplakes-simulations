import os
import shutil
import traceback
import numpy as np
import pandas as pd
from requests import get
from datetime import datetime, timedelta
from distutils.dir_util import copy_tree


def download_file(url, file_name):
    # open in binary mode
    with open(file_name, "wb") as file:
        # get request
        response = get(url)
        if response.status_code != 200:
            raise ValueError("Unable to download file, HTTP error code {}".format(response.status_code))
        # write to file
        file.write(response.content)

def verify_args(args):
    checks = [{"name": "model", "type": valid_model},
              {"name": "start", "type": valid_date},
              {"name": "end", "type": valid_date, "default": False},
              {"name": "upload", "type": valid_bucket, "default": False},
              {"name": "run", "type": valid_bool, "default": False},
              {"name": "files", "type": valid_path, "default": False},
              {"name": "log", "type": valid_path, "default": False},
              ]

    for i in range(len(checks)):
        args[checks[i]["name"]] = checks[i]["type"](checks[i], args)

    return args


def valid_date(check, args):
    if check["name"] not in args:
        if "default" in check:
            return check["default"]
        else:
            raise Exception("A valid key: {} format YYYYMMDD must be provided.".format(check["name"]))
    else:
        try:
            return datetime.strptime(args[check["name"]], '%Y%m%d')
        except:
            raise Exception("A valid key: {} format YYYYMMDD must be provided.".format(check["name"]))


def valid_model(check, args):
    if check["name"] not in args:
        if "default" in check:
            return check["default"]
        else:
            raise Exception('A valid key: {} format "software/model-name" must be provided.'.format(check["name"]))
    else:
        if os.path.isdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../static", args[check["name"]])):
            return args[check["name"]]
        else:
            raise Exception('A valid key: {} format "software/model-name" must be provided.'.format(check["name"]))


def valid_bool(check, args):
    if check["name"] not in args:
        if "default" in check:
            return check["default"]
        else:
            raise Exception("A valid key: {} format boolean must be provided.".format(check["name"]))
    else:
        if type(args[check["name"]]) is bool:
            return args[check["name"]]
        else:
            raise Exception("A valid key: {} format boolean must be provided.".format(check["name"]))


def valid_bucket(check, args):
    if check["name"] not in args:
        if "default" in check:
            return check["default"]
        else:
            raise Exception("A valid key: {} format boolean must be provided.".format(check["name"]))
    else:
        if type(args[check["name"]]) is str:
            return args[check["name"]]
        else:
            raise Exception("A valid key: {} format boolean must be provided.".format(check["name"]))


def valid_path(check, args):
    if check["name"] not in args:
        if "default" in check:
            return check["default"]
        else:
            raise Exception("A valid key: {} format path must be provided.".format(check["name"]))
    else:
        if os.path.isdir(args[check["name"]]) or args[check["name"]] == False:
            return args[check["name"]]
        else:
            raise Exception("The path {} does not exist.".format(args[check["name"]]))


def error(string):
    print('\033[91m' + string + '\033[0m')


class logger(object):
    def __init__(self, name, write=False):
        if write != False:
            self.path = os.path.join(write, name)
        else:
            self.path = False
        self.stage = 1

    def info(self, string, indent=0):
        out = datetime.now().strftime("%H:%M:%S.%f") + (" " * 3 * (indent + 1)) + string
        print(out)
        if self.path:
            with open(self.path, "a") as file:
                file.write(out + "\n")

    def initialise(self, string):
        out = "****** " + string + " ******"
        print('\033[1m' + out + '\033[0m')
        if self.path:
            with open(self.path, "a") as file:
                file.write(out + "\n")

    def begin_stage(self, string):
        self.newline()
        out = datetime.now().strftime("%H:%M:%S.%f") + "   Stage {}: ".format(self.stage) + string
        self.stage = self.stage + 1
        print('\033[95m' + out + '\033[0m')
        if self.path:
            with open(self.path, "a") as file:
                file.write(out + "\n")
        return self.stage - 1

    def end_stage(self, stage):
        out = datetime.now().strftime("%H:%M:%S.%f") + "   Stage {}: Completed.".format(stage)
        print('\033[92m' + out + '\033[0m')
        if self.path:
            with open(self.path, "a") as file:
                file.write(out + "\n")

    def warning(self, string, indent=0):
        out = datetime.now().strftime("%H:%M:%S.%f") + (" " * 3 * (indent + 1)) + "WARNING: " + string
        print('\033[93m' + out + '\033[0m')
        if self.path:
            with open(self.path, "a") as file:
                file.write(out + "\n")

    def error(self, stage):
        out = datetime.now().strftime("%H:%M:%S.%f") + "   ERROR: Script failed on stage {}".format(stage)
        print('\033[91m' + out + '\033[0m')
        if self.path:
            with open(self.path, "a") as file:
                file.write(out + "\n")
                file.write("\n")
                traceback.print_exc(file=file)

    def end(self, string):
        out = "****** " + string + " ******"
        print('\033[92m' + out + '\033[0m')
        if self.path:
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
            if self.path:
                with open(self.path, "a") as file:
                    file.write(out + "\n")
            return_code = process.poll()
            if return_code is not None:
                for output in process.stdout.readlines():
                    out = output.strip()
                    print(out)
                    if self.path:
                        with open(self.path, "a") as file:
                            file.write(out + "\n")
                break
        return failed

    def newline(self):
        print("")
        if self.path:
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
