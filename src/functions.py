import os
import boto3
from botocore.exceptions import ClientError
import shutil
import logging
import traceback
import numpy as np
import pandas as pd
from requests import get
from datetime import datetime, timedelta


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


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
              {"name": "end", "type": valid_date},
              {"name": "upload", "type": valid_bool, "default": False},
              {"name": "bucket", "type": valid_bucket, "default": False},
              {"name": "run", "type": valid_bool, "default": False},
              {"name": "files", "type": valid_path, "default": False},
              {"name": "api", "type": valid_string, "default": False},
              {"name": "today", "type": valid_date, "default": datetime.now()},
              {"name": "log", "type": valid_path, "default": False},
              ]

    for i in range(len(checks)):
        args[checks[i]["name"]] = checks[i]["type"](checks[i], args)

    return args


def boolean_string(s):
    if s not in {'False', 'True'}:
        raise ValueError('Not a valid boolean string')
    return s == 'True'


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
        if type(args[check["name"]]) is str or args[check["name"]] == check["default"]:
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
        if os.path.isdir(args[check["name"]]) or args[check["name"]] == check["default"]:
            return args[check["name"]]
        else:
            raise Exception("The path {} does not exist.".format(args[check["name"]]))


def valid_string(check, args):
    if check["name"] not in args:
        if "default" in check:
            return check["default"]
        else:
            raise Exception("A valid key: {} format path must be provided.".format(check["name"]))
    else:
        if type(args[check["name"]]) is str or args[check["name"]] == False:
            return args[check["name"]]
        else:
            raise Exception("The path {} does not exist.".format(args[check["name"]]))


def error(string):
    print('\033[91m' + string + '\033[0m')


class logger(object):
    def __init__(self, path=False, time=True):
        if path != False:
            if os.path.exists(os.path.dirname(path)):
                path.split(".")[0]
                if time:
                    self.path = "{}_{}.log".format(path.split(".")[0], datetime.now().strftime("%H%M%S.%f"))
                else:
                    self.path = "{}.log".format(path.split(".")[0])
            else:
                print("\033[93mUnable to find log folder: {}. Logs will be printed but not saved.\033[0m".format(os.path.dirname(path)))
                self.path = False
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

    def end_stage(self):
        out = datetime.now().strftime("%H:%M:%S.%f") + "   Stage {}: Completed.".format(self.stage - 1)
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

    def error(self):
        out = datetime.now().strftime("%H:%M:%S.%f") + "   ERROR: Script failed on stage {}".format(self.stage - 1)
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



def list_local_cosmo_files(folder, start_date, end_date, template="VNXQ34.{}0000.nc"):
    cosmo_files = []
    cosmo_dates = []
    cosmo_dates_str = []
    path = os.path.join(folder, "meteoswiss/cosmo/VNXQ34")
    for file in os.listdir(path):
        date = datetime.strptime(file.split(".")[1][:8], '%Y%m%d') - timedelta(days=1)
        if start_date <= date <= end_date:
            cosmo_files.append(os.path.join(path, file))
            cosmo_dates.append(date)
            cosmo_dates_str.append(date.strftime("%Y%m%d"))

    for day in np.arange(start_date, end_date, timedelta(days=1)).astype(datetime):
        if day.strftime("%Y%m%d") not in cosmo_dates_str:
            raise ValueError("COSMO data does not cover full simulation period.")

    df = pd.DataFrame(list(zip(cosmo_dates, cosmo_files)), columns=['dates', 'files'])
    df = df.sort_values(by=['dates'])
    return list(df["files"])


def latlng_to_ch1903(lat, lng):
    lat = lat * 3600
    lng = lng * 3600
    lat_aux = (lat - 169028.66) / 10000
    lng_aux = (lng - 26782.5) / 10000
    x = 2600072.37 + 211455.93 * lng_aux - 10938.51 * lng_aux * lat_aux - 0.36 * lng_aux * lat_aux ** 2 - 44.54 * lng_aux ** 3 - 2000000
    y = 1200147.07 + 308807.95 * lat_aux + 3745.25 * lng_aux ** 2 + 76.63 * lat_aux ** 2 - 194.56 * lng_aux ** 2 * lat_aux + 119.79 * lat_aux ** 3 - 1000000
    return x, y


def ch1903_to_latlng(x, y):
    x_aux = (x - 600000) / 1000000
    y_aux = (y - 200000) / 1000000
    lat = 16.9023892 + 3.238272 * y_aux - 0.270978 * x_aux ** 2 - 0.002528 * y_aux ** 2 - 0.0447 * x_aux ** 2 * y_aux - 0.014 * y_aux ** 3
    lng = 2.6779094 + 4.728982 * x_aux + 0.791484 * x_aux * y_aux + 0.1306 * x_aux * y_aux ** 2 - 0.0436 * x_aux ** 3
    lat = (lat * 100) / 36
    lng = (lng * 100) / 36
    return lat, lng


