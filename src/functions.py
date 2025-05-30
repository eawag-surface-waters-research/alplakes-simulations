import os
import json
import time
import boto3
import shutil
import pylake
import netCDF4
import logging
import requests
import traceback
import subprocess
import numpy as np
import xarray as xr
import pandas as pd
from requests import get
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone


def download_data(query, attempts=3, timeout=120, sleep=30, download=False):
    print(query)
    for attempt in range(attempts):
        try:
            response = requests.get(query, timeout=timeout)
            if response.status_code == 200:
                if download:
                    with open(download, "w") as file:
                        file.write(response.text)
                return response.json()
            else:
                raise ValueError("Unable to download data, HTTP error code {}".format(response.status_code))
        except Exception as e:
            print("Attempt {}/{} failed. Sleeping for {}s.".format(attempt + 1, attempts, sleep))
            print(e)
            if attempt == attempts - 1:
                return False
            time.sleep(sleep)


def convert_to_unit(time, units):
    if units == "seconds since 2008-03-01 00:00:00":
        return (time.replace(tzinfo=timezone.utc) - datetime(2008, 3, 1).replace(tzinfo=timezone.utc)).total_seconds()
    elif units == "seconds since 1970-01-01 00:00:00":
        return time.timestamp()
    else:
        raise ValueError("Unrecognised time unit.")


def convert_from_unit(time, units):
    if units == "seconds since 2008-03-01 00:00:00":
        return datetime.utcfromtimestamp(time + (
                    datetime(2008, 3, 1).replace(tzinfo=timezone.utc) - datetime(1970, 1, 1).replace(
                tzinfo=timezone.utc)).total_seconds())
    elif units == "seconds since 1970-01-01 00:00:00":
        return datetime.utcfromtimestamp(time)
    else:
        raise ValueError("Unrecognised time unit.")


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
    try:
        with open(file_name, "wb") as file:
            url = url.replace("\\", "/")
            response = get(url)
            if response.status_code != 200:
                return response.status_code
            file.write(response.content)
        return response.status_code
    except:
        return 400


def closest_sunday(input_date):
    days_until_previous_sunday = input_date.weekday() + 1
    if days_until_previous_sunday == 7:
        days_until_previous_sunday = 0
    days_until_next_sunday = (6 - input_date.weekday()) % 7
    previous_sunday = input_date - timedelta(days=days_until_previous_sunday)
    next_sunday = input_date + timedelta(days=days_until_next_sunday)
    if days_until_previous_sunday <= days_until_next_sunday:
        sunday = previous_sunday
    else:
        sunday = next_sunday
    return sunday


def sunday_before(input_date):
    days_until_previous_sunday = input_date.weekday() + 1
    if days_until_previous_sunday == 7:
        days_until_previous_sunday = 0
    previous_sunday = input_date - timedelta(days=days_until_previous_sunday)
    return previous_sunday


def iterate_weeks(start_date, end_date):
    current_date = start_date
    while current_date < end_date:
        yield current_date
        current_date += timedelta(weeks=1)


def verify_args(args):
    checks = [{"name": "model", "type": valid_model},
              {"name": "start", "type": valid_date},
              {"name": "end", "type": valid_date},
              {"name": "upload", "type": valid_bool, "default": False},
              {"name": "bucket", "type": valid_bucket, "default": False},
              {"name": "restart", "type": valid_file, "default": False},
              {"name": "run", "type": valid_bool, "default": False},
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


def get_ice():
    response = get("https://alplakes-eawag.s3.eu-central-1.amazonaws.com/simulations/ice.json")
    if response.status_code == 200:
        json_data = response.json()
        return json_data
    else:
        raise ValueError(f"Request failed with status code {response.status_code}")


def check_ice(lake, date, ice):
    if lake in ice:
        for period in ice[lake]:
            if len(period) == 2:
                if datetime.strptime(str(period[0]), '%Y%m%d') < date < datetime.strptime(str(period[1]), '%Y%m%d'):
                    return True
            elif len(period) == 1:
                if date > datetime.strptime(str(period[0]), '%Y%m%d'):
                    return True
    else:
        return False


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


def valid_file(check, args):
    if check["name"] not in args:
        if "default" in check:
            return check["default"]
        else:
            raise Exception("A valid key: {} format path must be provided.".format(check["name"]))
    else:
        if os.path.isfile(args[check["name"]]) or args[check["name"]] == check["default"]:
            return args[check["name"]]
        else:
            raise Exception("The file {} does not exist.".format(args[check["name"]]))


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
                print("\033[93mUnable to find log folder: {}. Logs will be printed but not saved.\033[0m".format(
                    os.path.dirname(path)))
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


def run_simulation(bucket, model, lake, restart, docker, simulation_dir, simulation_dir_docker, cores, AWS_ID, AWS_KEY):
    r = ("s3://{}/simulations/{}/restart-files/{}/tri-rst.Simulation_Web_rst.{}.000000"
         .format(bucket, model, lake, restart))
    cmd = ["docker", "run",
           "-e", "AWS_ID={}".format(AWS_ID),
           "-e", "AWS_KEY={}".format(AWS_KEY),
           "-v", "{}:/job".format(simulation_dir_docker),
           "--rm", docker, "-p", str(cores),
           "-r", r]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                               cwd=simulation_dir)
    while True:
        output = process.stdout.readline()
        out = output.strip()
        print(out)
        return_code = process.poll()
        if return_code is not None:
            for output in process.stdout.readlines():
                out = output.strip()
                print(out)
            break
    if process.returncode != 0:
        stderr_output = process.stderr.read()
        print(stderr_output)
        raise RuntimeError("Simulation failed.")


def upload_results(simulation_dir, api_server_folder, api_server, api_user, api_password):
    cmd = ("sshpass -p {} ssh {}@{} mkdir -p {}"
           .format(api_password, api_user, api_server, api_server_folder))

    process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if process.returncode != 0:
        print(process.returncode, process.stdout)
        raise RuntimeError("Create folder failed.")

    cmd = ("sshpass -p {} scp -r -o StrictHostKeyChecking=no {}/postprocess/* {}@{}:{}"
           .format(api_password, simulation_dir, api_user, api_server, api_server_folder))

    process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if process.returncode != 0:
        print(process.returncode, process.stdout)
        raise RuntimeError("Upload files failed.")


def thermocline(file, overwrite=False):
    with netCDF4.Dataset(file, 'r') as nc:
        if "THERMOCLINE" in nc.variables.keys() and not overwrite:
            print("Thermocline already calculated.")
            return
    temp_file = file.replace(".nc", "_temp.nc")
    try:
        shutil.copyfile(file, temp_file)
        with netCDF4.Dataset(temp_file, 'a') as nc:
            data = np.array(nc.variables["R1"][:, 0, :, :, :])
            data = np.reshape(data, [data.shape[0], data.shape[1], data.shape[2] * data.shape[3]])
            data[data == -999] = np.nan
            depth = np.array(nc.variables["ZK_LYR"][:]) * -1
            data_xr = xr.DataArray(
                data=data,
                dims=["time", "depth", "data"],
                coords=dict(
                    time=("time", nc.variables["time"][:]),
                    depth=("depth", depth),
                    data=("data", np.arange(data.shape[2]))
                )
            )
            t, index = pylake.thermocline(data_xr)
            t = np.array(t)
            t = np.reshape(t, [t.shape[0], nc.dimensions["M"].size, nc.dimensions["N"].size])
            t[t == np.nanmax(t)] = np.nan
            t[t < 0] = np.nan
            t[t > np.nanmax(depth)] = np.nan
            t[np.isnan(t)] = -999.0

            if overwrite:
                var = nc.variables["THERMOCLINE"]
            else:
                var = nc.createVariable("THERMOCLINE", np.float64, ['time', 'M', 'N'], fill_value=-999.0)
                var.units = "m"
                var.description = 'Thermocline calculate using PyLake'

            var[:] = t
        os.rename(temp_file, file)
    except:
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise


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


def utm_to_latlng(easting, northing, zone_number, zone_letter=None, northern=None, strict=True):
    K0 = 0.9996

    E = 0.00669438
    E2 = E * E
    E3 = E2 * E
    E_P2 = E / (1 - E)

    SQRT_E = np.sqrt(1 - E)
    _E = (1 - SQRT_E) / (1 + SQRT_E)
    _E2 = _E * _E
    _E3 = _E2 * _E
    _E4 = _E3 * _E
    _E5 = _E4 * _E

    M1 = (1 - E / 4 - 3 * E2 / 64 - 5 * E3 / 256)

    P2 = (3 / 2 * _E - 27 / 32 * _E3 + 269 / 512 * _E5)
    P3 = (21 / 16 * _E2 - 55 / 32 * _E4)
    P4 = (151 / 96 * _E3 - 417 / 128 * _E5)
    P5 = (1097 / 512 * _E4)
    R = 6378137

    if not zone_letter and northern is None:
        raise ValueError('either zone_letter or northern needs to be set')

    elif zone_letter and northern is not None:
        raise ValueError('set either zone_letter or northern, but not both')

    if strict:
        if not in_bounds(easting, 100000, 1000000, upper_strict=True):
            raise ValueError('easting out of range (must be between 100,000 m and 999,999 m)')
        if not in_bounds(northing, 0, 10000000):
            raise ValueError('northing out of range (must be between 0 m and 10,000,000 m)')

    check_valid_zone(zone_number, zone_letter)

    if zone_letter:
        zone_letter = zone_letter.upper()
        northern = (zone_letter >= 'N')

    x = easting - 500000
    y = northing

    if not northern:
        y -= 10000000

    m = y / K0
    mu = m / (R * M1)

    p_rad = (mu +
             P2 * np.sin(2 * mu) +
             P3 * np.sin(4 * mu) +
             P4 * np.sin(6 * mu) +
             P5 * np.sin(8 * mu))

    p_sin = np.sin(p_rad)
    p_sin2 = p_sin * p_sin

    p_cos = np.cos(p_rad)

    p_tan = p_sin / p_cos
    p_tan2 = p_tan * p_tan
    p_tan4 = p_tan2 * p_tan2

    ep_sin = 1 - E * p_sin2
    ep_sin_sqrt = np.sqrt(1 - E * p_sin2)

    n = R / ep_sin_sqrt
    r = (1 - E) / ep_sin

    c = E_P2 * p_cos ** 2
    c2 = c * c

    d = x / (n * K0)
    d2 = d * d
    d3 = d2 * d
    d4 = d3 * d
    d5 = d4 * d
    d6 = d5 * d

    latitude = (p_rad - (p_tan / r) *
                (d2 / 2 -
                 d4 / 24 * (5 + 3 * p_tan2 + 10 * c - 4 * c2 - 9 * E_P2)) +
                d6 / 720 * (61 + 90 * p_tan2 + 298 * c + 45 * p_tan4 - 252 * E_P2 - 3 * c2))

    longitude = (d -
                 d3 / 6 * (1 + 2 * p_tan2 + c) +
                 d5 / 120 * (5 - 2 * c + 28 * p_tan2 - 3 * c2 + 8 * E_P2 + 24 * p_tan4)) / p_cos

    longitude = mod_angle(longitude + np.radians(zone_number_to_central_longitude(zone_number)))

    return (np.degrees(latitude),
            np.degrees(longitude))


def latlng_to_utm(latitude, longitude, force_zone_number=None, force_zone_letter=None):
    K0 = 0.9996

    E = 0.00669438
    E2 = E * E
    E3 = E2 * E
    E_P2 = E / (1 - E)

    SQRT_E = np.sqrt(1 - E)
    _E = (1 - SQRT_E) / (1 + SQRT_E)
    _E2 = _E * _E
    _E3 = _E2 * _E
    _E4 = _E3 * _E
    _E5 = _E4 * _E

    M1 = (1 - E / 4 - 3 * E2 / 64 - 5 * E3 / 256)
    M2 = (3 * E / 8 + 3 * E2 / 32 + 45 * E3 / 1024)
    M3 = (15 * E2 / 256 + 45 * E3 / 1024)
    M4 = (35 * E3 / 3072)
    R = 6378137

    if not in_bounds(latitude, -80, 84):
        raise ValueError('latitude out of range (must be between 80 deg S and 84 deg N)')
    if not in_bounds(longitude, -180, 180):
        raise ValueError('longitude out of range (must be between 180 deg W and 180 deg E)')
    if force_zone_number is not None:
        check_valid_zone(force_zone_number, force_zone_letter)

    lat_rad = np.radians(latitude)
    lat_sin = np.sin(lat_rad)
    lat_cos = np.cos(lat_rad)

    lat_tan = lat_sin / lat_cos
    lat_tan2 = lat_tan * lat_tan
    lat_tan4 = lat_tan2 * lat_tan2

    if force_zone_number is None:
        zone_number = latlon_to_zone_number(latitude, longitude)
    else:
        zone_number = force_zone_number

    if force_zone_letter is None:
        zone_letter = latitude_to_zone_letter(latitude)
    else:
        zone_letter = force_zone_letter

    lon_rad = np.radians(longitude)
    central_lon = zone_number_to_central_longitude(zone_number)
    central_lon_rad = np.radians(central_lon)

    n = R / np.sqrt(1 - E * lat_sin ** 2)
    c = E_P2 * lat_cos ** 2

    a = lat_cos * mod_angle(lon_rad - central_lon_rad)
    a2 = a * a
    a3 = a2 * a
    a4 = a3 * a
    a5 = a4 * a
    a6 = a5 * a

    m = R * (M1 * lat_rad -
             M2 * np.sin(2 * lat_rad) +
             M3 * np.sin(4 * lat_rad) -
             M4 * np.sin(6 * lat_rad))

    easting = K0 * n * (a +
                        a3 / 6 * (1 - lat_tan2 + c) +
                        a5 / 120 * (5 - 18 * lat_tan2 + lat_tan4 + 72 * c - 58 * E_P2)) + 500000

    northing = K0 * (m + n * lat_tan * (a2 / 2 +
                                        a4 / 24 * (5 - lat_tan2 + 9 * c + 4 * c ** 2) +
                                        a6 / 720 * (61 - 58 * lat_tan2 + lat_tan4 + 600 * c - 330 * E_P2)))

    if mixed_signs(latitude):
        raise ValueError("latitudes must all have the same sign")
    elif negative(latitude):
        northing += 10000000

    return easting, northing, zone_number, zone_letter


def in_bounds(x, lower, upper, upper_strict=False):
    if upper_strict:
        return lower <= np.min(x) and np.max(x) < upper
    else:
        return lower <= np.min(x) and np.max(x) <= upper


def check_valid_zone(zone_number, zone_letter):
    if not 1 <= zone_number <= 60:
        raise ValueError('zone number out of range (must be between 1 and 60)')

    if zone_letter:
        zone_letter = zone_letter.upper()

        if not 'C' <= zone_letter <= 'X' or zone_letter in ['I', 'O']:
            raise ValueError('zone letter out of range (must be between C and X)')


def mixed_signs(x):
    return np.min(x) < 0 and np.max(x) >= 0


def negative(x):
    return np.max(x) < 0


def mod_angle(value):
    """Returns angle in radians to be between -pi and pi"""
    return (value + np.pi) % (2 * np.pi) - np.pi


def latitude_to_zone_letter(latitude):
    zone_letters = "CDEFGHJKLMNPQRSTUVWXX"
    if isinstance(latitude, np.ndarray):
        latitude = latitude.flat[0]

    if -80 <= latitude <= 84:
        return zone_letters[int(latitude + 80) >> 3]
    else:
        return None


def latlon_to_zone_number(latitude, longitude):
    if isinstance(latitude, np.ndarray):
        latitude = latitude.flat[0]
    if isinstance(longitude, np.ndarray):
        longitude = longitude.flat[0]

    if 56 <= latitude < 64 and 3 <= longitude < 12:
        return 32

    if 72 <= latitude <= 84 and longitude >= 0:
        if longitude < 9:
            return 31
        elif longitude < 21:
            return 33
        elif longitude < 33:
            return 35
        elif longitude < 42:
            return 37

    return int((longitude + 180) / 6) + 1


def zone_number_to_central_longitude(zone_number):
    return (zone_number - 1) * 6 - 180 + 3


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


class MitgcmGrid:
    """Class representing an MITgcm grid, with optional loading from .npy files."""

    def __init__(self):
        """Initialize an empty MITgcm grid."""
        self.x = np.array([])
        self.y = np.array([])
        self.lat_grid = np.array([])
        self.lon_grid = np.array([])
        self.dz = np.array([])
        self.parameters = {}

    def load_from_path(self, path_grid: str):
        """
        Load grid data from a given folder containing .npy files.

        Args:
            path_grid (str): Path to the folder containing the grid files.

        Raises:
            FileNotFoundError: If any required grid file is missing.
            RuntimeError: If loading fails due to other errors.
        """
        try:
            self.x = np.load(os.path.join(path_grid, 'x.npy'))
            self.y = np.load(os.path.join(path_grid, 'y.npy'))
            self.lat_grid = np.load(os.path.join(path_grid, 'lat_grid.npy'))
            self.lon_grid = np.load(os.path.join(path_grid, 'lon_grid.npy'))
            self.dz = pd.read_csv(os.path.join(path_grid, 'dz.csv'), header=None).to_numpy()
            with open(os.path.join(path_grid, 'parameters.json'), 'r') as file:
                self.parameters = json.load(file)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Missing grid file: {e.filename}") from e
        except Exception as e:
            raise RuntimeError(f"Error loading grid data: {e}") from e


def get_mitgcm_grid(path_folder_grid: str) -> MitgcmGrid:
    grid = MitgcmGrid()
    grid.load_from_path(path_folder_grid)
    return grid

def modify_arguments(param_name: str, values: np.array, file_path):
    """
    Function to modify run-time parameters, based on variable name, with the
    assumption that they are stored the file as '!varName!'
      param_name  - parameter name that should be replaced in the config file
      values - values replacing the param_name in the config file
      fileIn  - 00-template_mitgcm configuration
      fileOut - output run-time configuration
    """

    with open(file_path, 'r') as infile:
        content = infile.read()

    str_values = ''
    if len(values) > 1:
        for row in values:
            for val in row:
                if val != np.nan:
                    str_values += str(str(val) + ',')
            str_values += '\n'
    else:
        str_values = str(values[0])

    modified_content = content.replace(param_name, str_values)

    with open(file_path, 'w') as outfile:
        outfile.write(modified_content)

def calculate_specific_humidity(temp, relhum, atm_press):
    """
    Compute specific humidity from air temperature, relative humidity and atmospheric pressure.

    - temp: Air temperature in Kelvin
    - relhum: Relative humidity in %
    - atm_press: Atmospheric pressure in Pascal
    """
    # temp needs to be in celcius
    temp = temp - 273.15
    # atmospheric pressure should be in hPa
    atm_press = atm_press / 100.0
    # saturation vapour pressure (e_s)
    e_s = 6.112 * np.exp((17.67 * temp) / (temp + 243.5))
    # actual vapour pressure (e)
    e = (relhum / 100) * e_s
    # Step 3: Calculate the specific humidity (q)
    q = (0.622 * e) / (atm_press - (0.378 * e))

    return (q)


def compute_vapor_pressure(atemp, relhum):
    """
    Calculate vapor pressure using the Magnus formula.

    Parameters:
    - temperature (float or numpy array): Air temperature in Kelvin
    - relhum (float or numpy array): Relative humidity as a percentage (e.g., 60 for 60%)

    Returns:
    - vapor_pressure (float or numpy array): Vapor pressure in hPa (hectopascals)
    """
    a = 17.27
    b = 237.7

    rh_fraction = relhum / 100.0
    atemp_celsius = atemp-273.15

    saturation_vapor_pressure = 6.112 * np.exp((a * atemp_celsius) / (atemp_celsius + b))

    return rh_fraction * saturation_vapor_pressure  # vapor pressure in mbar (=hPa)


def compute_longwave_radiation(atemp, relhum, cloud_cover):
    """
    Compute longwave radiation from air temperature and cloud cover.

    - temp: Air temperature in Kelvin
    - cloud_cover: %
    """
    # cloud cover should be from 0 to 1
    cloud_cover = cloud_cover/100
    vaporPressure = compute_vapor_pressure(atemp, relhum)  # in units mbar
    A_L = 0.03   # Infrared radiation albedo
    a = 1.09     # Calibration parameter

    E_a = a * (1 + 0.17 * np.power(cloud_cover, 2)) * 1.24 * np.power(vaporPressure / atemp, 1./7)  # emissivity

    lwr = (1 - A_L) * 5.67e-8 * E_a * np.power(atemp, 4)
    return lwr