import os
import boto3
import math
from botocore.exceptions import ClientError
import shutil
import logging
import traceback
import numpy as np
import pandas as pd
from requests import get
from datetime import datetime, timedelta, timezone


def convert_to_unit(time, units):
    if units == "seconds since 2008-03-01 00:00:00":
        return (time.replace(tzinfo=timezone.utc) - datetime(2008, 3, 1).replace(tzinfo=timezone.utc)).total_seconds()
    elif units == "seconds since 1970-01-01 00:00:00":
        return time.timestamp()
    else:
        raise ValueError("Unrecognised time unit.")


def convert_from_unit(time, units):
    if units == "seconds since 2008-03-01 00:00:00":
        return datetime.utcfromtimestamp(time + (datetime(2008, 3, 1).replace(tzinfo=timezone.utc) - datetime(1970, 1, 1).replace(tzinfo=timezone.utc)).total_seconds())
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

    c = E_P2 * p_cos**2
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

    n = R / np.sqrt(1 - E * lat_sin**2)
    c = E_P2 * lat_cos**2

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
                                        a4 / 24 * (5 - lat_tan2 + 9 * c + 4 * c**2) +
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