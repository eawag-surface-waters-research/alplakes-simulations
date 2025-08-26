# -*- coding: utf-8 -*-
import os
import json
import shutil
import subprocess
import numpy as np
import pandas as pd
import xarray as xr
from distutils.dir_util import copy_tree
from datetime import datetime, timedelta

import river
import secchi
import weather
from functions import logger, ch1903_to_latlng, download_file, upload_file, utm_to_latlng, get_mitgcm_grid, modify_arguments, calculate_specific_humidity, compute_longwave_radiation, overwrite_defaults


class Delft3D(object):
    def __init__(self, params):
        self.params = params
        self.properties = {}
        self.simulation_dir = ""
        self.restart_file = ""
        self.profile = "None"
        self.files = [
            {"filename": 'CloudCoverage.amc', "parameter": "CLCT", "quantity": "cloudiness", "unit": "%", "adjust": 0,
             "min": 0, "max": 100},
            {"filename": 'Pressure.amp', "parameter": "PMSL", "quantity": "air_pressure", "unit": "Pa", "adjust": 0,
             "min": 87000, "max": 108560},
            {"filename": 'RelativeHumidity.amr', "parameter": "RELHUM_2M", "quantity": "relative_humidity", "unit": "%",
             "adjust": 0, "min": 0, "max": 100},
            {"filename": 'ShortwaveFlux.ams', "parameter": "GLOB", "quantity": "sw_radiation_flux", "unit": "W/m2",
             "adjust": 0, "min": 0, "max": 1361},
            {"filename": 'Temperature.amt', "parameter": "T_2M", "quantity": "air_temperature", "unit": "Celsius",
             "adjust": -273.15},
            {"filename": 'WindU.amu', "parameter": "U", "quantity": "x_wind", "unit": "m s-1", "adjust": 0},
            {"filename": 'WindV.amv', "parameter": "V", "quantity": "y_wind", "unit": "m s-1", "adjust": 0},
        ]

        if "log" in params and params["log"]:
            log_prefix = "{}_{}_{}".format(params["model"].replace("/", "_"), params["start"], params["end"])
            self.log = logger(path=os.path.join(params["log"], log_prefix))
        else:
            self.log = logger()

        if "model" in params and "docker" in params:
            self.log.initialise("Writing input files for simulation {} using {}".format(params["model"], params["docker"]))

        self.log.info("Creating input files from {} to {}".format(params["start"], params["end"] - timedelta(seconds=1)))

    def process(self):
        self.initialise_simulation_directory()
        self.copy_static_data()
        self.collect_restart_file()
        self.load_properties()
        self.update_control_file()
        self.weather_data_files()
        self.secchi_data_files()
        self.river_data_files()
        if self.params["upload"]:
            self.upload_data()
        if self.params["run"]:
            self.run_simulation()
        return self.simulation_dir

    def initialise_simulation_directory(self, remove=True):
        try:
            self.log.begin_stage("Initialising simulation directory.")
            name = "{}_{}_{}_{}".format(self.params["docker"], self.params["model"],
                                        self.params["start"].strftime("%Y%m%d"), self.params["end"].strftime("%Y%m%d"))
            name = name.replace("/", "_").replace(".", "").replace(":", "").replace("-", "")
            self.simulation_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../runs", name)
            self.log.info("Simulation directory: {}".format(self.simulation_dir), indent=1)
            if os.path.isdir(self.simulation_dir) and remove:
                self.log.info("Removing existing simulation simulation directory.", indent=1)
                shutil.rmtree(self.simulation_dir)
            if not os.path.isdir(self.simulation_dir):
                os.mkdir(self.simulation_dir)
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def copy_static_data(self):
        try:
            self.log.begin_stage("Copying static data to simulation folder.")
            parent_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
            static = os.path.join(parent_dir, "static", self.params["model"])
            files = copy_tree(static, self.simulation_dir)
            for file in files:
                self.log.info("Copied {} to simulation folder.".format(os.path.basename(file)), indent=1)
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def collect_restart_file(self, region="eu-central-1"):
        try:
            self.log.begin_stage("Collecting restart file.")
            self.restart_file = "tri-rst.Simulation_Web_rst.{}.000000".format(self.params["start"].strftime("%Y%m%d"))
            components = self.params["model"].split("/")
            if self.params["profile"]:
                self.log.info("Using profile ({}), no restart file required.".format(self.params["profile"]), indent=1)
                if ".txt" not in self.params["profile"]:
                    self.params["profile"] = self.params["profile"] + ".txt"
                if not os.path.exists(os.path.join(self.simulation_dir, "profiles", self.params["profile"])):
                    raise ValueError('Specified profile "{}" cannot be found, select from {}'.format(self.params["profile"], os.listdir(os.path.join(self.simulation_dir, "profiles"))))
                self.profile = self.params["profile"]
                self.log.end_stage()
                return
            if self.params["restart"]:
                self.log.info("Copying restart file from local storage.", indent=1)
                file = self.params["restart"]
                if os.path.isfile(file):
                    shutil.copyfile(file, os.path.join(self.simulation_dir, self.restart_file))
                    self.log.end_stage()
                    return
                else:
                    raise ValueError("Unable to locate restart file: ".format(file))
            self.log.info("Downloading restart file from remote storage.", indent=1)
            if not self.params["bucket"]:
                raise ValueError("No bucket address provided, either include local files or specify a bucket.")
            bucket = "https://{}.s3.{}.amazonaws.com".format(self.params["bucket"], region)
            file = os.path.join(bucket, "simulations", components[0], "restart-files", components[1], self.restart_file)
            self.log.info("File location: {}".format(file), indent=2)
            status_code = download_file(file, os.path.join(self.simulation_dir, self.restart_file))
            if status_code == 200:
                self.log.info("Successfully downloaded restart file.", indent=2)
            elif status_code == 403:
                self.log.warning("Restart file doesn't exist on server.", indent=1)
                if os.path.exists(os.path.join(self.simulation_dir, "profiles", "default.txt")):
                    self.log.warning("Using default restart profile", indent=1)
                    self.profile = "default.txt"
                else:
                    raise ValueError("Not restart file and no default restart profile.")
            else:
                raise ValueError("Unable to download restart file, please check your internet connection.")
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def load_properties(self, manual=False):
        try:
            self.log.begin_stage("Loading properties.")
            if manual:
                self.properties = manual
            else:
                with open(os.path.join(self.simulation_dir, "properties.json"), 'r') as f:
                    self.properties = json.load(f)
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def update_control_file(self, origin=datetime(2008, 3, 1), period=180):
        try:
            self.log.begin_stage("Updating control file dates.")
            self.log.info("Reading simulation file.", indent=1)
            with open(os.path.join(self.simulation_dir, "Simulation_Web.mdf"), 'r') as f:
                lines = f.readlines()
            start = "{:.7e}".format((self.params["start"] - origin).total_seconds() / 60)
            end = "{:.7e}".format(((self.params["end"] - origin).total_seconds() / 60) - period)
            restid_idx = 0
            for i in range(len(lines)):
                if "Restid" in lines[i]:
                    lines[i] = "Restid = #" + self.restart_file.replace("tri-rst.", "") + "#\n"
                    restid_idx = i
                if "Tstart" in lines[i]:
                    lines[i] = "Tstart = " + start + "\n"
                if "Tstop" in lines[i]:
                    lines[i] = "Tstop = " + end + "\n"
                if lines[i].split(" ")[0] in ["Flmap", "Flhis", "Flwq"]:
                    lines[i] = "{} = {} {} {}\n".format(lines[i].split(" ")[0], start, str(period), end)

            if self.profile != "None":
                self.log.info("Reading profile from: {}".format(self.profile), indent=2)
                with open(os.path.join(self.simulation_dir, "profiles", self.profile), 'r') as f:
                    T0 = f.readlines()
                lines = lines[:restid_idx] + T0 + lines[restid_idx + 1:]

            self.log.info("Writing updated simulation file.", indent=1)
            with open(os.path.join(self.simulation_dir, "Simulation_Web.mdf"), 'w') as f:
                f.writelines(lines)

            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def weather_data_files(self, buffer=10, no_data_value="-999.00"):
        try:
            self.log.begin_stage("Creating weather data files.")

            self.log.info("Creating the meteo grid", indent=1)
            grid = self.properties["grid"]
            if "system" not in grid:
                raise ValueError("System must be defined.")
            minx, miny, maxx, maxy, system = grid["minx"], grid["miny"], grid["maxx"], grid["maxy"], grid["system"]
            gx = np.arange(minx, maxx + grid["dx"], grid["dx"])
            gy = np.arange(miny, maxy + grid["dy"], grid["dy"])
            gxx, gyy = np.meshgrid(gx, gy)

            self.log.info("Define buffer region to fill grid", indent=1)
            minx = minx - buffer * grid["dx"]
            miny = miny - buffer * grid["dy"]
            maxx = maxx + buffer * grid["dx"]
            maxy = maxy + buffer * grid["dy"]

            self.log.info("Initialise the output meteo files and write their headers", indent=1)
            for i in range(len(self.files)):
                with open(os.path.join(self.simulation_dir, self.files[i]["filename"]), "w") as f:
                    f.write('FileVersion = 1.03')
                    f.write('\nfiletype = meteo_on_equidistant_grid')
                    f.write('\nNODATA_value = ' + str(no_data_value))
                    f.write('\nn_cols = ' + str(len(gx)))
                    f.write('\nn_rows = ' + str(len(gy)))
                    f.write('\ngrid_unit = m')
                    f.write('\nx_llcenter = ' + str(gx[0]))
                    f.write('\ny_llcenter = ' + str(gy[0]))
                    f.write('\ndx = ' + str(grid["dx"]))
                    f.write('\ndy = ' + str(grid["dy"]))
                    f.write('\nn_quantity = 1')
                    f.write('\nquantity1 = ' + self.files[i]["quantity"])
                    f.write('\nunit1 = ' + self.files[i]["unit"] + '\n')

            if system == "WGS84":
                self.log.info("Grid using WGS84 coordinate system.", indent=1)
            elif system == "CH1903":
                self.log.info("Grid using ch1903 coordinate system, converting to WGS84 to collect weather data.", indent=1)
                minx, miny = ch1903_to_latlng(minx, miny)
                maxx, maxy = ch1903_to_latlng(maxx, maxy)
            elif system == "UTM":
                if "zone_letter" not in grid or "zone_number" not in grid:
                    raise ValueError("zone_letter and zone_number must be defined in grid with using UTM")
                minx, miny = utm_to_latlng(minx, miny, grid["zone_number"], grid["zone_letter"])
                maxx, maxy = utm_to_latlng(maxx, maxy, grid["zone_number"], grid["zone_letter"])

            self.log.info("Collecting weather data for region: [{}, {}] [{}, {}]".format(minx, miny, maxx, maxy), indent=1)
            self.log.info("Writing weather data to simulation files.", indent=1)
            variables = [file["parameter"] for file in self.files]
            days = [self.params["start"]+timedelta(days=x) for x in range((min(self.params["today"], self.params["end"]) - self.params["start"]).days+1)]
            for day in days:
                self.log.info("Collecting data for {} from remote API.".format(day), indent=2)
                if day >= datetime(2024, 7, 30):
                    data = weather.download_meteolakes_icon_area(minx, miny, maxx, maxy, day, variables, self.params["api"], self.params["today"])
                else:
                    data = weather.download_meteolakes_cosmo_area(minx, miny, maxx, maxy, day, variables, self.params["api"], self.params["today"])
                for file in self.files:
                    self.log.info("Processing parameter " + file["parameter"], indent=3)
                    weather.write_weather_data_to_file(data["time"], data["variables"][file["parameter"]]["data"], data["lat"], data["lng"], gxx, gyy, system, file, self.simulation_dir, no_data_value, warning=self.log.warning)

            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def secchi_data_files(self, no_data_value="-999.00"):
        try:
            self.log.begin_stage("Creating secchi data file.")
            if "secchi" not in self.properties:
                self.log.warning("No secchi params specified, skipping stage.", indent=1)
            else:
                self.log.info("Creating the secchi grid", indent=1)
                grid = self.properties["grid"]
                minx, miny, maxx, maxy = grid["minx"], grid["miny"], grid["maxx"], grid["maxy"]
                gx = np.arange(minx, maxx + grid["dx"], grid["dx"])
                gy = np.arange(miny, maxy + grid["dy"], grid["dy"])
                scaling_factor = 1 if "scaling" not in self.properties["secchi"] else float(
                    self.properties["secchi"]["scaling"])

                self.log.info("Initialise the output secchi file and write the header", indent=1)
                file = os.path.join(self.simulation_dir, "Secchi.scc")
                with open(file, "w") as f:
                    f.write('FileVersion = 1.03')
                    f.write('\nfiletype = meteo_on_equidistant_grid')
                    f.write('\nNODATA_value = ' + str(no_data_value))
                    f.write('\nn_cols = ' + str(len(gx)))
                    f.write('\nn_rows = ' + str(len(gy)))
                    f.write('\ngrid_unit = m')
                    f.write('\nx_llcenter = ' + str(gx[0]))
                    f.write('\ny_llcenter = ' + str(gy[0]))
                    f.write('\ndx = ' + str(grid["dx"]))
                    f.write('\ndy = ' + str(grid["dy"]))
                    f.write('\nn_quantity = 1')
                    f.write('\nquantity1 = Secchi_depth')
                    f.write('\nunit1 = m' + '\n')

                if "monthly" in self.properties["secchi"]:
                    self.log.info("Writing fixed value for secchi depth", indent=1)
                    secchi.write_monthly_secchi_to_file(file, self.properties["secchi"]["monthly"], scaling_factor, self.params["start"], self.params["end"], len(gx), len(gy))
                elif "fixed" in self.properties["secchi"]:
                    self.log.info("Writing fixed value for secchi depth", indent=1)
                    secchi.write_fixed_secchi_to_file(file, self.properties["secchi"]["fixed"], scaling_factor, self.params["start"], self.params["end"], len(gx), len(gy))
                else:
                    raise ValueError("No method specified for generating secchi input file.")
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def river_data_files(self, pre_days=7, post_days=2):
        try:
            self.log.begin_stage("Creating river data file.")
            if "rivers" not in self.properties:
                self.log.warning("No river params specified, skipping stage.", indent=1)
            else:
                start = self.params["start"] - timedelta(days=pre_days)
                end = self.params["end"]
                if self.params["end"].strftime("%Y%m%d") == self.params["today"].strftime("%Y%m%d") or self.params["end"] > self.params["today"]:
                    self.log.info("Requested timeframe exceeds available data, data from {} to {} will be forecast."
                                  .format(self.params["today"].strftime("%Y%m%d"), self.params["end"].strftime("%Y%m%d")), indent=1)
                    forecast = True
                else:
                    if self.params["end"] + timedelta(days=post_days) < self.params["today"]:
                        end = self.params["end"] + timedelta(days=post_days)
                    forecast = False

                self.log.info("Generating empty arrays", indent=1)
                self.properties = river.empty_arrays(self.properties, self.params["start"], self.params["end"])

                self.log.info("Collecting river data from {} to {}".format(start, end), indent=1)
                self.properties = river.download_bafu_hydrodata(self.properties, start, end, self.params["api"], self.params["today"], log=self.log)

                self.log.info("Cleaning, smoothing and resampling downloaded data.", indent=1)
                self.properties = river.clean_smooth_resample(self.properties, start, end, log=self.log)

                if forecast:
                    self.log.info("Forcasting beyond measured data.", indent=1)
                    self.properties = river.forecast(self.properties, log=self.log)

                self.log.info("Map station data to rivers and compute flow balance.", indent=1)
                self.properties = river.flow_balance(self.properties, self.simulation_dir, log=self.log)

                self.log.info("Write river data to files.", indent=1)
                self.properties = river.write_river_data_to_file(self.properties, self.simulation_dir)
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def upload_data(self):
        try:
            self.log.begin_stage("Uploading simulation inputs to S3 bucket.")
            self.log.info("Zipping simulation folder.", indent=1)
            zipfile = self.simulation_dir + ".zip"
            if os.path.isfile(zipfile):
                self.log.info("Removing existing zip file.", indent=2)
                os.remove(zipfile)
            shutil.make_archive(self.simulation_dir, 'zip', self.simulation_dir)
            self.log.info("Uploading file to bucket.", indent=1)
            upload_path = os.path.join("simulations",
                                       self.params["model"].split("/")[0],
                                       "simulation-files",
                                       os.path.basename(zipfile))
            upload_file(zipfile, self.params["bucket"], object_name=upload_path)
            self.log.info("Removing zip file.", indent=1)
            os.remove(zipfile)
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def run_simulation(self):
        try:
            self.log.begin_stage("Running simulation.")
            self.log.info("Running simulation as a subprocess.", indent=1)
            print("-v {}:/job".format(self.simulation_dir))
            process = subprocess.Popen(["docker", "run", "-v", "{}:/job".format(self.simulation_dir),
                                        self.docker],
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       universal_newlines=True,
                                       cwd=self.simulation_dir)
            error = self.log.subprocess(process, error="Flow exited abnormally")
            if process.returncode != 0:
                output, error = process.communicate()
                raise RuntimeError("Subprocess failed with the following error: {}".format(error))
            elif error:
                raise RuntimeError("Simulation failed check the logs for more information.")
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise


class delft3d_flow_501002163(Delft3D):
    def __init__(self, *args, **kwargs):
        super(delft3d_flow_501002163, self).__init__(*args, **kwargs)
        self.version = "5.01.00.2163"
        self.docker = "eawag/delft3d-flow:5.01.00.2163"


class delft3d_flow_6030062434(Delft3D):
    def __init__(self, *args, **kwargs):
        super(delft3d_flow_6030062434, self).__init__(*args, **kwargs)
        self.version = "6.03.00.62434"
        self.docker = "eawag/delft3d-flow:6.03.00.62434"


class delft3d_flow_60210142612(Delft3D):
    def __init__(self, *args, **kwargs):
        super(delft3d_flow_60210142612, self).__init__(*args, **kwargs)
        self.version = "6.02.10.142612"
        self.docker = "eawag/delft3d-flow:6.02.10.142612"


class MitGCM(object):
    def __init__(self, params):
        self.params = params
        self.properties = {}
        self.simulation_dir = ""
        self.restart_id = False
        self.profile = False
        self.grid = ""
        self.initial_temperature = np.array([0])
        self.initial_salinity = np.array([0])
        self.default_salinity = 0.050

        if "log" in params and params["log"]:
            log_prefix = "{}_{}_{}".format(params["model"].replace("/", "_"), params["start"], params["end"])
            self.log = logger(path=os.path.join(params["log"], log_prefix))
        else:
            self.log = logger()

        if "model" in params and "docker" in params:
            self.log.initialise("Writing input files for simulation {} using {}".format(params["model"], params["docker"]))

        self.log.info("Creating input files from {} to {}".format(params["start"], params["end"] - timedelta(seconds=1)))

    def process(self):
        self.initialise_simulation_directory()
        self.copy_static_data()
        self.load_properties()
        self.load_grid()
        self.collect_restart_file()
        self.initial_conditions()
        self.update_control_files()
        self.weather_data_files()
        if self.params["upload"]:
            self.upload_data()
        if self.params["run"]:
            self.run_simulation()
        return self.simulation_dir

    def initialise_simulation_directory(self, remove=True):
        try:
            self.log.begin_stage("Initialising simulation directory.")
            name = "{}_{}_{}_{}_{}".format(self.params["docker"], self.params["model"],
                                        self.params["start"].strftime("%Y%m%d"), self.params["end"].strftime("%Y%m%d"), self.params["threads"])
            name = name.replace("/", "_").replace(".", "").replace(":", "").replace("-", "")
            self.simulation_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../runs", name)
            self.log.info("Simulation directory: {}".format(self.simulation_dir), indent=1)
            if os.path.isdir(self.simulation_dir) and remove:
                self.log.info("Removing existing simulation simulation directory.", indent=1)
                shutil.rmtree(self.simulation_dir)
            if not os.path.isdir(self.simulation_dir):
                os.mkdir(self.simulation_dir)
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def copy_static_data(self):
        try:
            self.log.begin_stage("Copying static data to simulation folder.")
            parent_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
            self.log.info("Copying default files to simulation folder.", indent=1)
            default = os.path.join(parent_dir, "static", self.params["model"].split("/")[0], "default")
            files = copy_tree(default, self.simulation_dir)
            for file in files:
                self.log.info("Copied {} to simulation folder.".format(os.path.basename(file)), indent=2)
            self.log.info("Copying model files to simulation folder.", indent=1)
            model = os.path.join(parent_dir, "static", self.params["model"])
            files = copy_tree(model, self.simulation_dir)
            for file in files:
                self.log.info("Copied {} to simulation folder.".format(os.path.basename(file)), indent=2)
            os.makedirs(os.path.join(self.simulation_dir, "run"), exist_ok=True)
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def collect_restart_file(self, region="eu-central-1"):
        try:
            self.log.begin_stage("Collecting restart files.")
            components = self.params["model"].split("/")
            restart_file = "pickup.{}.data".format(self.params["start"].strftime("%Y%m%d"))
            if self.params["profile"]:
                self.log.info("Using profile ({}), no restart file required.".format(self.params["profile"]), indent=1)
                if ".csv" not in self.params["profile"]:
                    self.params["profile"] = self.params["profile"] + ".csv"
                if not os.path.exists(os.path.join(self.simulation_dir, "profiles", self.params["profile"])):
                    raise ValueError('Specified profile "{}" cannot be found, select from {}'.format(self.params["profile"], os.listdir(os.path.join(self.simulation_dir, "profiles"))))
                self.profile = self.params["profile"]
                self.log.end_stage()
                return
            if self.params["restart"]:
                self.log.info("Copying restart files from local storage.", indent=1)
                data_file = self.params["restart"] + ".data"
                meta_file = self.params["restart"] + ".meta"
                if not os.path.isfile(data_file):
                    raise ValueError("Unable to locate restart data file: ".format(data_file))
                if not os.path.isfile(meta_file):
                    raise ValueError("Unable to locate restart meta file: ".format(meta_file))
                shutil.copyfile(data_file, os.path.join(self.simulation_dir, "run", restart_file))
                shutil.copyfile(meta_file, os.path.join(self.simulation_dir, "run", restart_file.replace(".data", ".meta")))
                self.log.end_stage()
                return
            self.log.info("Downloading restart file from remote storage.", indent=1)
            if not self.params["bucket"]:
                raise ValueError("No bucket address provided, either include local files or specify a bucket.")
            bucket = "https://{}.s3.{}.amazonaws.com".format(self.params["bucket"], region)
            file = os.path.join(bucket, "simulations", components[0], "restart-files", components[1], restart_file)
            self.log.info("File location: {}".format(file), indent=2)
            status_code1 = download_file(file, os.path.join(self.simulation_dir, "run", restart_file))
            status_code2 = download_file(file.replace(".data", ".meta"), os.path.join(self.simulation_dir, "run", restart_file.replace(".data", ".meta")))
            if status_code1 == 200 and status_code2 == 200:
                self.restart_id = self.params["start"].strftime("%Y%m%d")
                self.log.info("Successfully downloaded restart files.", indent=2)
            elif status_code1 == 403 or status_code2 == 403:
                self.log.warning("Restart files doesn't exist on server.", indent=1)
                self.log.warning("Using default 4 degree starting temperature", indent=1)
            else:
                raise ValueError("Unable to download restart file, please check your internet connection.")
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def load_properties(self, manual=False):
        try:
            self.log.begin_stage("Loading properties.")
            if manual:
                self.properties = manual
            else:
                with open(os.path.join(self.simulation_dir, "properties.json"), 'r') as f:
                    self.properties = json.load(f)
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def load_grid(self):
        self.log.begin_stage("Loading grid.")
        self.grid = get_mitgcm_grid(os.path.join(self.simulation_dir, "grid"))
        self.log.end_stage()

    def initial_conditions(self):
        self.log.begin_stage("Setting initial conditions.")
        if self.profile:
            self.log.info("Using profile {} for initial conditions".format(self.profile), indent=1)
            df = pd.read_csv(os.path.join(self.simulation_dir, "profiles", self.profile))
            df = df.set_index('depth').to_xarray()
            z_grid = np.cumsum(self.grid.dz)
            df_i = df.interp(depth=z_grid, method='linear')
            df_f = df_i.ffill(dim='depth').bfill(dim='depth')
            profile = np.round(df_f['temperature'].values, 3)
        else:
            profile = np.full(self.grid.dz.shape, 4.0)
        self.initial_temperature = profile

        if "salinity" in self.properties:
            self.initial_salinity = np.full(self.grid.dz.shape, self.properties["salinity"])
        else:
            self.initial_salinity = np.full(self.grid.dz.shape, self.default_salinity)

        self.log.end_stage()

    def update_control_files(self, origin=datetime(2008, 6, 1)):
        # Origin must be a Sunday as restart files are based on origin not on start time
        try:
            self.log.begin_stage("Updating control files.")
            start_time_in_second_from_ref_date = (self.params["start"] - origin).total_seconds()
            end_time_in_second_from_ref_date = (self.params["end"] - origin).total_seconds()

            self.log.info("Editing run_config/data.cal", indent=1)
            modify_arguments('!reference_date!', origin.strftime('%Y%m%d'), os.path.join(self.simulation_dir, 'run_config/data.cal'))

            self.log.info("Editing run_config/data.exf", indent=1)
            file_path = os.path.join(self.simulation_dir, 'run_config/data.exf')
            modify_arguments('!start_date!', self.params["start"].strftime('%Y%m%d'), file_path)
            if "overwrite" in self.properties and "data.exf" in self.properties["overwrite"]:
                overwrite_defaults(self.properties["overwrite"]["data.exf"], file_path)

            self.log.info("Editing run_config/data", indent=1)
            file_path = os.path.join(self.simulation_dir, 'run_config/data')
            modify_arguments('!initial_temperature!', self.initial_temperature, file_path)
            modify_arguments('!initial_salinity!', self.initial_salinity, file_path)
            modify_arguments('!start_time!', start_time_in_second_from_ref_date, file_path)
            modify_arguments('!end_time!', end_time_in_second_from_ref_date, file_path)
            if self.restart_id:
                modify_arguments('!pickup_number!', self.restart_id, file_path)
            else:
                modify_arguments('!pickup_number!', "", file_path)
            modify_arguments('!grid_resolution!', self.grid.parameters["resolution"], file_path)
            modify_arguments('!time_step!', self.properties["timestep"], file_path)
            modify_arguments('!dz_grid!', self.grid.dz, file_path)
            if "overwrite" in self.properties and "data" in self.properties["overwrite"]:
                overwrite_defaults(self.properties["overwrite"]["data"], file_path)

            threads = self.params["threads"]
            Nx = self.grid.parameters["Nx"]
            Ny = self.grid.parameters["Ny"]
            nPxy = str(threads).split("_")
            if len(nPxy) > 1:
                self.log.info("Using multiple threads.", indent=1)
                nPx, nPy = int(nPxy[0]), int(nPxy[1])
            else:
                self.log.info("Single threaded, for multiple threads set the thread parameter to nPx_nPy", indent=1)
                nPx, nPy = 1, 1

            if Nx % nPx != 0 or Ny % nPy != 0:
                raise ValueError("Grid division must be an integer of number of cells.")
            sNx = int(Nx / nPx)
            sNy = int(Ny / nPy)

            exch2_path = os.path.join(self.simulation_dir, 'run_config/data.exch2')
            if os.path.isfile(exch2_path):
                self.log.info("Editing run_config/data.exch2. Using exch2 to avoid computing empty grid regions", indent=1)

                with open(os.path.join(self.simulation_dir, "binary_data", "bathy.bin"), 'rb') as fid:
                    bathy = np.fromfile(fid, dtype='>f8')

                bathy = np.reshape(bathy, (Ny, Nx))
                core_number = np.zeros(bathy.shape)
                mean_bathy = np.zeros(bathy.shape)
                for i in range(nPx):
                    for j in range(nPy):
                        core_number[j * int(sNy): (j + 1) * int(sNy), i * int(sNx): (i + 1) * int(sNx)] = int(
                            1 + i + j * int(nPx))
                        mean_bathy[j * int(sNy): (j + 1) * int(sNy), i * int(sNx): (i + 1) * int(sNx)] = np.mean(
                            bathy[j * int(sNy): (j + 1) * int(sNy), i * int(sNx): (i + 1) * int(sNx)])
                land_cores = sorted(set(core_number[mean_bathy >= 2]))

                nPx = (nPx * nPy) - len(land_cores)
                nPy = 1
                modify_arguments('!Nx!', Nx, exch2_path)
                modify_arguments('!Ny!', Ny, exch2_path)
                if len(land_cores) > 0:
                    modify_arguments('!blank_list!', f'blankList(1:{len(land_cores)})=   {",".join(map(str, map(int, land_cores)))},', exch2_path)
                else:
                    modify_arguments('!blank_list!', "", exch2_path)
                self.log.info("Ignoring {} chunks with no lake data, actually using {} cores".format(len(land_cores), nPx), indent=2)
            else:
                self.log.info("exch2 files not available, computing entire grid.", indent=1)

            self.log.info("Editing code/SIZE.h", indent=1)
            size_file = os.path.join(self.simulation_dir, "code/SIZE.h")
            Nr = np.count_nonzero(~np.isnan(self.grid.dz))
            modify_arguments('!nPx!', nPx, size_file)
            modify_arguments('!nPy!', nPy, size_file)
            modify_arguments('!Nx!', Nx, size_file)
            modify_arguments('!Ny!', Ny, size_file)
            modify_arguments('!Nr!', Nr, size_file)
            modify_arguments('!sNx!', sNx, size_file)
            modify_arguments('!sNy!', sNy, size_file)

            self.log.info("Editing code/swfrac.F", indent=1)
            if "secchi" not in self.properties or not isinstance(self.properties["secchi"], list):
                raise ValueError("A list of secchi values must be provided in properties.json")
            swfrac_file = os.path.join(self.simulation_dir, "code/swfrac.F")
            depths = "_RL secchiDepths({})".format(len(self.properties["secchi"]))
            secchi = "DATA secchiDepths / {} _d 0".format(self.properties["secchi"][0])
            for i in range(1, len(self.properties["secchi"])):
                secchi = secchi + ",\n     &                    {} _d 0".format(self.properties["secchi"][i])
            secchi = secchi + " /"

            modify_arguments('!depths!', depths, swfrac_file)
            modify_arguments('!secchi!', secchi, swfrac_file)

            self.log.info("Editing entrypoint.sh", indent=1)
            modify_arguments('!cores!', nPx * nPy, os.path.join(self.simulation_dir, "entrypoint.sh"))

            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def weather_data_files(self):
        try:
            self.log.begin_stage("Creating weather data files.")

            self.log.info("Creating the meteo grid", indent=1)
            minx, maxx = self.grid.lat_grid.min(), self.grid.lat_grid.max()
            miny, maxy = self.grid.lon_grid.min(), self.grid.lon_grid.max()
            buffer = self.grid.parameters["buffer"]
            if "endian_type" in self.properties:
                endian_type = self.properties["endian_type"]
            else:
                endian_type = ">f8"

            self.log.info("Define buffer region to fill grid", indent=1)
            minx = minx - buffer
            miny = miny - buffer
            maxx = maxx + buffer
            maxy = maxy + buffer

            self.log.info("Collecting weather data for region: [{}, {}] [{}, {}]".format(minx, miny, maxx, maxy), indent=1)
            variables = ['T_2M', 'U', 'V', 'GLOB', 'RELHUM_2M', 'PMSL', 'CLCT', 'PS']
            days = [self.params["start"]+timedelta(days=x) for x in range((min(self.params["today"], self.params["end"]) - self.params["start"]).days+1)]
            for day in days:
                self.log.info("Collecting data for {} from remote API.".format(day), indent=2)
                if day >= datetime(2024, 7, 30):
                    weather.download_meteolakes_icon_area(minx, miny, maxx, maxy, day, variables, self.params["api"], self.params["today"], download=os.path.join(self.simulation_dir, "weather"))
                else:
                    weather.download_meteolakes_cosmo_area(minx, miny, maxx, maxy, day, variables, self.params["api"], self.params["today"], download=os.path.join(self.simulation_dir, "weather"))

            self.log.info("Writing weather data to simulation files.", indent=1)
            binary_folder = os.path.join(self.simulation_dir, "binary_data")
            weather_folder = os.path.join(self.simulation_dir, "weather")
            os.makedirs(binary_folder, exist_ok=True)

            def process_variable(var_name, output_name, zero_nan_slice=False):
                self.log.info(f'Interpolating {var_name} to grid...', indent=2)
                data = weather.weather_files_to_grid(weather_folder, var_name, self.params["start"], self.params["end"], self.grid, 1, zero_nan_slice)
                weather.write_binary(os.path.join(binary_folder, f'{output_name}.bin'), data, endian_type=endian_type)
                return data

            process_variable('U', 'u10')
            process_variable('V', 'v10')
            process_variable('GLOB', 'swdown', zero_nan_slice=True)

            atemp = process_variable('T_2M', 'atemp')
            apress = process_variable('PS', 'apressure')

            self.log.info('Computing specific humidity (aqh)...', indent=2)
            relhum = process_variable('RELHUM_2M', 'relhum')
            aqh = calculate_specific_humidity(atemp, relhum, apress)
            weather.write_binary(os.path.join(binary_folder, 'aqh.bin'), aqh, endian_type=endian_type)

            self.log.info('Computing longwave radiation (lwdown)...', indent=2)
            clct = process_variable('CLCT', 'clct')
            lwr = compute_longwave_radiation(atemp, relhum, clct)
            weather.write_binary(os.path.join(binary_folder, 'lwdown.bin'), lwr, endian_type=endian_type)

            shutil.rmtree(weather_folder)
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def upload_data(self):
        try:
            self.log.begin_stage("Uploading simulation inputs to S3 bucket.")
            self.log.info("Zipping simulation folder.", indent=1)
            zipfile = self.simulation_dir + ".zip"
            if os.path.isfile(zipfile):
                self.log.info("Removing existing zip file.", indent=2)
                os.remove(zipfile)
            shutil.make_archive(self.simulation_dir, 'zip', self.simulation_dir)
            self.log.info("Uploading file to bucket.", indent=1)
            upload_path = os.path.join("simulations",
                                       self.params["model"].split("/")[0],
                                       "simulation-files",
                                       os.path.basename(zipfile))
            upload_file(zipfile, self.params["bucket"], object_name=upload_path)
            self.log.info("Removing zip file.", indent=1)
            os.remove(zipfile)
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def run_simulation(self):
        try:
            self.log.begin_stage("Running simulation.")
            self.log.info("Building docker container.", indent=1)
            docker = "{}_{}".format(self.docker, self.params["model"].split("/")[1])
            process = subprocess.Popen(
                ["docker", "build", "-t", docker, "."],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                cwd=self.simulation_dir,
                bufsize=1
            )
            for line in process.stdout:
                print(line, end='')
            process.wait()
            if process.returncode != 0:
                raise RuntimeError("Docker build failed with exit code {}".format(process.returncode))

            self.log.info("Running simulation as a subprocess.", indent=1)
            process = subprocess.Popen(["docker", "run",
                                        "-v", "{}:/simulation/binary_data".format(os.path.join(self.simulation_dir, "binary_data")),
                                        "-v", "{}:/simulation/run_config".format(os.path.join(self.simulation_dir, "run_config")),
                                        "-v", "{}:/simulation/run".format(os.path.join(self.simulation_dir, "run")),
                                        docker],
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT,
                                       universal_newlines=True,
                                       cwd=self.simulation_dir,
                                       bufsize=1)
            for line in process.stdout:
                print(line, end='')
            process.wait()
            if process.returncode != 0:
                raise RuntimeError("Docker run failed with exit code {}".format(process.returncode))
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

class mitgcm_67z(MitGCM):
    def __init__(self, *args, **kwargs):
        super(mitgcm_67z, self).__init__(*args, **kwargs)
        self.version = "67z"
        self.docker = "eawag/mitgcm:67z"