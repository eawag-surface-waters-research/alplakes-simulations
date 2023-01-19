# -*- coding: utf-8 -*-
import os
import json
import shutil
import numpy as np
import subprocess

import river
import weather
from datetime import datetime, timedelta
from functions import logger, list_local_cosmo_files, ch1903_to_latlng, download_file, upload_file
from distutils.dir_util import copy_tree


class Delft3D(object):
    def __init__(self, params):
        self.params = params
        self.properties = {}
        self.simulation_dir = ""
        self.restart_file = ""
        self.files = [
            {"filename": 'CloudCoverage.amc', "parameter": "CLCT", "quantity": "cloudiness", "unit": "%", "adjust": 0},
            {"filename": 'Pressure.amp', "parameter": "PMSL", "quantity": "air_pressure", "unit": "Pa", "adjust": 0},
            {"filename": 'RelativeHumidity.amr', "parameter": "RELHUM_2M", "quantity": "relative_humidity", "unit": "%",
             "adjust": 0},
            {"filename": 'ShortwaveFlux.ams', "parameter": "GLOB", "quantity": "sw_radiation_flux", "unit": "W/m2",
             "adjust": 0},
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

        self.log.info("Creating input files from {} to {}".format(params["start"], params["end"] + timedelta(hours=24) - timedelta(seconds=1)))

    def process(self):
        self.initialise_simulation_directory(remove=False)
        # self.copy_static_data()
        # self.collect_restart_file()
        self.load_properties()
        # self.update_control_file()
        # self.weather_data_files()
        self.river_data_files()
        if self.params["upload"]:
            self.upload_data()
        if self.params["run"]:
            self.run_simulation()

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
            if self.params["restart"]:
                self.log.info("Copying restart file from local storage.", indent=1)
                file = self.params["restart"]
                if os.path.isfile(file):
                    shutil.copyfile(file, os.path.join(self.simulation_dir, self.restart_file))
                    self.log.end_stage()
                    return
                else:
                    self.log.info("Unable to locate restart file: ".format(file), indent=1)
            self.log.info("Downloading restart file from remote storage.", indent=1)
            if not self.params["bucket"]:
                raise ValueError("No bucket address provided, either include local files or specify a bucket.")
            bucket = "https://{}.s3.{}.amazonaws.com".format(self.params["bucket"], region)
            file = os.path.join(bucket, "simulations", components[0], "restart-files", components[1], self.restart_file)
            self.log.info("File location: {}".format(file), indent=2)
            download_file(file, os.path.join(self.simulation_dir, self.restart_file))
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

            for i in range(len(lines)):
                if "Restid" in lines[i]:
                    lines[i] = "Restid = #" + self.restart_file.replace("tri-rst.", "") + "#\n"
                if "Tstart" in lines[i]:
                    lines[i] = "Tstart = " + start + "\n"
                if "Tstop" in lines[i]:
                    lines[i] = "Tstop = " + end + "\n"
                if lines[i].split(" ")[0] in ["Flmap", "Flhis", "Flwq"]:
                    lines[i] = "{} = {} {} {}\n".format(lines[i].split(" ")[0], start, str(period), end)

            self.log.info("Writing new dates to simulation file.", indent=1)
            with open(os.path.join(self.simulation_dir, "Simulation_Web.mdf"), 'w') as f:
                f.writelines(lines)
            self.log.end_stage()
        except Exception as e:
            self.log.error()
            raise

    def weather_data_files(self, swiss_grid=True, buffer=10, no_data_value="-999.00"):
        try:
            self.log.begin_stage("Creating weather data files.")

            self.log.info("Creating the meteo grid", indent=1)
            grid = self.properties["grid"]
            minx, miny, maxx, maxy = grid["minx"], grid["miny"], grid["maxx"], grid["maxy"]
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

            if swiss_grid and grid["minx"] < 180:
                self.log.info("Grid using WGS84 coordinate system.", indent=1)
            else:
                self.log.info("Grid using ch1903 coordinate system, converting to WGS84 to collect weather data.", indent=1)
                minx, miny = ch1903_to_latlng(minx, miny)
                maxx, maxy = ch1903_to_latlng(maxx, maxy)

            self.log.info("Collecting weather data for region: [{}, {}] [{}, {}]".format(minx, miny, maxx, maxy), indent=1)
            self.log.info("Writing weather data to simulation files.", indent=1)
            variables = [file["parameter"] for file in self.files]
            days = [self.params["start"]+timedelta(days=x) for x in range((min(self.params["today"], self.params["end"]) - self.params["start"]).days+1)]
            for day in days:
                self.log.info("Collecting data for {} from remote API.".format(day), indent=2)
                data = weather.download_meteolakes_cosmo_area(minx, miny, maxx, maxy, day, variables, self.params["api"], self.params["today"])
                for file in self.files:
                    self.log.info("Processing parameter " + file["parameter"], indent=3)
                    weather.write_weather_data_to_file(data["time"], data[file["parameter"]]["data"], data["lat"], data["lng"], gxx, gyy, file, self.simulation_dir, no_data_value)

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
                self.properties = river.flow_balance(self.properties, log=self.log)

                self.log.info("Write river data to files.", indent=1)
                self.properties = river.write_river_data_to_file(self.properties, self.simulation_dir, log=self.log)
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
