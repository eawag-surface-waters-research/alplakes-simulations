# -*- coding: utf-8 -*-
import os
import json
import netCDF4
import shutil
import numpy as np
import subprocess
from scipy.interpolate import griddata
from datetime import datetime, timedelta
from river import *
from functions import log, list_local_cosmo_files, latlng_to_ch1900
from distutils.dir_util import copy_tree


class Delft3D (object):
    def __init__(self, parameters):
        self.parameters = parameters
        self.properties = {}
        self.static = "static/delft3d-flow"
        self.files = [
                {"filename": 'CloudCoverage.amc', "parameter": "CLCT", "quantity": "cloudiness", "unit": "%", "adjust": 0},
                {"filename": 'Pressure.amp', "parameter": "PMSL", "quantity": "air_pressure", "unit": "Pa", "adjust": 0},
                {"filename": 'RelativeHumidity.amr', "parameter": "RELHUM_2M", "quantity": "relative_humidity", "unit": "%", "adjust": 0},
                {"filename": 'ShortwaveFlux.ams', "parameter": "GLOB", "quantity": "sw_radiation_flux", "unit": "W/m2", "adjust": 0},
                {"filename": 'Temperature.amt', "parameter": "T_2M", "quantity": "air_temperature", "unit": "Celsius", "adjust": -273.15},
                {"filename": 'WindU.amu', "parameter": "U", "quantity": "x_wind", "unit": "m s-1", "adjust": 0},
                {"filename": 'WindV.amv', "parameter": "V", "quantity": "y_wind", "unit": "m s-1", "adjust": 0},
            ]
        self.log = log(parameters["log_name"], parameters["log_path"])
        self.log.initialise("Initialising hydrodynamic simulation {} using {}".format(parameters["model"], parameters["setup"].replace("_", " ")))
        self.log.log("Simulating from {} to {}".format(parameters["start_date"], parameters["end_date"]))

    def process(self):
        self.copy_static_data()
        self.collect_restart_file()
        self.load_properties()
        self.update_control_file()
        self.weather_data_files()
        self.river_data_files()
        self.run_simulation()
        self.extract_simulation_results()

    def copy_static_data(self):
        try:
            stage = self.log.begin_stage("Copying static data to simulation folder.")
            files = copy_tree(os.path.join(self.static, self.parameters["model"]), self.parameters["simulation_folder"])
            for file in files:
                self.log.log("Copied {} to simulation folder.".format(os.path.basename(file)), indent=1)
            self.log.end_stage(stage)
        except Exception as e:
            self.log.error(stage)
            raise

    def collect_restart_file(self):
        try:
            stage = self.log.begin_stage("Collecting restart file.")
            file, start_date = self.collect_restart_file_local_storage()
            if start_date != self.parameters["start_date"]:
                self.parameters["start_date"] = start_date
                self.log.warning("Start date adjusted to {} to match restart file.".format(start_date), indent=1)
            self.log.log("Copying restart file to simulation folder.", indent=1)
            shutil.copyfile(file, os.path.join(self.parameters["simulation_folder"], "tri-rst.Simulation_Web_rst.000000"))
            self.log.end_stage(stage)
        except Exception as e:
            self.log.error(stage)
            raise

    def collect_restart_file_local_storage(self):
        self.log.log("Collecting restart file from local storage.", indent=1)
        files = os.listdir(self.parameters["restart_files"])
        files.sort()
        dates = [self.parameters["start_date"].timestamp() - datetime.strptime(x.split(".")[-2], '%Y%m%d').timestamp() for x in files]
        dates = [x for x in dates if x > 0]
        file = files[dates.index(min(dates))]
        date = datetime.strptime(file.split(".")[-2], '%Y%m%d')
        return os.path.join(self.parameters["restart_files"], file), date

    def load_properties(self):
        try:
            stage = self.log.begin_stage("Loading properties.")
            with open(os.path.join(self.parameters["simulation_folder"], "properties.json"), 'r') as f:
                self.properties = json.load(f)
            self.log.end_stage(stage)
        except Exception as e:
            self.log.error(stage)
            raise

    def update_control_file(self, origin=datetime(2008, 3, 1), period=180):
        try:
            stage = self.log.begin_stage("Updating control file dates.")
            self.log.log("Reading simulation file.", indent=1)
            with open(os.path.join(self.parameters["simulation_folder"], "Simulation_Web.mdf"), 'r') as f:
                lines = f.readlines()
            start = "{:.7e}".format((self.parameters["start_date"] - origin).total_seconds() / 60)
            end = "{:.7e}".format(((self.parameters["end_date"] - origin).total_seconds() / 60) - period)

            for i in range(len(lines)):
                if "Tstart" in lines[i]:
                    lines[i] = "Tstart = " + start + "\n"
                if "Tstop" in lines[i]:
                    lines[i] = "Tstop = " + end + "\n"
                if lines[i].split(" ")[0] in ["Flmap", "Flhis", "Flwq"]:
                    lines[i] = "{} = {} {} {}\n".format(lines[i].split(" ")[0], start, str(period), end)

            self.log.log("Writing new dates to simulation file.", indent=1)
            with open(os.path.join(self.parameters["simulation_folder"], "Simulation_Web.mdf"), 'w') as f:
                f.writelines(lines)
            self.log.end_stage(stage)
        except Exception as e:
            self.log.error(stage)
            raise

    def weather_data_files(self):
        try:
            stage = self.log.begin_stage("Creating weather data files.")
            self.create_weather_data_local_storage()
            self.log.end_stage(stage)
        except Exception as e:
            self.log.error(stage)
            raise

    def create_weather_data_local_storage(self, origin=datetime(2008, 3, 1)):
        self.log.log("Creating weather data files from local storage.", indent=1)
        files = list_local_cosmo_files(self.parameters["weather_data"], self.parameters["start_date"], self.parameters["end_date"])
        self.log.log("Located {} local meteo files covering the full simulation period.".format(len(files)), indent=2)
        self.log.log("Read latitude and longitude", indent=2)
        nc = netCDF4.Dataset(files[0], mode='r', format='NETCDF4_CLASSIC')
        lat = nc.variables["lat_1"][:]
        lon = nc.variables["lon_1"][:]
        nc.close()

        self.log.log("Reduce size of MeteoSwiss dataset", indent=2)
        grid = self.properties["grid"]
        mx, my = latlng_to_ch1900(lat, lon)
        mxx, myy = mx.flatten(), my.flatten()
        mask = np.logical_and(
            np.logical_and(mxx >= grid["minx"] - 3 * grid["dx"], mxx <= grid["maxx"] + 3 * grid["dx"]),
            np.logical_and(myy >= grid["miny"] - 3 * grid["dy"], myy <= grid["maxy"] + 3 * grid["dy"]))
        ind = np.where(mask)
        mxxx, myyy = mxx[ind], myy[ind]

        self.log.log("Creating the grid", indent=2)
        gx = np.arange(grid["minx"], grid["maxx"] + grid["dx"], grid["dx"])
        gy = np.arange(grid["miny"], grid["maxy"] + grid["dy"], grid["dy"])
        gxx, gyy = np.meshgrid(gx, gy)

        self.log.log("Create the output meteo files and write the header", indent=2)
        write_files = []
        for i in range(len(self.files)):
            f = open(os.path.join(self.parameters["simulation_folder"], self.files[i]["filename"]), "w")
            f.write('FileVersion = 1.03')
            f.write('\nfiletype = meteo_on_equidistant_grid')
            f.write('\nNODATA_value = -999.00')
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
            write_files.append(f)

        self.log.log("Extracting data from files.", indent=2)
        for file in files:
            self.log.log("Processing file " + file, indent=3)
            nc = netCDF4.Dataset(file, mode='r', format='NETCDF4_CLASSIC')
            date = datetime.strptime(file.split(".")[-2].split("_")[-1], '%Y%m%d')
            for i in range(len(self.files)):
                self.log.log("Processing parameter " + self.files[i]["parameter"], indent=4)
                var = nc.variables[self.files[i]["parameter"]][:]
                var = var + self.files[i]["adjust"]
                nDims = len(nc.variables[self.files[i]["parameter"]].dimensions)
                for j in range(var.shape[0]):
                    dt = date + timedelta(hours=j)
                    diff = dt - origin
                    hours = diff.total_seconds() / 3600
                    time_str = "TIME = " + str(hours) + "0 hours since " + origin.strftime(
                        "%Y-%m-%d %H:%M:%S") + " +00:00"
                    write_files[i].write(time_str)
                    if nDims == 3:
                        data = var[j, :, :]
                    elif nDims == 4:
                        data = var[j, 0, :, :]
                    else:
                        raise ValueError(
                            "Incorrect number of dimensions for variable " + self.files[i]["parameter"] + " in file " + file)
                    grid_interp = griddata((mxxx, myyy), data.flatten()[ind], (gxx, gyy))
                    grid_interp[np.isnan(grid_interp)] = -999.00
                    write_files[i].write("\n")
                    np.savetxt(write_files[i], grid_interp, fmt='%.2f')
        nc.close()

        for i in range(len(write_files)):
            write_files[i].close()

    def river_data_files(self):
        try:
            stage = self.log.begin_stage("Creating river data files.")
            if "inflows" not in self.properties:
                self.log.warning("No river inputs specified, skipping stage.", indent=1)
            else:
                self.create_river_data_local_storage()
            self.log.end_stage(stage)
        except Exception as e:
            self.log.error(stage)
            raise

    def create_river_data_local_storage(self):
        self.log.log("Creating weather data files from local storage.", indent=1)
        self.log.log("Collecting river data.", indent=2)
        self.properties = get_raw_river_data(self.properties, self.parameters["river_data"], self.parameters["start_date"], self.parameters["end_date"])
        self.log.log("Cleaning raw river data.", indent=2)
        self.properties = clean_raw_river_data(self.properties, self.parameters["start_date"], self.parameters["end_date"])
        self.log.log("Flow balance ungauged rivers.", indent=2)
        self.properties = flow_balance_ungauged_rivers(self.properties)
        self.log.log("Estimate flow temperatures using meteoswiss data.", indent=2)
        files = list_local_cosmo_files(self.parameters["weather_data"], self.parameters["start_date"],
                                       self.parameters["end_date"])
        self.log.log("Located {} local meteo files covering the full simulation period.".format(len(files)), indent=3)
        self.properties = estimate_flow_temperature(self.properties, files, self.parameters["start_date"])
        verify_flow_balance(self.properties)
        self.log.log("Writing data to file.", indent=2)
        write_river_data_to_file(self.properties, self.parameters["simulation_folder"])

    def run_simulation(self):
        try:
            stage = self.log.begin_stage("Running simulation.")
            self.log.log("Running simulation as a subprocess.", indent=1)
            print("-v {}:/job".format(self.parameters["simulation_folder"]))
            process = subprocess.Popen(["docker", "run", "-v", "{}:/job".format(self.parameters["simulation_folder"]),
                                        self.docker],
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       universal_newlines=True,
                                       cwd=self.parameters["simulation_folder"])
            error = self.log.subprocess(process, error="Flow exited abnormally")
            if process.returncode != 0:
                output, error = process.communicate()
                raise RuntimeError("Subprocess failed with the following error: {}".format(error))
            elif error:
                raise RuntimeError("Simulation failed check the logs for more information.")
            self.log.end_stage(stage)
        except Exception as e:
            self.log.error(stage)
            raise

    def extract_simulation_results(self):
        try:
            stage = self.log.begin_stage("Extracting simulation results.")
            self.extract_local_netcdf()
            self.log.end_stage(stage)
        except Exception as e:
            self.log.error(stage)
            raise

    def extract_local_netcdf(self):
        self.log.log("Extracting simulation results to local NetCDF library.", indent=1)
        os.makedirs(self.parameters["output_files"], exist_ok=True)
        out_nc = os.path.join(self.parameters["simulation_folder"], "trim-Simulation_Web.nc")
        if os.path.isfile(out_nc):
            shutil.copy(out_nc, os.path.join(self.parameters["output_files"],
                                             "{}_{}_{}_{}.nc".format(self.parameters["setup"], self.parameters["model"],
                                                                     self.parameters["start_date"].strftime("%Y%m%d"),
                                                                     self.parameters["end_date"].strftime("%Y%m%d"))))
        else:
            raise ValueError(
                "Output NetCDF file not found, the simulation may have failed or you need to use a newer version of Delft3D.")


class Delft3D_501002163(Delft3D):
    def __init__(self, *args, **kwargs):
        super(Delft3D_501002163, self).__init__(*args, **kwargs)
        self.version = "5.01.00.2163"
        self.static = "static/delft3d-flow"
        self.docker = "eawag/delft3d-flow:5.01.00.2163"


class Delft3D_6030062434(Delft3D):
    def __init__(self, *args, **kwargs):
        super(Delft3D_6030062434, self).__init__(*args, **kwargs)
        self.version = "6.03.00.62434"
        self.docker = "eawag/delft3d-flow:6.03.00.62434"
