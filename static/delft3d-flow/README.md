# Delft3D Flow

## Alplakes

Steps for creating a Alplakes compliant Delft3D model setup.

1. Copy the following files from one of the other simulations to the new simulation folder.
    ```
    config_d_hydro.xml
    config_flow2d3d.ini
    config_flow2d3d.xml
    ```

2. Rename .mdf file to Simulation_Web.mdf
3. Check the units of the grid, default is CH1903
4. Delete any unnecessary files such as .sh scripts and output files
5. Edit the `Simulation_Web.mdf` file and ensure that the following parameters are set:
    ```
    T0 =  ${Initial Conditions}
    Itdate = #2008-03-01#
    Dt = 0.5
    Tunit  = #M#
    Zmodel = #Y# 
    Filscc = #Secchi.scc#
    Filwp  = #Pressure.amp#
    Filwu  = #WindU.amu#
    Filwv  = #WindV.amv#
    Filws  = #ShortwaveFlux.ams#
    Filwt  = #Temperature.amt#
    Filwr  = #RelativeHumidity.amr#
    Filwc  = #CloudCoverage.amc#
    Flmap  =  7.4908800e+006 180  7.4988000e+006
    Flhis  =  7.4908800e+006 180  7.4988000e+006
    Flrst  = 10080
    ncFormat = 4
    FlNcdf = #map#
    ```
6. Create the properties.json file.
   ```json
   {
     "grid": {
       "minx": 506000,
       "miny": 161000,
       "maxx": 517000,
       "maxy": 171000,
       "dx": 500,
       "dy": 500
     },
     "secchi" : {
       "fixed":  2.8,
       "monthly":  [2.50, 2.59, 2.51, 2.63, 2.31, 2.77, 4.18, 2.96, 2.50, 2.56, 2.66, 3.25]
     }
   }
   ```
7. Run a start-up run in order to collect the restart file for the previous Sunday. Make sure to start run on a Sunday 
in order to get correct restart file dates.
   ```commandline
   python main.py -m delft3d-flow/joux -x -d eawag/delft3d-flow:6.03.00.62434 -s 20230416 -e 20230425
   ```
8. Replace the `T0` with `Restid =  #Simulation_Web_rst.000000#`
9. Push changes to the repo.
