# Delft3d-flow output files

The output NetCDF4 files from delft3d-flow simulations are described below.

**Warning:** Velocity variables U1 and V1 are in the grid coordinate system and need be rotated using ALFAS in order to 
obtain the velocity components in geographic coordinate system.

### Metadata:

```python
import netCDF4

with netCDF4.Dataset("example.nc") as nc:
    print(nc)
```

```
root group (NETCDF3_64BIT_OFFSET data model, file format NETCDF3):

Conventions: CF-1.6 SGRID-0.3
institution: Deltares
references: www.deltares.nl
source: Deltares, FLOW2D3D Version 6.02.10.7409, Jul 24 2017, 20:45:02
history: This file is created on 2023-01-04T00:38:03+0100, Delft3D
LAYER_MODEL: Z-MODEL
dimensions(sizes): 
    NC(22), MC(153), N(22), M(153), strlen20(20), LSTSCI(1), LTUR(2), K_LYR(50), K_INTF(51), KMAXOUT(51), 
    KMAXOUT_RESTR(50), time(2)
variables(dimensions): 
    float32 XCOR(MC, NC) YCOR(MC, NC) XZ(M, N) YZ(M, N) ALFAS(M, N) KCU(MC, N) KCV(M, NC) KCS(M, N) DP0(MC, NC) 
    DPS0(M, N) DPU0(MC, N) DPV0(M, NC), |S1 NAMCON(LSTSCI, strlen20), |S1 NAMTUR(LTUR, strlen20) ZK_LYR(K_LYR) 
    ZK(K_INTF) GSQS(M, N) PPARTITION(M, N) KMAXOUT(KMAXOUT) KMAXOUT_RESTR(KMAXOUT_RESTR) RHOCONST() GRAVITY() 
    grid() time(time) S1(time, M, N) KFU(time, MC, N) KFV(time, M, NC) U1(time, KMAXOUT_RESTR, MC, N) 
    V1(time, KMAXOUT_RESTR, M, NC) W(time, KMAXOUT, M, N) WPHY(time, KMAXOUT_RESTR, M, N) 
    R1(time, LSTSCI, KMAXOUT_RESTR, M, N) RTUR1(time, LTUR, KMAXOUT, M, N) TAUKSI(time, MC, N) TAUETA(time, M, NC) 
    TAUMAX(time, M, N) VICWW(time, KMAXOUT, M, N) DICWW(time, KMAXOUT, M, N) RICH(time, KMAXOUT, M, N) 
    RHO(time, KMAXOUT_RESTR, M, N) UMNLDF(time, MC, N) VMNLDF(time, M, NC) VICUV(time, KMAXOUT_RESTR, M, N) 
    HYDPRES(time, KMAXOUT_RESTR, M, N) WINDU(time, M, N) WINDV(time, M, N) PATM(time, M, N) WINDCD(time, M, N) 
    CLOUDS(time, M, N) AIRHUM(time, M, N) AIRTEM(time, M, N) EVAP(time, M, N) QEVA(time, M, N) QCO(time, M, N) 
    QBL(time, M, N) QIN(time, M, N) QNET(time, M, N) HFREE(time, M, N) EFREE(time, M, N)
```

### Dimensions

| Dimension     | Description                                                                                                                                   |
|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| NC            | number of grid cell corners in the η (==V) direction                                                                                          |
| MC            | number of grid cell corners in the ξ (==U) direction                                                                                          |
| N             | number of grid cell centers in the V direction                                                                                                |
| M             | number of grid cell centers in the U direction                                                                                                |
| strlen20      | number of characters in string variables                                                                                                      |
| LSTSCI        | Number of Constituents (Salinity, Temperature, Sediment, Conservative Constituents and Secondary Flow)                                        |
| LTUR          | Flag for 3D turbulence model, also denoting the number of turbulent energy constituents (0 = Algebraic model, 1 = k-l model, 2 = k-eps model) |
| K_LYR         | number of layers                                                                                                                              |
| K_INTF        | number of layers interfaces (K_LYR + 1)                                                                                                       |
| KMAXOUT       | number of layer interfaces                                                                                                                    |
| KMAXOUT_RESTR | number of layer interfaces                                                                                                                    |
| time          | number of timestep stored in seconds since Itdate (in .mdf file, reference date of the simulation)                                            |

### Variables

| Variable                              | Description                                                                                           |
|---------------------------------------|-------------------------------------------------------------------------------------------------------|
| XCOR(MC, NC)                          | East coordinate of grid cell corners                                                                  |
| YCOR(MC, NC)                          | North coordinate of grid cell corners                                                                 |
| XZ(M, N)                              | East coordinate of grid cell centers (zeta points)                                                    |
| YZ(M, N)                              | North coordinate of grid cell centers (zeta points)                                                   |
| ALFAS(M, N)                           | Orientation of ξ axis with respect to the x-axis. Positive clockwise.                                 |
| KCU(MC, N)                            | Mask array for U velocity points (one more unmasked column per row, no boundary conditions for U)     |
| KCV(M, NC)                            | Mask array for V velocity points (one more unmasked row per column, no boundary conditions for V)     |
| KCS(M, N)                             | Active/non-active water level points                                                                  |
| DP0(MC, NC)                           | Initial bottom depth at grid cell corners                                                             |
| DPS0(M, N)                            | Initial bottom depth at zeta points                                                                   |
| DPU0(MC, N)                           | Initial bottom depth at U points                                                                      |
| DPV0(M, NC)                           | Initial bottom depth at V points                                                                      |
| S1 NAMCON(LSTSCI, strlen20)           | Name of constituents defined in water level/zeta points                                               |
| S1 NAMTUR(LTUR, strlen20)             | Name of turbulent quantities defined in water level/zeta points                                       |
| ZK_LYR(K_LYR)                         | Vertical coordinate of layer center                                                                   |
| ZK(K_INTF)                            | Vertical coordinate of layer interfaces                                                               |
| GSQS(M, N)                            | Horizontal area of computational cell                                                                 |
| PPARTITION(M, N)                      | Partition to which each grid cell is associated (0 to 5 for Lake Joux case)                           |
| KMAXOUT(KMAXOUT)                      | Number of layer interfaces                                                                            |
| KMAXOUT_RESTR(KMAXOUT_RESTR)          | Number of layer centers                                                                               |
| RHOCONST()                            | User specified constant density                                                                       |
| GRAVITY()                             | Gravitational acceleration                                                                            |
| grid()                                | Grid information                                                                                      |
| time(time)                            | Time in seconds since reference date of the simulation                                                |
| S1(time, M, N)                        | Water level in z points                                                                               |
| KFU(time, MC, N)                      | Active/non-active U points (can change due to drying and flooding)                                    |
| KFV(time, M, NC)                      | Active/non-active V points (can change due to drying and flooding)                                    |
| U1(time, KMAXOUT_RESTR, MC, N)        | U velocity per layer in U points                                                                      |
| V1(time, KMAXOUT_RESTR, M, NC)        | V velocity per layer in V points                                                                      |
| W(time, KMAXOUT, M, N)                | Vertical velocity per layer at layer interfaces                                                       |
| WPHY(time, KMAXOUT_RESTR, M, N)       | Vertical velocity per layer at layer center                                                           |
| R1(time, LSTSCI, KMAXOUT_RESTR, M, N) | Concentrations of constituents per layer in z points and at layer centers                             |
| RTUR1(time, LTUR, KMAXOUT, M, N)      | Turbulent quantities per layer in z points and at layer interfaces                                    |
| TAUKSI(time, MC, N)                   | Bottom stress in U points (ξ direction)                                                               |
| TAUETA(time, M, NC)                   | Bottom stress in V points (η direction)                                                               |
| TAUMAX(time, M, N)                    | Maximum bottom stress in z points                                                                     |
| VICWW(time, KMAXOUT, M, N)            | Vertical eddy viscosity in z points at layer interfaces                                               |
| DICWW(time, KMAXOUT, M, N)            | Vertical eddy diffusivity in z points at layer interfaces                                             |
| RICH(time, KMAXOUT, M, N)             | Richardson number in z points at layer interfaces                                                     |
| RHO(time, KMAXOUT_RESTR, M, N)        | Density (computed with Unesco formulation from Temperature and Salinity) in z points at layer centers |
| UMNLDF(time, MC, N)                   | Filtered U velocity after Forester filter application                                                 |
| VMNLDF(time, M, NC)                   | Filtered V velocity after Forester filter application                                                 |
| VICUV(time, KMAXOUT_RESTR, M, N)      | Horizontal eddy viscosity in Z points and layer centers                                               |
| HYDPRES(time, KMAXOUT_RESTR, M, N)    | Hydrostatic pressure in Z points and layer centers                                                    |
| WINDU(time, M, N)                     | Wind speed velocity in x-direction in z points                                                        |
| WINDV(time, M, N)                     | Wind speed velocity in y-direction in z points                                                        |
| PATM(time, M, N)                      | Air pressure in z points                                                                              |
| WINDCD(time, M, N)                    | Wind drag coefficient in z points                                                                     |
| CLOUDS(time, M, N)                    | Cloud coverage percentage in z points                                                                 |
| AIRHUM(time, M, N)                    | Relative humidity in z points                                                                         |
| AIRTEM(time, M, N)                    | Air temperature in z points                                                                           |
| EVAP(time, M, N)                      | Evaporation rate in z points (in mm)                                                                  |
| QEVA(time, M, N)                      | Evaporation heat flux in z points (only includes forced evaporation component)                        |
| QCO(time, M, N)                       | Heat flux of forced convection in z points                                                            |
| QBL(time, M, N)                       | Net back radiation in z points                                                                        |
| QIN(time, M, N)                       | Net solar radiation in z points                                                                       |
| QNET(time, M, N)                      | Total net heat flux in z points                                                                       |
| HFREE(time, M, N)                     | Free convection of sensible heat in z points                                                          |
| EFREE(time, M, N)                     | Free convection of latent heat in z points                                                            |

