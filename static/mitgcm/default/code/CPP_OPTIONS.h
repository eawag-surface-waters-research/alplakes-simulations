#ifndef CPP_OPTIONS_H
#define CPP_OPTIONS_H

C Note: the following code shows the modifications from
C       the default values, listed on
C https://mitgcm.readthedocs.io/en/latest/getting_started/getting_started.html

C Options for EXF (external forcing package)
#define ATMOSPHERIC_LOADING
#define SHORTWAVE_HEATING

C Include hydrostatic pressure, and choose a nonhydrostatic formulation
#define INCLUDE_PHIHYD_CALCULATION_CODE
#define ALLOW_NONHYDROSTATIC

C Code for convective adjustment mixing algorithm
#define INCLUDE_CONVECT_CALL

C Include code to determine the net vertical diffusivity
#define INCLUDE_CALC_DIFFUSIVITY_CALL

C Use "Exact Convervation" of fluid in Free-Surface formulation so that
C d/dt(eta) is exactly equal to - Div.Transport
#define EXACT_CONSERV

C TODO: check if we need the following options:
C #define INCLUDE_IMPLVERTADV_CODE
C #define ALLOW_ADAMSBASHFORTH_3

#include "CPP_EEOPTIONS.h"

#endif /* CPP_OPTIONS_H */

