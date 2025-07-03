C Options for the EXF (external forcing) package, as per the list here
C https://mitgcm.readthedocs.io/en/latest/phys_pkgs/exf.html

#ifndef EXF_OPTIONS_H
#define EXF_OPTIONS_H

#include "PACKAGES_CONFIG.h"
#include "CPP_OPTIONS.h"

C External forces interacting with the surface of the water
#define ALLOW_ATM_WIND
#define ALLOW_ATM_TEMP
#define ALLOW_DOWNWARD_RADIATION

#define ALLOW_BULKFORMULAE

#endif /* EXF_OPTIONS_H */
