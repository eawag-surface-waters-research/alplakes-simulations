C CPP options file for OBCS package, as per
C https://mitgcm.readthedocs.io/en/latest/phys_pkgs/obcs.html

#ifndef OBCS_OPTIONS_H
#define OBCS_OPTIONS_H

#include "PACKAGES_CONFIG.h"
#include "CPP_OPTIONS.h"

#ifdef ALLOW_OBCS

C Enable individual open boundaries
#undef ALLOW_OBCS_NORTH
#define ALLOW_OBCS_EAST
#define ALLOW_OBCS_SOUTH
#define ALLOW_OBCS_WEST

C Enable OB values to be prescribed via external fields that are read
C from a file
#define ALLOW_OBCS_PRESCRIBE

#endif /* ALLOW_OBCS */
#endif /* OBCS_OPTIONS_H */
