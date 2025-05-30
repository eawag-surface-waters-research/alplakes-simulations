#!/bin/bash
cd /simulation/run_config
cp /simulation/build/mitgcmuv .
mpirun -np !cores! ./mitgcmuv
