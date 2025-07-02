#!/bin/bash
cd /simulation/run
ln -s /simulation/run_config/* .
cp /simulation/build/mitgcmuv .
mpirun -np !cores! ./mitgcmuv
