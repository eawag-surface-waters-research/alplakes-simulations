#!/bin/bash
cd /simulation/
ln -s /simulation/run_config/* .
cp /simulation/build/mitgcmuv .
mpirun -np 20 ./mitgcmuv
