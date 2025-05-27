#!/bin/bash -l

#SBATCH --account=em09
#SBATCH --job-name="MITgcmInference"
#SBATCH --time=15:00:00
#SBATCH --ntasks=576
#SBATCH --cpus-per-task=1
#SBATCH --partition=normal
#SBATCH --mail-user=anne.leroquais@eawag.ch
#SBATCH --mail-type=end
# Script to run the simulation on the CSSC cluster

export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MPICH_GPU_SUPPORT_ENABLED=0

srun --ntasks=${SLURM_NTASKS} ./mitgcmuv

source activate mitgcm
python extract_lexplore_results.py
python extract_surface_results.py