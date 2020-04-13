#!/bin/bash
#SBATCH --partition=physical
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1
#SBATCH --output=2n8c.out
#SBATCH --time=0-12:00:00




# Load required modules
module load Python/3.4.3-goolf-2015a
time mpirun -np 8 python ass1.py