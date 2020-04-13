#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --output=1n1c.out
#SBATCH --time=0-12:00:00
#SBATCH --mail-user=guoningq@student.unimelb.edu.au
#SBATCH --mail-type=ALL



# Load required modules
module load Python/3.4.3-goolf-2015a
time mpirun -np 1 python ass1.py