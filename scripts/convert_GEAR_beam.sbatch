#!/bin/bash
#SBATCH -p par-multi
#SBATCH -o %J.out
#SBATCH -e %J.err
#SBATCH -n 64
#SBATCH --time=2:00:00
#SBATCH --mem=40GB

# Example SLURM job script for running the convert_GEAR_beam.py
# script on a SLURM-controlled HPC

# set up conda env (link to the existing one) (not sure this is strictly necessary)
export PATH=/home/users/mattjbr/miniconda3/bin:$PATH
source /home/users/mattjbr/miniconda3/bin/activate gear

# run script, ensuring correct env is picked up
/home/users/mattjbr/miniconda3/envs/gear/bin/ipython /gws/nopw/j04/fdri/users/matbro/dri_gridded_data/scripts/convert_GEAR_beam.py
