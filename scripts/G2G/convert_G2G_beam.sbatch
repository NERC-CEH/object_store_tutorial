#!/bin/bash
#SBATCH -p par-multi
#SBATCH -o %J.out
#SBATCH -e %J.err
#SBATCH -n 64
#SBATCH --time=20:00:00
#SBATCH --mem=40GB

# Example SLURM job script for running the convert_G2G_beam.py
# script on a SLURM-controlled HPC

# set up conda env
export PATH=/home/users/mattjbr/miniconda3/bin:$PATH
source /home/users/mattjbr/miniconda3/bin/activate apache

# run script
/home/users/mattjbr/miniconda3/envs/apache/bin/ipython /gws/nopw/j04/ceh_generic/matbro/object_storage/scripts/convert_G2G_beam.py
