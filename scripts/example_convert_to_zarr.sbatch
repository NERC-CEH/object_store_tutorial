#!/bin/bash
#SBATCH -p long-serial
#SBATCH -o %J.out
#SBATCH -e %J.err
#SBATCH --time=120:00:00
#SBATCH --mem=64GB

export PATH=/home/users/mattjbr/anaconda3/bin:$PATH
source /home/users/mattjbr/anaconda3/bin/activate s2lai
/home/users/mattjbr/anaconda3/envs/s2lai/bin/python /gws/nopw/j04/ceh_generic/matbro/chess_scape/convert_to_zarr_monthchunk.py 01 tas
