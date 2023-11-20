#!/bin/bash
#SBATCH -p high-mem
#SBATCH -o %J.out
#SBATCH -e %J.err
#SBATCH --time=10:00:00
#SBATCH --mem=600GB

export PATH=/home/users/mattjbr/miniconda3/bin:$PATH
source /home/users/mattjbr/miniconda3/bin/activate apache
/home/users/mattjbr/miniconda3/envs/apache/bin/ipython /gws/nopw/j04/ceh_generic/matbro/object_storage/scripts/preproc_G2G.py
