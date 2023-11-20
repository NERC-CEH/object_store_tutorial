#!/bin/bash

for var in {hurs,huss,pr,psurf,rlds,rsds,sfcWind,tasmax,tasmin,tas}
  do for ensmem in {01,04,06,15}
    do cp -v chessscape_lotus_job_template 10kmmonthchunk/sbatch_scripts/chessscape_lotus_job_ensmem_${ensmem}_${var}.sh
    sed -i "s/template1/${ensmem}/" 10kmmonthchunk/sbatch_scripts/chessscape_lotus_job_ensmem_${ensmem}_${var}.sh
    sed -i "s/template2/${var}/" 10kmmonthchunk/sbatch_scripts/chessscape_lotus_job_ensmem_${ensmem}_${var}.sh
  done
done
