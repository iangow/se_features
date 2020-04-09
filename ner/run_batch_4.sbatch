#!/bin/bash

#SBATCH --job-name=NER_4_test
#SBATCH --output=NER_4_test_%A_%a.out
#SBATCH --error=NER_4_test_%A_%a.err
#SBATCH --partition=sandyb
#SBATCH --nodes=1
#SBATCH --account=pi-aaz
#SBATCH --ntasks-per-node=16
#SBATCH --time=24:00:00

######################
# Begin work section #
######################

module load python/3.4-2015q1
module load java

export PGHOST=aaz.chicagobooth.edu
export PGDATABASE=postgres
export PGUSER=hpourreza
export JAVAHOME=/software/java-1.8-x86_64/bin/java

echo "job started at `date`"

python /project/aaz/bs_linguistics/proper_nouns/run_NER.py --ner-class 4

echo "job finished at `date`"
