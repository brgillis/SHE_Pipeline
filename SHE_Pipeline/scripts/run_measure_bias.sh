#!/bin/sh
#
#SBATCH -N 1                      # number of nodes
#SBATCH -n 8                      # number of cores
#SBATCH -o bias_parallel.out            # STDOUT
#SBATCH -e bias_parallel.err            # STDERR
#SBATCH -t 2-0:0:0

ARCHIVE_DIR=$1
TAG=$2

source /cvmfs/euclid-dev.in2p3.fr/CentOS7/EDEN-2.0/etc/profile.d/euclid.sh

export BINARY_TAG=x86_64-co7-gcc48-o2g

CMD="E-Run SHE_CTE 0.6.7 SHE_CTE_MeasureBias --workdir $ARCHIVE_DIR/$TAG --archive_dir $ARCHIVE_DIR/$TAG --bootstrap_seed 1 --shear_bias_measurements shear_bias_measurements.xml"

echo "Executing command: $CMD"
eval $CMD

if [ $? -ne 0 ]; then
    exit 1
else
    exit 0
fi