#!/bin/sh
#
#SBATCH -N 1                      # number of nodes
#SBATCH -n 8                      # number of cores
#SBATCH -o bias_parallel.out            # STDOUT
#SBATCH -e bias_parallel.err            # STDERR
#SBATCH -t 0-20:0:0



ISF="AUX/SHE_Pipeline/bias_measurement_isf.txt"
CFG_TEMPLATE_HEAD="AUX/SHE_GST_PrepareConfigs/Sensitivity"


SEED_START=$1
SEEDS_PER_BATCH=$2
NUM_GALAXIES_PER_SEED=$3

TAG=$4
I=$5
NUM_THREADS=$6

ARCHIVE_DIR=$7
WORKSPACE_ROOT=$8
SCRIPTDIR=$9
TEMPLATE_PREFIX=${10}
TEMPLATE_POSTFIX=${11}


let "SEED_MIN=$SEED_START + $SEEDS_PER_BATCH*$I"
let "SEED_MAX=$SEED_START + $SEEDS_PER_BATCH*($I+1) - 1"

WORKDIR="$WORKSPACE_ROOT"$I"_$TAG"

source /cvmfs/euclid-dev.in2p3.fr/CentOS7/EDEN-2.0/etc/profile.d/euclid.sh

export BINARY_TAG=x86_64-co7-gcc48-o2g

E-Run SHE_Pipeline 0.8.25 SHE_Pipeline_RunBiasParallel --isf $ISF --isf_args config_template $CFG_TEMPLATE_HEAD$TEMPLATE_PREFIX$TAG$TEMPLATE_POSTFIX mdb $SCRIPTDIR/EUC_MDB_MISSIONCONFIGURATION-SC456_2019-03-28T1224.00Z_01.xml bfd_training_data $SCRIPTDIR/EUC_SHE_BFD-TRAINING-P_0_20190528T140053.8Z_00.07.xml --workdir $WORKDIR --config_args  SHE_CTE_EstimateShear_methods "KSB REGAUSS LensMC MomentsML" SHE_CTE_MeasureBias_archive_dir $ARCHIVE_DIR/$TAG/sens_$I --plan_args MSEED_MIN $SEED_MIN MSEED_MAX $SEED_MAX NSEED_MIN $SEED_MIN NSEED_MAX $SEED_MAX NUM_GALAXIES $NUM_GALAXIES_PER_SEED --cluster --number_threads $NUM_THREADS

# python3 /ceph/home/hpcgill1/bin/rm_r.py $WORKDIR

if [ $? -ne 0 ]; then
    rm -r $WORKDIR
fi


if [ $? -ne 0 ]; then
    exit 1
else
    exit 0
fi

