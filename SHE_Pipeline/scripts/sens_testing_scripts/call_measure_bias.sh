#/bin/bash

DATA_DIR="/data/backup/archive/21-11-19"
ARCHIVE_DIR="/ceph/home/hpcgill1/sens_data"
LOGDIR=$DATA_DIR
SCRIPTDIR="/ceph/home/hpcgill1/sens_testing_scripts"

NUM_THREADS=8

mkdir -p $LOGDIR

if [ $? -ne 0 ]; then
    exit 1
fi

# for TAG in Ep0Pp0Sp0 Ep1Pp0Sp0 Ep2Pp0Sp0 Em1Pp0Sp0 Em2Pp0Sp0 Ep0Pp0Sp1 Ep0Pp0Sp2 Ep0Pp0Sm1 Ep0Pp0Sm2 CO WB COWB TAG Tm2 Tm1 Tp1 Tp2
for TAG in Tp1 Tp2
do

    # CMD="sbatch -p ral -N 1 -n 8 $SCRIPTDIR/run_measure_bias.sh $DATA_DIR $TAG"
    CMD="$SCRIPTDIR/run_measure_bias.sh $DATA_DIR $TAG"

    echo "Executing command: $CMD"
    eval $CMD

done
