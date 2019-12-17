#/bin/bash

DATA_DIR="/data/backup/archive/17-10-19"
ARCHIVE_DIR=$DATA_DIR
LOGDIR=$DATA_DIR
SCRIPTDIR="/ceph/home/hpcgill1/sens_testing_scripts"

NUM_THREADS=8

mkdir -p $LOGDIR

if [ $? -ne 0 ]; then
    exit 1
fi

for TAG in Ep0Pp0Sp0 Ep1Pp0Sp0 Ep2Pp0Sp0 Em1Pp0Sp0 Em2Pp0Sp0 Ep0Pp0Sp1 Ep0Pp0Sp2 Ep0Pp0Sm1 Ep0Pp0Sm2 CO WB COWB
do

    CMD="$SCRIPTDIR/run_measure_bias.sh $ARCHIVE_DIR $TAG"

    echo "Executing command: $CMD"
    eval $CMD

done