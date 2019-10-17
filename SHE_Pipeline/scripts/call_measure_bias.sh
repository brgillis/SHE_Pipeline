#/bin/bash

QUEUE="ral"
DATA_DIR="/data"
ARCHIVE_DIR=$DATA_DIR"/sens_archive"
LOGDIR=$DATA_DIR"/sens_logs"
SCRIPTDIR="/ceph/home/hpcgill1/sens_testing_scripts"

NUM_THREADS=8

mkdir -p $LOGDIR

if [ $? -ne 0 ]; then
    exit 1
fi

for TAG in Ep0Pp0Sp0 Ep1Pp0Sp0 Ep2Pp0Sp0 Em1Pp0Sp0 Em2Pp0Sp0 Ep0Pp1Sp0 Ep0Pp2Sp0 Ep0Pm1Sp0 Ep0Pm2Sp0 Ep0Pp0Sp1 Ep0Pp0Sp2 Ep0Pp0Sm1 Ep0Pp0Sm2
do

    CMD="sbatch -p $QUEUE -N 1 -n $NUM_THREADS -o $LOGDIR/sens_testing_"$TAG"_combine.out -e $LOGDIR/sens_testing_"$TAG"_combine.err $SCRIPTDIR/run_bias_measurement_pipeline.sh $ARCHIVE_DIR $TAG"

    echo "Executing command: $CMD"
    eval $CMD

done
