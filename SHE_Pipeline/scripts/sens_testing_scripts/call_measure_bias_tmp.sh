#/bin/bash

DATA_DIR="/tmp/2020-05-15"
ARCHIVE_DIR="/tmp/2020-05-15"
LOGDIR=$DATA_DIR
SCRIPTDIR="/ceph/home/hpcgill1/sens_testing_scripts"

NUM_THREADS=28

mkdir -p $LOGDIR

if [ $? -ne 0 ]; then
    exit 1
fi

# for TAG in Ep0Pp0Sp0 Ep1Pp0Sp0 Ep2Pp0Sp0 Em1Pp0Sp0 Em2Pp0Sp0 Ep0Pp0Sp1 Ep0Pp0Sp2 Ep0Pp0Sm1 Ep0Pp0Sm2 CO WB COWB Tm2 Tm1 Tp1 Tp2 Pm2 Pm1 Pp0 Pp1 Pp2 Lm2 Lm1 Lp1 Lp2 Sm2 Sm1 Sp1 Sp2 Um2 Um1 Up1 Up2 Rm2 Rm1 Rp1 Rp2 Xm2 Xm1 Xp1 Xp2
for TAG in Sp1 Sp2 Rm2 Rp1
do
    if [ ! -f $DATA_DIR/$TAG/shear_bias_measurements.xml ]; then 

        # CMD="sbatch -p ral -N 1 -n $NUM_THREADS -o $LOGDIR/measure_bias_"$TAG".out $LOGDIR/measure_bias_"$TAG".err $SCRIPTDIR/run_measure_bias.sh $DATA_DIR $TAG"
        CMD="$SCRIPTDIR/run_measure_bias.sh $DATA_DIR $TAG 2> $LOGDIR/$TAG.log"

    	echo "Executing command: $CMD"
        eval $CMD

    fi

done
