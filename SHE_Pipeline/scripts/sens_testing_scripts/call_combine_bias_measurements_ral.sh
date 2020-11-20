#/bin/bash

QUEUE="ral"

ARCHIVE_DIR="/ceph/ral/sens_archive"
WORKSPACE_ROOT="/ceph/ral/sens_workdirs/sens_"
LOGDIR="/ceph/ral/sens_logs"
SCRIPTDIR="/ceph/home/hpcgill1/sens_testing_scripts"

SLEEP_TIME=1m
RETRY_SLEEP=5m
JOB_LIMIT=200

SEED_START=1
SEEDS_PER_BATCH=96
NUM_GALAXIES_PER_SEED=16

BATCH_START=1
BATCH_END=2500
NUM_THREADS=8

ALL_DONE=0

for TAG in Ep0Pp0Sp0 Ep1Pp0Sp0 Ep2Pp0Sp0 Em1Pp0Sp0 Em2Pp0Sp0 Ep0Pp0Sp1 Ep0Pp0Sp2 Ep0Pp0Sm1 Ep0Pp0Sm2 CO WB COWB
do

	CMD="sbatch -p $QUEUE -N 1 -n $NUM_THREADS -o $LOGDIR/sens_testing_"$TAG"_combine.out -e $LOGDIR/sens_testing_"$TAG"_combine.err $SCRIPTDIR/run_combine_bias_measurements.sh $SEED_START $SEEDS_PER_BATCH $NUM_GALAXIES_PER_SEED $TAG $NUM_THREADS $ARCHIVE_DIR $WORKSPACE_ROOT $SCRIPTDIR $TEMPLATE_PREFIX $TEMPLATE_POSTFIX"

	RUNNING_JOBS=`squeue | grep " $QUEUE " | wc -l`
	while [ $RUNNING_JOBS -ge $JOB_LIMIT ]
	do
	    sleep $SLEEP_TIME
	    RUNNING_JOBS=`squeue | grep " $QUEUE " | wc -l`
	done

	echo "Executing command: $CMD"
	eval $CMD

done

rm $KILLFILE
