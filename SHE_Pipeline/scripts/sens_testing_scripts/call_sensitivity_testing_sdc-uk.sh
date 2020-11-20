#/bin/bash

QUEUE="eden1.2"

ARCHIVE_DIR="/mnt/cephfs/sens-test/archive"
WORKSPACE_ROOT="/tmp/sens_"
LOGDIR="/mnt/cephfs/sens-test/logs"
SCRIPTDIR="/home/brg/sens_testing_scripts"

SLEEP_TIME=1m
RETRY_SLEEP=5m
JOB_LIMIT=60

SEED_START=1
SEEDS_PER_BATCH=96
NUM_GALAXIES_PER_SEED=16

BATCH_START=200001
BATCH_END=300000
NUM_THREADS=12

TEMPLATE_PREFIX="''"
TEMPLATE_POSTFIX="Template.conf"

KILLFILE="DELETE_ME_TO_STOP_SDC-UK_SCRIPT"

touch $KILLFILE

ALL_DONE=0

while [ $ALL_DONE -eq 0 ]
do

	ALL_DONE=1

	for ((I=$BATCH_START; I<=$BATCH_END; I++))
	do

		for TAG in Ep0Pp0Sp0 Ep1Pp0Sp0 Ep2Pp0Sp0 Em1Pp0Sp0 Em2Pp0Sp0 Ep0Pp0Sp1 Ep0Pp0Sp2 Ep0Pp0Sm1 Ep0Pp0Sm2 CO WB COWB
		do

			if [ ! -f $KILLFILE ]; then
			    exit 0
			fi

			if [ -f $ARCHIVE_DIR/$TAG/sens_$I/sens_$I\_$TAG/shear_bias_measurements_final.xml ]; then
			    continue
			fi
			ALL_DONE=0

			mkdir -p $ARCHIVE_DIR/$TAG

			CMD="sbatch -p $QUEUE -N 1 -n $NUM_THREADS -o $LOGDIR/sens_testing_"$TAG"_"$I".out -e $LOGDIR/sens_testing_"$TAG"_"$I".err $SCRIPTDIR/run_bias_measurement_pipeline.sh $SEED_START $SEEDS_PER_BATCH $NUM_GALAXIES_PER_SEED $TAG $I $NUM_THREADS $ARCHIVE_DIR $WORKSPACE_ROOT $SCRIPTDIR $TEMPLATE_PREFIX $TEMPLATE_POSTFIX"

			RUNNING_JOBS=`squeue | grep " $QUEUE " | wc -l`
			while [ $RUNNING_JOBS -ge $JOB_LIMIT ]
			do
			    sleep $SLEEP_TIME
			    RUNNING_JOBS=`squeue | grep " $QUEUE " | wc -l`
			done

			echo "Executing command: $CMD"
			eval $CMD

		done

	done

	sleep $RETRY_SLEEP
done

rm $KILLFILE
