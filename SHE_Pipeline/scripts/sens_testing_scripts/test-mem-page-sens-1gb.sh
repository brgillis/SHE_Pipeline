#/bin/bash

QUEUE="memTest1g"

ARCHIVE_DIR="/ceph/cam/memTest/sens_archive1gb"
WORKSPACE_ROOT="/ceph/cam/memTest/sens1gb_workdirs/sens_"
LOGDIR="/ceph/cam/memTest/sens1gb_logs"
SCRIPTDIR="/ceph/home/hpcgill1/sens_testing_scripts"

SLEEP_TIME=1m
RETRY_SLEEP=8h
JOB_LIMIT=150

SEED_START=1
SEEDS_PER_BATCH=96
NUM_GALAXIES_PER_SEED=16

BATCH_START=9900020
BATCH_END=10000000
NUM_THREADS=52

TEMPLATE_PREFIX="''"
TEMPLATE_POSTFIX="Template.conf"

KILLFILE="DELETE_ME_TO_STOP_MEMTEST1gb_SCRIPT"

touch $KILLFILE

ALL_DONE=0

while [ $ALL_DONE -eq 0 ]
do

	ALL_DONE=1

	for ((I=$BATCH_START; I<=$BATCH_END; I++))
	do

		for TAG in Ep0Pp0Sp0 Ep1Pp0Sp0 Ep2Pp0Sp0 Em1Pp0Sp0 Em2Pp0Sp0 Ep0Pp0Sp1 Ep0Pp0Sp2 Ep0Pp0Sm1 Ep0Pp0Sm2 CO WB COWB Tm2 Tm1 Tp1 Tp2
		do

			if [ ! -f $KILLFILE ]; then
			    exit 0
			fi

			if [ -f $ARCHIVE_DIR/$TAG/sens_$I/sens_$I\_$TAG/shear_bias_measurements_final.xml ]; then
			    continue
			fi
			ALL_DONE=0

			mkdir -p $ARCHIVE_DIR/$TAG

			CMD="sbatch -p $QUEUE -N 1 -n $NUM_THREADS -o $LOGDIR/sens_testing_"$TAG"_"$I".out -e $LOGDIR/sens_testing_"$TAG"_"$I".err $SCRIPTDIR/testmem_run_bias_measurement_pipeline.sh $SEED_START $SEEDS_PER_BATCH $NUM_GALAXIES_PER_SEED $TAG $I $NUM_THREADS $ARCHIVE_DIR $WORKSPACE_ROOT $SCRIPTDIR $TEMPLATE_PREFIX $TEMPLATE_POSTFIX &"

			RUNNING_JOBS=`squeue | grep " $QUEUE " | grep hpcgill1 | wc -l`
			while [ $RUNNING_JOBS -ge $JOB_LIMIT ]
			do
			    sleep $SLEEP_TIME
			    RUNNING_JOBS=`squeue | grep " $QUEUE " | grep hpcgill1 | wc -l`
			done

			echo "Executing command: $CMD"
			eval $CMD

		done

	done

	sleep $RETRY_SLEEP
done

rm $KILLFILE