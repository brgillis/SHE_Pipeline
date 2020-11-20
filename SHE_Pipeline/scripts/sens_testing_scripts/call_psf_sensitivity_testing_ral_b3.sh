#/bin/bash

QUEUE="ral"

ARCHIVE_DIR="/ceph/ral/sens_psf_archive"
WORKSPACE_ROOT="/ceph/ral/sens_workdirs/sens_"
LOGDIR="/ceph/ral/sens_psf_logs"
SCRIPTDIR="/ceph/home/hpcgill1/sens_testing_scripts"

SLEEP_TIME=1m
RETRY_SLEEP=8h
JOB_LIMIT=100

SEED_START=1
SEEDS_PER_BATCH=96
NUM_GALAXIES_PER_SEED=16

BATCH_START=120001
BATCH_END=140000
NUM_THREADS=28

TEMPLATE_PREFIX="_PSF_"
TEMPLATE_POSTFIX=".conf"

KILLFILE="DELETE_ME_TO_STOP_RAL_B3_PSF_SCRIPT"

touch $KILLFILE

ALL_DONE=0

while [ $ALL_DONE -eq 0 ]
do

	ALL_DONE=1

	for ((I=$BATCH_START; I<=$BATCH_END; I++))
	do

		for TAG in Pm2 Pm1 Pp0 Pp1 Pp2 Lm2 Lm1 Lp1 Lp2 Sm2 Sm1 Sp1 Sp2 Um2 Um1 Up1 Up2 Rm2 Rm1 Rp1 Rp2 Xm2 Xm1 Xp1 Xp2
		do

			if [ ! -f $KILLFILE ]; then
			    exit 0
			fi

			if [ -f $ARCHIVE_DIR/$TAG/sens_$I/sens_$I\_$TAG/shear_bias_measurements_final.xml ]; then
			    continue
			fi
			ALL_DONE=0

			mkdir -p $ARCHIVE_DIR/$TAG

			CMD="sbatch -p $QUEUE -N 1 -n $NUM_THREADS -o $LOGDIR/sens_testing_"$TAG"_"$I".out -e $LOGDIR/sens_testing_"$TAG"_"$I".err $SCRIPTDIR/run_bias_measurement_pipeline.sh $SEED_START $SEEDS_PER_BATCH $NUM_GALAXIES_PER_SEED $TAG $I $NUM_THREADS $ARCHIVE_DIR $WORKSPACE_ROOT $SCRIPTDIR $TEMPLATE_PREFIX $TEMPLATE_POSTFIX &"

			RUNNING_JOBS=`squeue | grep $QUEUE | wc -l`
			while [ $RUNNING_JOBS -ge $JOB_LIMIT ]
			do
			    sleep $SLEEP_TIME
			    RUNNING_JOBS=`squeue | grep $QUEUE | wc -l`
			done

			echo "Executing command: $CMD"
			eval $CMD

		done

	done

	sleep $RETRY_SLEEP

done 

