#/bin/bash

ISF="AUX/SHE_Pipeline/bias_measurement_isf.txt"
CFG_TEMPLATE_HEAD="AUX/SHE_GST_PrepareConfigs/Sensitivity"
CFG_TEMPLATE_TAIL="Template.conf"

ARCHIVE_DIR="/euclid/euclid-ial/sens_archive"
WORKSPACE_ROOT="/mnt/lustre/euclid-workdir/sens_"

# WORKSPACE_ROOT="/home/user/Work/workspace_"
# ARCHIVE_DIR="/home/user/Work/sens_archive"

BATCH_SIZE=1024
NUM_GALAXIES=256

SEED_START=1

SLEEP_TIME=20m

for I in (0..4000)
do

	let "SEED_MIN=SEED_START + $BATCH_SIZE*$I"
    let "SEED_MAX=SEED_START + $BATCH_SIZE*($I+1) - 1"

	for TAG in Ep0Pp0Sp0 Ep1Pp0Sp0 Ep2Pp0Sp0 Em1Pp0Sp0 Em2Pp0Sp0 Ep0Pp1Sp0 Ep0Pp2Sp0 Ep0Pm1Sp0 Ep0Pm2Sp0 Ep0Pp0Sp1 Ep0Pp0Sp2 Ep0Pp0Sm1 Ep0Pp0Sm2
	do

		CMD="E-Run SHE_Pipeline 0.4.2 SHE_Pipeline_Run --pipeline bias_measurement --isf $ISF --isf_args config_template $CFG_TEMPLATE_HEAD$TAG$CFG_TEMPLATE_TAIL --workdir $WORKSPACE_ROOT$TAG --config_args SHE_CTE_MeasureBias_archive_dir $ARCHIVE_DIR SHE_CTE_MeasureStatistics_archive_dir $ARCHIVE_DIR --app_workdir /euclid/euclid-ial/app_workdir" --plan_args MSEED_MIN $SEED_MIN MSEED_MAX $SEED_MAX NSEED_MIN $SEED_MIN NSEED_MAX $SEED_MAX NUM_GALAXIES $NUM_GALAXIES

		echo "Executing command: $CMD"
		eval $CMD

	done

    sleep $SLEEP_TIME

done
