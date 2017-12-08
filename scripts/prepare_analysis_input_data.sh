#!/usr/bin/bash

SCRIPTDIR=/home/user/Work/Projects/SHE_Pipeline/scripts

source $SCRIPTDIR/set_analysis_envvars.sh

mkdir -p $WORKDIR
mkdir -p $LOGDIR

CMD="E-Run SHE_CTE 0.3 SHE_CTE_MakeMockAnalysisData --psf_calibration_products $PSF_CAL_LF --aocs_time_series_products $AOCS_LF --mission_time_products $MT_LF --galaxy_population_priors_table $GP_PROD --calibration_parameters_product $CAL_PROD --shear_validation_statistics_table $SV_PROD --workdir $WORKDIR --logdir $LOGDIR"

echo "Making mock analysis data with command:"
echo $CMD
`$CMD`

if [ $? -ne 0 ]; then
    exit
fi

CMD="E-Run SHE_GST 1.3 GenGalaxyImages --config-file /home/user/Work/Projects/SHE_GST/SHE_GST_GalaxyImageGeneration/conf/SHE_GST_GalaxyImageGeneration/SampleStamps.conf --dithering_scheme 2x2 --data_images $DATA_IM_LF --segmentation_images $SEG_IM_LF --detections_tables $DTC_LF --details_tables $DAL_LF  --psf_images_and_tables $PSF_LF --workdir $WORKDIR --logdir $LOGDIR"

echo "Making simulated galaxy images with command:"
echo $CMD
`$CMD`

