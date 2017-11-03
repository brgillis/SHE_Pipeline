#!/usr/bin/bash

SCRIPTDIR=/home/user/Work/Projects/SHE_Pipeline/SHE_Pipeline/scripts

source $SCRIPTDIR/prepare_pipeline_envvars.sh

mkdir -p $WORKDIR
mkdir -p $LOGDIR

CMD="E-Run SHE_CTE 0.2 SHE_CTE_MakeMockAnalysisData --data_images $DATA_IM --psf_calibration_products $PSF_CAL --segmentation_images $SEG_IM --detections_tables $DTC_TAB --astrometry_products $AST_PROD --aocs_time_series_products $AOCS_PROD --mission_time_products $MT_PROD --galaxy_population_priors_table $GP_TAB --calibration_parameters_product $CAL_PROD --calibration_parameters_listfile $CAL_LF --shear_validation_statistics_table $SV_TAB --workdir $WORKDIR --logdir $LOGDIR"

echo $CMD
`$CMD`

if [ $? -ne 0 ]; then
    exit
fi

CMD="E-Run SHE_GST 1.2 GenGalaxyImages --config-file /home/user/Work/Projects/SHE_GST/SHE_GST_GalaxyImageGeneration/conf/SHE_GST_GalaxyImageGeneration/SampleStamps.conf --dithering_scheme 2x2 --data_images $DATA_IM --segmentation_images $SEG_IM --detections_tables $DTC_TAB --details_tables $DAL_TAB  --psf_images_and_tables $PSF_IMTAB --workdir $WORKDIR --logdir $LOGDIR"

echo $CMD
`$CMD`

if [ $? -ne 0 ]; then
    exit
fi

CMD="E-Run SHE_CTE 0.2 SHE_CTE_FitPSFs --data_images $DATA_IM --detections_tables $DTC_TAB --astrometry_products $AST_PROD --aocs_time_series_products $AOCS_PROD --mission_time_products $MT_PROD --psf_calibration_products $PSF_CAL --psf_images_and_tables $PSF_IMTAB --workdir $WORKDIR --logdir $LOGDIR"

echo $CMD
`$CMD`

if [ $? -ne 0 ]; then
    exit
fi

CMD="E-Run SHE_CTE 0.2 SHE_CTE_EstimateShear --data_images $DATA_IM --psf_images_and_tables $PSF_IMTAB --segmentation_images $SEG_IM --detections_tables $DTC_TAB --galaxy_population_priors_table $GP_TAB --calibration_parameters_product $CAL_PROD --calibration_parameters_listfile $CAL_LF --shear_estimates_product $SE_PROD --shear_estimates_listfile $SE_LF --workdir $WORKDIR --logdir $LOGDIR"

echo $CMD
`$CMD`

if [ $? -ne 0 ]; then
    exit
fi

CMD="E-Run SHE_CTE 0.2 SHE_CTE_ValidateShear --shear_estimates_product $SE_PROD --shear_estimates_listfile $SE_LF --shear_validation_statistics_table $SV_TAB --validated_shear_estimates_table $VSE_TAB --workdir $WORKDIR --logdir $LOGDIR"

echo $CMD
`$CMD`

