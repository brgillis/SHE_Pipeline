#!/usr/bin/bash

SCRIPTDIR=/home/user/Work/Projects/SHE_Pipeline/scripts

source $SCRIPTDIR/set_analysis_envvars.sh

CMD="E-Run SHE_CTE 0.3 SHE_CTE_FitPSFs --data_images $DATA_IM_LF --detections_tables $DTC_LF --astrometry_products $AST_LF --aocs_time_series_products $AOCS_LF --mission_time_products $MT_LF --psf_calibration_products $PSF_CAL_LF --psf_images_and_tables $PSF_LF --workdir $WORKDIR --logdir $LOGDIR"

echo "Fitting PSFs with command:"
echo $CMD
`$CMD`

if [ $? -ne 0 ]; then
    exit
fi

CMD="E-Run SHE_CTE 0.3 SHE_CTE_EstimateShear --data_images $DATA_IM_LF --psf_images_and_tables $PSF_LF --segmentation_images $SEG_IM_LF --detections_tables $DTC_LF --galaxy_population_priors_table $GP_PROD --calibration_parameters_product $CAL_PROD --shear_estimates_product $SE_PROD --workdir $WORKDIR --logdir $LOGDIR"

echo "Estimating shear with command:"
echo $CMD
`$CMD`

if [ $? -ne 0 ]; then
    exit
fi

CMD="E-Run SHE_CTE 0.3 SHE_CTE_ValidateShear --shear_estimates_product $SE_PROD --shear_validation_statistics_table $SV_PROD --validated_shear_estimates_table $VSE_PROD --workdir $WORKDIR --logdir $LOGDIR"

echo "Validating shear with command:"
echo $CMD
`$CMD`

