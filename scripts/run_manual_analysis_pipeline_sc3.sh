#!/usr/bin/bash

if [ $# -gt 0 ]; then
  SCRIPTDIR=$1
else
  SCRIPTDIR=/home/user/Work/Projects/SHE_Pipeline/scripts
fi

source $SCRIPTDIR/set_analysis_envvars_sc3.sh

CMD="E-Run SHE_CTE 0.3 SHE_CTE_FitPSFs --data_images $DATA_IM_LF --detections_tables $DTC_LF --psf_images_and_tables $PSF_LF --workdir $WORKDIR --logdir $LOGDIR"

echo "Fitting PSFs with command:"
echo $CMD
eval $CMD

if [ $? -ne 0 ]; then
    exit
fi

CMD="E-Run SHE_CTE 0.3 SHE_CTE_EstimateShear --data_images $DATA_IM_LF --stacked_image $DATA_STACK_PROD --psf_images_and_tables $PSF_LF --segmentation_images $SEG_LF --stacked_segmentation_image $SEG_STACK_F --detections_tables $DTC_LF --shear_estimates_product $SE_PROD --workdir $WORKDIR --logdir $LOGDIR"

echo "Estimating shear with command:"
echo $CMD
eval $CMD

if [ $? -ne 0 ]; then
    exit
fi

CMD="E-Run SHE_CTE 0.3 SHE_CTE_ValidateShear --shear_estimates_product $SE_PROD --cross_validated_shear_estimates_product $CVSE_PROD --workdir $WORKDIR --logdir $LOGDIR"

echo "Validating shear with command:"
echo $CMD
eval $CMD

