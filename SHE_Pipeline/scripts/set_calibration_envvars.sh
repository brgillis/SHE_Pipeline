# This file must be sourced, not executed, so these variables will be
# available to the calling script. This shoul

export WORKDIR="/home/user/euclid-ial/workspace/tests/she_calibration"
export LOGDIR=$WORKDIR"/logdir"

# Inputs

export PSF_CAL="mock_psf_calibration_products.json"
export AOCS_PROD="mock_aocs_time_series_products.json"
export MT_PROD="mock_mission_time_products.json"
export GP_TAB="mock_galaxy_population_priors_table.bin"
export CAL_PROD="mock_calibration_parameters_product.bin"
export SV_TAB="mock_shear_validation_statistics_table.bin"

# Intermediate products/listfiles

export SEG_IM="mock_segmentation_products.json"
export DAL_TAB="mock_details_tables.json"
export DTC_TAB="mock_detections_tables.json"
export PSF_PROD="mock_psf_images_and_tables.bin"
export SE_PROD="mock_shear_estimates.bin"
export VSE_PROD="mock_validated_shear_estimates.bin"
