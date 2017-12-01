# This file must be sourced, not executed, so these variables will be
# available to the calling script

export WORKDIR="/home/user/euclid-ial/workspace/tests/she_analysis"
export LOGDIR=$WORKDIR"/logdir"

# Input products/listfiles

export DATA_IM_LF="sim_data_images.json"
export PSF_CAL_LF="mock_psf_calibration_products.json"
export SEG_IM_LF="mock_segmentation_products.json"
export DAL_LF="mock_details_tables.json"
export DTC_LF="mock_detections_tables.json"
export AST_LF="mock_astrometry_products.json"
export AOCS_LF="mock_aocs_time_series_products.json"
export MT_LF="mock_mission_time_products.json"
export GP_PROD="mock_galaxy_population_priors_table.bin"
export CAL_PROD="mock_calibration_parameters_product.bin"
export SV_PROD="mock_shear_validation_statistics_table.bin"

# Intermediate products/listfiles

export PSF_LF="mock_psf_images_and_tables.bin"
export SE_PROD="mock_shear_estimates.bin"
export VSE_PROD="mock_validated_shear_estimates.bin"
