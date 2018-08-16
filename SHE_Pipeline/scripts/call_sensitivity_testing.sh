#/bin/bash

ISF="AUX/SHE_Pipeline/bias_measurement_isf.txt"
CFG_TEMPLATE_HEAD="AUX/SHE_GST_PrepareConfigs/Sensitivity"
CFG_TEMPLATE_TAIL="Template.conf"

# WORKSPACE_ROOT="/euclid/workspace_"
# ARCHIVE_DIR="/mnt/lustre/euclid_archive"

WORKSPACE_ROOT="/home/user/Work/workspace_"
ARCHIVE_DIR="/home/user/Work/sens_archive"

for TAG in Ep0Pp0Sp0 Ep1Pp0Sp0 Ep2Pp0Sp0 Em1Pp0Sp0 Em2Pp0Sp0 Ep0Pp1Sp0 Ep0Pp2Sp0 Ep0Pm1Sp0 Ep0Pm2Sp0 Ep0Pp0Sp1 Ep0Pp0Sp2 Ep0Pp0Sm1 Ep0Pp0Sm2
do

CMD="E-Run SHE_Pipeline 0.4.2 SHE_Pipeline_Run --pipeline bias_measurement --isf $ISF --isf_args config_template $CFG_TEMPLATE_HEAD$TAG$CFG_TEMPLATE_TAIL --workdir $WORKSPACE_ROOT$TAG --config_args SHE_CTE_MeasureBias_archive_dir $ARCHIVE_DIR"

echo "Executing command: $CMD"
eval $CMD

done
