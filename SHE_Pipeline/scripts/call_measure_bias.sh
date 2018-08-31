#/bin/bash

ARCHIVE_DIR="/euclid/euclid-ial/sens_archive"

for TAG in Ep0Pp0Sp0 Ep1Pp0Sp0 Ep2Pp0Sp0 Em1Pp0Sp0 Em2Pp0Sp0 Ep0Pp1Sp0 Ep0Pp2Sp0 Ep0Pm1Sp0 Ep0Pm2Sp0 Ep0Pp0Sp1 Ep0Pp0Sp2 Ep0Pp0Sm1 Ep0Pp0Sm2
do

    CMD="E-Run SHE_CTE 0.6.4 SHE_CTE_MeasureBias --workdir $ARCHIVE_DIR/$TAG --archive_dir $ARCHIVE_DIR/$TAG --bootstrap_seed 1 --shear_bias_measurements shear_bias_measurements.xml"

    echo "Executing command: $CMD"
    eval $CMD

done
