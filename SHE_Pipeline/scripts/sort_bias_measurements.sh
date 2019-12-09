#!/bin/bash

for TAG in Ep0Pp0Sp0 Em2Pp0Sp0 Em1Pp0Sp0 Ep1Pp0Sp0 Ep2Pp0Sp0 Ep0Pp0Sm2 Ep0Pp0Sm1 Ep0Pp0Sp1 Ep0Pp0Sp2
do
  NEW_TAG=${TAG}Tp0
  cp $TAG/shear_bias_measurements.xml shear_bias_measurements_${NEW_TAG}.xml
  cp -r $TAG/data .
done

for TAG in Ep0Pm2Sp0 Ep0Pm1Sp0 Ep0Pp1Sp0 Ep0Pp2Sp0
do
  NEW_TAG=${TAG}Tp0
  cp Ep0Pp0Sp0/shear_bias_measurements.xml shear_bias_measurements_${NEW_TAG}.xml
done

for TAG in Tm2 Tm1 Tp1 Tp2
do
  NEW_TAG=Ep0Pp0Sp0${TAG}
  cp $TAG/shear_bias_measurements.xml shear_bias_measurements_${NEW_TAG}.xml
  cp -r $TAG/data .
done

for TAG in CO WB COWB
do
  NEW_TAG=$TAG
  cp $TAG/shear_bias_measurements.xml shear_bias_measurements_${!NEW_TAG}.xml
  cp -r $TAG/data .
done
