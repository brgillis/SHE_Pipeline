#/bin/bash

DATASETRELEASE=SC8_MAIN_V0
# OBS_ID=10351 # When called at command-line, preface with OBS_ID=... to just query for a specific observation

QUERY="Header.ManualValidationStatus.ManualValidationStatus!=\"INVALID\"&&Header.DataSetRelease=$DATASETRELEASE"

# Iterate over observation IDs
for PICKED_OBS_ID in $OBS_ID; do

if [ ! -z ${OBS_ID+x} ]
then
  QUERY_FOR_OBS=$QUERY"&&Data.ObservationSequence.ObservationId==$OBS_ID"
fi

echo "Query: $QUERY"

BASEDIR=$(dirname $(realpath "$0"))

# Get the DpdVisStackedFrame product and fits files
CMD='python '$BASEDIR'/dataProductRetrieval_SC8.py --username '`cat $HOME/.username.txt`' --password '`cat $HOME/.password.txt`' --project TEST --data_product DpdVisStackedFrame --query "'$QUERY'"'
echo "Command: $CMD"
eval $CMD

# Get the DpdVisCalibratedFrame product and fits files
CMD='python '$BASEDIR'/dataProductRetrieval_SC8.py --username '`cat $HOME/.username.txt`' --password '`cat $HOME/.password.txt`' --project TEST --data_product DpdVisCalibratedFrame --query "'$QUERY'"'
echo "Command: $CMD"
eval $CMD

done
