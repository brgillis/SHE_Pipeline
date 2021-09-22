#/bin/bash

DATASETRELEASE=SC8_MAIN_V0
# OBS_ID=10351 # When called at command-line, preface with OBS_ID=... to just query for a specific observation

QUERY="Header.ManualValidationStatus.ManualValidationStatus!=\"INVALID\"&&Header.DataSetRelease=$DATASETRELEASE"

if [ -z ${OBS_ID+x} ]; then
  OBS_ID=all
fi

BASEDIR=$(dirname $(realpath "$0"))

# Iterate over observation IDs
for PICKED_OBS_ID in $OBS_ID; do

  if [ $OBS_ID != all ]; then
    QUERY_FOR_STACK=$QUERY"&&Data.ObservationId==$OBS_ID"
    QUERY_FOR_CALIB=$QUERY"&&Data.ObservationSequence.ObservationId==$OBS_ID"
  else
    QUERY_FOR_STACK=$QUERY
    QUERY_FOR_CALIB=$QUERY
  fi

  echo "Query: $QUERY_FOR_OBS"

  # Get the DpdVisStackedFrame product and fits files
  CMD='python '$BASEDIR'/dataProductRetrieval_SC8.py --username '`cat $HOME/.username.txt`' --password '`cat $HOME/.password.txt`' --project TEST --data_product DpdVisStackedFrame --query "'$QUERY_FOR_STACK'"'
  echo "Command: $CMD"
  eval $CMD

  # Get the DpdVisCalibratedFrame product and fits files
  CMD='python '$BASEDIR'/dataProductRetrieval_SC8.py --username '`cat $HOME/.username.txt`' --password '`cat $HOME/.password.txt`' --project TEST --data_product DpdVisCalibratedFrame --query "'$QUERY_FOR_CALIB'"'
  echo "Command: $CMD"
  eval $CMD

done
