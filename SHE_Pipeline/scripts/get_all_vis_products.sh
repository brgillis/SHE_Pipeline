#/bin/bash

DATASETRELEASE=SC8_MAIN_V0
# OBS_ID=10351 # When called at command-line, preface with OBS_ID=... to just query for a specific observation

QUERY="Header.ManualValidationStatus.ManualValidationStatus!=\"INVALID\"&&Header.DataSetRelease=$DATASETRELEASE"

if [ -z ${OBS_ID+x} ]; then
  OBS_ID=all
fi

DATAPROD_RETRIEVAL_SCRIPT=dataProductRetrieval_SC8.py

# Check some common locations
if [ -f "$(dirname $(realpath "$0"))/$DATAPROD_RETRIEVAL_SCRIPT" ]; then
  BASEDIR=$(dirname $(realpath "$0"))
elif [ -f "$HOME/Work/Projects/SHE_IAL_Pipelines/SHE_Pipeline/scripts/$DATAPROD_RETRIEVAL_SCRIPT" ]; then
  BASEDIR=$HOME/Work/Projects/SHE_IAL_Pipelines/SHE_Pipeline/scripts
elif [ -f "$HOME/bin/$DATAPROD_RETRIEVAL_SCRIPT" ]; then
  BASEDIR=$HOME/bin
else:
  echo Could not find retrieval script.
  exit 1
fi

DATAPROD_RETRIEVAL_SCRIPT=$BASEDIR/$DATAPROD_RETRIEVAL_SCRIPT

# Iterate over observation IDs
for PICKED_OBS_ID in $OBS_ID; do

  if [ $PICKED_OBS_ID != all ]; then
    QUERY_FOR_STACK=$QUERY"&&Data.ObservationId==$PICKED_OBS_ID"
    QUERY_FOR_CALIB=$QUERY"&&Data.ObservationSequence.ObservationId==$PICKED_OBS_ID"
  else
    QUERY_FOR_STACK=$QUERY
    QUERY_FOR_CALIB=$QUERY
  fi

  echo "Query: $QUERY_FOR_STACK"

  # Get the DpdVisStackedFrame product and fits files
  CMD='python "'$DATAPROD_RETRIEVAL_SCRIPT'" --username '`cat $HOME/.username.txt`' --password '`cat $HOME/.password.txt`' --project TEST --data_product DpdVisStackedFrame --query "'$QUERY_FOR_STACK'"'
  echo "Command: $CMD"
  eval $CMD

  echo "Query: $QUERY_FOR_CALIB"

  # Get the DpdVisCalibratedFrame product and fits files
  CMD='python "'$DATAPROD_RETRIEVAL_SCRIPT'" --username '`cat $HOME/.username.txt`' --password '`cat $HOME/.password.txt`' --project TEST --data_product DpdVisCalibratedFrame --query "'$QUERY_FOR_CALIB'"'
  echo "Command: $CMD"
  eval $CMD

done
