#/bin/bash

BASEDIR=$(dirname $(realpath "$0"))

python $BASEDIR/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdSheValidatedMeasurements --query "Header.DataSetRelease=SC8_MAIN_V0"

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

  if [ $OBS_ID != all ]; then
    QUERY=$QUERY"&&Data.ObservationId==$PICKED_OBS_ID"
  fi

  echo "Query: $QUERY_FOR_STACK"

  # Get the DpdSheValidatedMeasurements product and fits files
  CMD='python "'$DATAPROD_RETRIEVAL_SCRIPT'" --username '`cat $HOME/.username.txt`' --password '`cat $HOME/.password.txt`' --project TEST --data_product DpdSheValidatedMeasurements --query "'QUERY'"'
  echo "Command: $CMD"
  eval $CMD

  echo "Query: $QUERY_FOR_CALIB"

  # Get the DpdSheLensMcChains product and fits files
  CMD='python "'$DATAPROD_RETRIEVAL_SCRIPT'" --username '`cat $HOME/.username.txt`' --password '`cat $HOME/.password.txt`' --project TEST --data_product DpdSheLensMcChains --query "'QUERY'"'
  echo "Command: $CMD"
  eval $CMD

done
