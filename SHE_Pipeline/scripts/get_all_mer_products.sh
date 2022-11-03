#/bin/bash

DATASETRELEASE=SC8_MAIN_V0
# TILE_ID=90346 # When called at command-line, preface with TILE_ID=... to just query for a specific tile

QUERY="Header.ManualValidationStatus.ManualValidationStatus!=\"INVALID\"&&Header.DataSetRelease=$DATASETRELEASE"

if [ -z ${TILE_ID+x} ]; then
  TILE_ID=all
fi

if [ -z ${GET_SEG+x} ]; then
  GET_SEG=1
fi

if [ -z ${GET_CAT+x} ]; then
  GET_CAT=1
fi

DATAPROD_RETRIEVAL_SCRIPT=dataProductRetrieval_SC8.py

# Check some common locations
if [ -f "$(dirname $(realpath "$0"))/$DATAPROD_RETRIEVAL_SCRIPT" ]; then
  BASEDIR=$(dirname $(realpath "$0"))
elif [ -f "$HOME/Work/Projects/SHE_Pipeline/SHE_Pipeline/scripts/$DATAPROD_RETRIEVAL_SCRIPT" ]; then
  BASEDIR=$HOME/Work/Projects/SHE_Pipeline/SHE_Pipeline/scripts
elif [ -f "$HOME/bin/$DATAPROD_RETRIEVAL_SCRIPT" ]; then
  BASEDIR=$HOME/bin
else:
  echo Could not find retrieval script.
  exit 1
fi

DATAPROD_RETRIEVAL_SCRIPT=$BASEDIR/$DATAPROD_RETRIEVAL_SCRIPT

# Iterate over tile IDs
for PICKED_TILE_ID in $TILE_ID; do

  if [ $PICKED_TILE_ID != all ]; then
    QUERY_FOR_TILE=$QUERY"&&Data.TileIndex==$PICKED_TILE_ID"
  else
    QUERY_FOR_TILE=$QUERY
  fi

  echo "Query: $QUERY_FOR_TILE"

  # Get the DpdMerFinalCatalog product and fits files
  if [ $GET_CAT == 1 ]; then
    CMD='python "'$DATAPROD_RETRIEVAL_SCRIPT'" --username '`cat $HOME/.username.txt`' --password '`cat $HOME/.password.txt`' --project TEST --data_product DpdMerFinalCatalog --query "'$QUERY_FOR_TILE'"'
    echo "Command: $CMD"
    eval $CMD
  fi

  # Get the DpdMerSegmentationMap product and fits files
  if [ $GET_SEG == 1 ]; then
    CMD='python "'$DATAPROD_RETRIEVAL_SCRIPT'" --username '`cat $HOME/.username.txt`' --password '`cat $HOME/.password.txt`' --project TEST --data_product DpdMerSegmentationMap --query "'$QUERY_FOR_TILE'"'
    echo "Command: $CMD"
    eval $CMD
  fi

done
