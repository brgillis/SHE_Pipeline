#/bin/bash

DATASETRELEASE=SC8_MAIN_V0
# TILE_ID=90346 # When called at command-line, preface with TILE_ID=... to just query for a specific tile

QUERY="Header.ManualValidationStatus.ManualValidationStatus!=\"INVALID\"&&Header.DataSetRelease=$DATASETRELEASE"

if [ -z ${TILE_ID+x} ]; then
  TILE_ID=all
fi

BASEDIR=$(dirname $(realpath "$0"))

# Iterate over tile IDs
for PICKED_TILE_ID in $TILE_ID; do

  if [ $PICKED_TILE_ID != all ]; then
    QUERY_FOR_TILE=$QUERY"&&Data.TileIndex==$PICKED_TILE_ID"
  else
    QUERY_FOR_TILE=$QUERY
  fi

  echo "Query: $QUERY_FOR_TILE"

  # Get the DpdMerFinalCatalog product and fits files
  CMD='python '$BASEDIR'/dataProductRetrieval_SC8.py --username '`cat $HOME/.username.txt`' --password '`cat $HOME/.password.txt`' --project TEST --data_product DpdMerFinalCatalog --query "'$QUERY_FOR_TILE'"'
  echo "Command: $CMD"
  eval $CMD

  # Get the DpdMerSegmentationMap product and fits files
  CMD='python '$BASEDIR'/dataProductRetrieval_SC8.py --username '`cat $HOME/.username.txt`' --password '`cat $HOME/.password.txt`' --project TEST --data_product DpdMerSegmentationMap --query "'$QUERY_FOR_TILE'"'
  echo "Command: $CMD"
  eval $CMD

done
