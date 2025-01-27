#/bin/bash

DATASETRELEASE=SC8_MAIN_V0
# TILE_ID=90346 # When called at command-line, preface with TILE_ID=... to just query for a specific tile
TILE_ID=90346
QUERY="Header.ManualValidationStatus.ManualValidationStatus!=\"INVALID\"&&Header.DataSetRelease=$DATASETRELEASE"

if [ ! -z ${TILE_ID+x} ]
then
  QUERY=$QUERY"&&Data.TileIndex==$TILE_ID"
fi

echo "Query: $QUERY"

# Get the DpdPhzpfOutputCatalog product and fits files
CMD='python $HOME/Work/Projects/SHE_Pipeline/SHE_Pipeline/scripts/dataProductRetrieval_SC8.py --username '`cat $HOME/.username.txt`' --password '`cat $HOME/.password.txt`' --project TEST --data_product DpdPhzPfOutputCatalog --query "'$QUERY'"'
echo "Command: $CMD"
eval $CMD
