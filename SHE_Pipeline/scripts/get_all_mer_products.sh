#/bin/bash

DATASETRELEASE=MER_GSIR_SWF1_R3_V1
# OBS_ID=10351

QUERY="Header.ManualValidationStatus!=INVALID&&Header.DataSetRelease=$DATASETRELEASE"

if [ -v ${OBS_ID+x} ]
then
  QUERY=$QUERY"&&Data.ObservationIdList==$OBS_ID"
fi

echo "Query: $QUERY"

# Get the DpdMerFinalCatalog product and fits files
python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdMerFinalCatalog --query $QUERY

# Get the DpdMerSegmentationMap product and fits files
python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdMerSegmentationMap --query $QUERY
