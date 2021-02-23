#/bin/bash

DATASETRELEASE=MER_GSIR_SWF1_R3_V1

# Get the DpdVisStackedFrame product and fits files
python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdMerFinalCatalog --query "Header.DataSetRelease=$DATASETRELEASE"

# Get the DpdVisCalibratedFrame product and fits files
python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdMerSegmentationMap --query "Header.DataSetRelease=$DATASETRELEASE"
