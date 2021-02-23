#/bin/bash

DATASETRELEASE=VIS_SC8_GSIR_VIS_SWF1_R3_0

# Get the DpdVisStackedFrame product and fits files
python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdVisStackedFrame --query "Header.DataSetRelease=$DATASETRELEASE"

# Get the DpdVisCalibratedFrame product and fits files
python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdVisCalibratedFrame --query "Header.DataSetRelease=$DATASETRELEASE"


