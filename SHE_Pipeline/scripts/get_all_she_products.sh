#/bin/bash

BASEDIR=$(dirname $(realpath "$0"))

python $BASEDIR/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdSheValidatedMeasurements --query "Header.DataSetRelease=SC8_MAIN_V0"
