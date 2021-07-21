#/bin/bash

python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdSheValidatedMeasurements --query "Header.DataSetRelease=SC8_MAIN_V0"