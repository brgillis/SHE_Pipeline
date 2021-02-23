#/bin/bash

python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdGalaxyCatalogProduct --query "Header.DataSetRelease=1.9.11_GSIR_SWF1_SWF2"

python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdStarsCatalogProduct --query "Header.DataSetRelease=v10_SC8_SWF1_SWF2"

