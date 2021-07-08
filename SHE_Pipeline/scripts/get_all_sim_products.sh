#/bin/bash

python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdTrueUniverseOutput --query "Header.DataSetRelease=SC8_PF_VIS_79171_R19"

# python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdGalaxyCatalogProduct --query "Header.DataSetRelease=1.10.18_SC8_MAIN_STD"

# python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdGalaxyCatalogProduct --query "Header.DataSetRelease=1.10.19_SC8_MAIN_QSO"

# python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdGalaxyCatalogProduct --query "Header.DataSetRelease=1.10.15_SC8_MAIN_HIGHZ"

# python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdStarsCatalogProduct --query "Header.DataSetRelease=v13_SC8_MAIN"
