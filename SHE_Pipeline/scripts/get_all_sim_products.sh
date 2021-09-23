#/bin/bash

DATAPROD_RETRIEVAL_SCRIPT=dataProductRetrieval_SC8.py

# Check some common locations
if [ -f "$(dirname $(realpath "$0"))/$DATAPROD_RETRIEVAL_SCRIPT" ]; then
  BASEDIR=$(dirname $(realpath "$0"))
elif [ -f "$HOME/Work/Projects/SHE_IAL_Pipelines/SHE_Pipeline/scripts/$DATAPROD_RETRIEVAL_SCRIPT" ]; then
  BASEDIR=$HOME/Work/Projects/SHE_IAL_Pipelines/SHE_Pipeline/scripts
elif [ -f "$HOME/bin/$DATAPROD_RETRIEVAL_SCRIPT" ]; then
  BASEDIR=$HOME/bin
else:
  echo Could not find retrieval script.
  exit 1
fi

DATAPROD_RETRIEVAL_SCRIPT=$BASEDIR/$DATAPROD_RETRIEVAL_SCRIPT

for i in {0..100000}
do
	grep \<ObservationId\>$i\</ObservationId\> *.xml > /dev/null
	if [ $? -eq 1 ]; then
		continue
    fi
	QUERY='Header.DataSetRelease=SC8_MAIN_V0&&Data.EuclidPointingId=='$i'&&Header.ManualValidationStatus.ManualValidationStatus!="INVALID"&&Header.PipelineDefinitionId=="SIM-VIS"'
	echo "Query: $QUERY"
	python "'$DATAPROD_RETRIEVAL_SCRIPT'" --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdTrueUniverseOutput --query "$QUERY"
done


# python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdTrueUniverseOutput --query "Header.DataSetRelease=SC8_PF_VIS_79171_R19"

# python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdGalaxyCatalogProduct --query "Header.DataSetRelease=1.10.18_SC8_MAIN_STD"

# python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdGalaxyCatalogProduct --query "Header.DataSetRelease=1.10.19_SC8_MAIN_QSO"

# python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdGalaxyCatalogProduct --query "Header.DataSetRelease=1.10.15_SC8_MAIN_HIGHZ"

# python $HOME/bin/dataProductRetrieval_SC8.py --username `cat $HOME/.username.txt` --password `cat $HOME/.password.txt` --project TEST --data_product DpdStarsCatalogProduct --query "Header.DataSetRelease=v13_SC8_MAIN"
