#/bin/bash

DATASETRELEASE=SC8_MAIN_V0
# OBS_ID=10351 # When called at command-line, preface with OBS_ID=... to just query for a specific observation

QUERY="Header.ManualValidationStatus.ManualValidationStatus!=\"INVALID\"&&Header.DataSetRelease=$DATASETRELEASE"

if [ ! -z ${OBS_ID+x} ]
then
  QUERY=$QUERY"&&Data.ObservationSequence.ObservationId==$OBS_ID"
fi

echo "Query: $QUERY"

# Get the DpdVisStackedFrame product and fits files
CMD='python '$HOME'/bin/dataProductRetrieval_SC8.py --username '`cat $HOME/.username.txt`' --password '`cat $HOME/.password.txt`' --project TEST --data_product DpdVisStackedFrame --query "'$QUERY'"'
echo "Command: $CMD"
eval $CMD

# Get the DpdVisCalibratedFrame product and fits files
CMD='python '$HOME'/bin/dataProductRetrieval_SC8.py --username '`cat $HOME/.username.txt`' --password '`cat $HOME/.password.txt`' --project TEST --data_product DpdVisCalibratedFrame --query "'$QUERY'"'
echo "Command: $CMD"
eval $CMD
