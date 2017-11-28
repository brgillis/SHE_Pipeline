#!/usr/bin/bash

SCRIPTDIR=/home/user/Work/Projects/SHE_Pipeline/SHE_Pipeline/scripts

source $SCRIPTDIR/set_pipeline_envvars.sh

mkdir -p $WORKDIR
mkdir -p $LOGDIR

CMD="rm -rf $WORKDIR/EUC*.bin $WORKDIR/*.fits $WORKDIR/*.json $LOGDIR/*"

echo $CMD
`$CMD`

