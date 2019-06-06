#!/bin/sh

SOURCEDIR=/mnt/cephfs/share/sc4-big-workdir

# Check we're given one argument
if [ "$#" -eq 1 ]; then
    TARGETDIR=$1
else
    if [ "$#" -eq 2 ]; then
        SOURCEDIR=$1
        TARGETDIR=$2
    fi
    echo "ERROR: Acceptable manners to call clone_workdir.sh are: "
    echo "    clone_workdir.sh TARGETDIR"
    echo "    clone_workdir.sh SOURCEDIR TARGETDIR"
    exit 1
fi

# Check that the target directory doesn't already exist
if [ -d "$1" ]; then
  echo "The target directory already exists! Please remove it and try again."
  exit 1
fi

# Keep tract of the directory we start in so we can return to it
STARTDIR=`pwd`

mkdir -p $TARGETDIR
cd $TARGETDIR

mkdir data
ln -s $SOURCEDIR/data/* data/
ln -s $SOURCEDIR/* .

chmod a+x data
chmod a+rw * */*

cd $STARTDIR

exit 0