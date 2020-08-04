#!/bin/sh

SOURCEDIR=/mnt/cephfs/share/SC7_migrated/SC7M_workdir

# Check we're given one argument
if [ "$#" -eq 1 ]; then
    TARGETDIR=$1
else
    if [ "$#" -eq 2 ]; then
        SOURCEDIR=$1
        TARGETDIR=$2
    else
        echo "ERROR: Acceptable manners to call clone_workdir.sh are: "
        echo "    clone_workdir.sh TARGETDIR"
        echo "    clone_workdir.sh SOURCEDIR TARGETDIR"
        exit 1
    fi
fi

# Check that the target directory doesn't already exist
if [ -d "$TARGETDIR" ]; then
  echo "The target directory already exists! Please remove it and try again."
  exit 1
fi

# Keep tract of the directory we start in so we can return to it
STARTDIR=`pwd`

mkdir -p $TARGETDIR
cd $TARGETDIR
if [ $? -ne 0 ]; then
    exit 1
fi

mkdir data
ln -s $SOURCEDIR/data/* data/
ln -s $SOURCEDIR/* .

echo "Please ignore the 'ln: failed to create symbolic link './data': File exists' error - it's expected"

chmod a+x . data
chmod a+rw . * */*

cd $STARTDIR

exit 0