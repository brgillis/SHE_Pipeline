Scripts
=======

.. contents::

In addition to Elements programs, this project contains various scripts which serve useful functions related to running pipelines. This page documents the most important scripts for this purpose. Be aware that these scripts are generally "rougher" than the distributed programs - they aren't coded to as high a standard, have inconsistent interfaces with each other, may produce spurious error messages, and aren't included in the automated testing process.

Main Scripts Available
----------------------

-  `clone_workdir.sh <clone_workdir.sh_>`_ : Symbolically links the contents of a template work directory and its subdirectories to a target location.
-  `create_listfiles <create_listfiles_>`_ : Generates listfiles and ISFs for input to the SHE Analysis pipeline for data products found in a given directory.
-  `get_all_*_products.sh <get_all_*_products.sh_>`_ : Downloads a selection of data products from the EAS.

Using the scripts
-----------------

``clone_workdir.sh``
~~~~~~~~~~~~~~~~~~~~

This script symbolically links the contents of a template work directory and its subdirectories to a target location. This ensures that the subdirectories themselves are not symbolically linked, which would otherwise result in any data writing to them instead write to the subdirectories of the template work directory.

**Running the script**

To run the clone_workdir.sh script, the following command can be called (assuming that this project is installed in the standard location):

.. code:: bash

   $HOME/Work/Projects/SHE_IAL_Pipelines/SHE_Pipeline/scripts/clone_workdir.sh <template_workdir> <target_workdir>

with the following required positional arguments:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Argument
     - Description
   * - ``<template_workdir>``
     - The fully-qualified path to the template work directory, which contains the data you desired to be symbolically linked to the new work directory.
   * - ``<target_workdir>``
     - The path (relative of fully-qualified) to the new work directory you wish to create. This must be the name of a directory which does not yet exist; however, its parent directory must already exist.

**Example**

The following example will clone a template work directory for the SHE Shear Calibration pipeline which is stored on the SDC-UK WebDAV server to a new work directory within the user's ``$HOME`` directory:

.. code:: bash

   $HOME/Work/Projects/SHE_IAL_Pipelines/SHE_Pipeline/scripts/clone_workdir.sh /mnt/webdav/PF-SHE/example_data/Shear_Cal_template_workdir/ $HOME/test_workdir

This requires that this project is installed in the standard location at ``$HOME/Work/Projects/SHE_IAL_Pipelines`` and that the SDC-UK WebDAV server is mounted and readable at the location ``/mnt/webdav/`` (see `instructions here <guide_webdav.html>`__). Modify these as appropriate for other locations.

``create_listfiles``
~~~~~~~~~~~~~~~~~~~~~~~

This script searches through the current directory for all ``.xml`` data products, identifies them by type, sorts them according to the Observation, Pointing, and/or Tile they correspond to, and generates appropriate listfiles and ISFs to be used as input for the SHE Analysis pipeline.

**Running the script**

To run the create_listfiles.py script on the contents of a directory, the following commands can be called (assuming that this project is installed in the standard location):

.. code:: bash

   cd <workdir>
   E-Run SHE_IAL_Pipelines 8.2 python $HOME/Work/Projects/SHE_IAL_Pipelines/SHE_Pipeline/scripts/create_listfiles

where ``<workdir>>`` is the directory you wish to run this script on. Note that this script must be run via E-Run within an EDEN 2.1 environment.

**Example**

(**TODO** when an appropriate template directory is set up.)

``get_all_*_products.sh``
~~~~~~~~~~~~~~~~~~~~~~~~~

These scripts are used to `download data products of different types from the Euclid Archive Server (EAS) <guide_eas.html>`__. The respective products downloaded are:

* ``get_all_mer_products.sh``: ``DpdMerFinalCatalog`` and ``DpdMerSegmentationMap``
* ``get_all_phz_products.sh``: ``DpdPhzPfOutputCatalog``
* ``get_all_she_products.sh``: ``DpdSheValidatedMeasurements`` and ``DpdSheLensMcChains``
* ``get_all_sim_products.sh``: ``DpdTrueUniverseOutput``
* ``get_all_vis_products.sh``: ``DpdVisStackedFrame`` and ``DpdVisCalibratedFrame``

Each script downloads products only from the ``DataSetRelease`` specified within the script (presently ``SC8_MAIN_V0``). To download products from a different ``DataSetRelease``, it will be necessary to copy the desired script, modify it, and run the copy.

By default, each script downloads all available data for the ``DataSetRelease``. This can be limited to a single Observation (in the case of SHE, SIM, and VIS data) or Tile (in the case of MER and PHZ data) through setting the environment variable ``OBS_ID`` or ``TILE_ID`` respectively when the script is executed.

**Running the scripts**

Before running these scripts, it is necessary to set up two files in your home directory, containing your ESAC username and password:

* ``$HOME/.username.txt``
* ``$HOME/.password.txt``

The username file can be created with a command such as:

.. code:: bash

   echo <username> > $HOME/.username.txt

For the password file, this is not recommended for security reasons, as doing this will result in your password being stored in plaintext in your bash history. Instead, it is recommended to create this file with your text editor of choice (e.g. ``vim``), and then delete it after use.

To run one of these scripts to download data to a desired directory, a command such as the following can now be used (assuming that this project is installed in the standard location):

.. code:: bash

   cd <workdir>
   [OBS_ID=<obs_id> OR TILE_ID=<tile_id>] $HOME/Work/Projects/SHE_IAL_Pipelines/SHE_Pipeline/scripts/<script>

where ``<workdir>`` is the directory you wish to download data to, either ``<obs_id>`` is the ``ObservationId`` (in the case of SHE, SIM, and VIS data) you wish to get data for or ``<tile_id>`` is the TileIndex (in the case of MER and PHZ data) you wish to get data for, and ``<script>`` is the filename of the specific script you wish to run.

**Example**

The following example will download SHE data for a single observation (with ``ObservationId`` 25463) to a desired directory:

.. code:: bash

   echo <username> > $HOME/.username.txt # Only necessary if not already present
   vim $HOME/.password.txt # Enter the password via text editor
   mkdir -p $HOME/test_workdir
   cd $HOME/test_workdir
   OBS_ID=25463 $HOME/Work/Projects/SHE_IAL_Pipelines/SHE_Pipeline/scripts/get_all_she_products.sh
   shred -u $HOME/.password.txt # Delete the file using ``shred`` to make sure the password is completely deleted

Similarly, the following code will download MER data for a single tile (with ``TileIndex`` 79170) to a desired directory:

.. code:: bash

   echo <username> > $HOME/.username.txt # Only necessary if not already present
   vim $HOME/.password.txt # Enter the password via text editor
   mkdir -p $HOME/test_workdir
   cd $HOME/test_workdir
   TILE_ID=79170 $HOME/Work/Projects/SHE_IAL_Pipelines/SHE_Pipeline/scripts/get_all_mer_products.sh
   shred -u $HOME/.password.txt # Delete the file using ``shred`` to make sure the password is completely deleted
