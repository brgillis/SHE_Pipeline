Programs
========

.. contents::

Main Programs Available
-----------------------

-  `SHE_Pipeline_Run <SHE_Pipeline_Run_>`_ : Triggers a run of a desired SHE pipeline
-  `SHE_Pipeline_RunBiasParallel <SHE_Pipeline_RunBiasParallel_>`_ : Executes the SHE Shear Calibration pipeline locally, without use of the IAL pipeline runner


Running the software
--------------------

.. _SHE_Pipeline_Run:

``SHE_Pipeline_Run``
~~~~~~~~~~~~~~~~~~~~


**Running the Program on EDEN/LODEEN**

To run the ``SHE_Pipeline_Run`` program with Elements use the following command:

.. code:: bash

    E-Run SHE_IAL_Pipelines 8.2 SHE_Pipeline_Run --pipeline <pipeline> --workdir <workdir> [--cluster] [--server_url <serverurl>] [--server_config <server_config>] [--isf <isf>] [--isf_args <isf_args>] [--config <config>] [--config_args <config_args>] [--plan_args <plan_args>] [--log-file <filename>] [--log-level <value>]

with the following options:


**Common Elements Arguments**

.. list-table::
   :widths: 15 50 10 25
   :header-rows: 1

   * - Argument
     - Description
     - Required
     - Default
   * - --workdir ``<path>``
     - Name of the working directory, where input data is stored and output data will be created.
     - yes
     - N/A
   * - --log-file ``<filename>``
     - Name of a filename to store logging data in, relative to the workdir. If not provided, logging data will only be output to the terminal. Note that this will only contain logs directly from the run of this executable. Logs of executables called during the pipeline execution will be stored in the "logs" directory of the workdir.
     - no
     - None
   * - --log-level ``<level>``
     - Minimum severity level at which to print logging information. Valid values are DEBUG, INFO, WARNING, and ERROR. Note that this will only contain logs directly from the run of this executable. The log level of executables called during pipeline execut will be set based on the configuration of the pipeline server (normally INFO).
     - no
     - INFO


**Input Arguments**

.. _filename_keywords:

The following arguments expect a filename to be provided. This filename should be either:

#. Relative to the workdir or current directory
#. Fully-qualified
#. Prefixed with one of the following special prefixes to indicate where it can be found:

   * ``AUX/`` - Search for the file within the path defined by the environmental variable ``ELEMENTS_AUX_PATH``, which is the combination of the "auxdir" folders of all projects used within the pipeline.
   * ``CONF/`` - Search for the file within the path defined by the environmental variable ``ELEMENTS_CONF_PATH``, which is the combination of the "conf" folders of all projects used within the pipeline.
   * ``WEB/`` - Search for the file on the SDC-UK WebDAV file system (see instructions to mount here: `SDC-UK_webdav <guide_webdav.html>`__), relative to the PF-SHE directory on it. This file will be downloaded and the path to the locally downloaded version will be used. Note that pipeline runs on any cluster will not have internet access, so this can only be used for local runs.

.. list-table::
   :widths: 15 50 10 25
   :header-rows: 1

   * - Argument
     - Description
     - Required
     - Default
   * - ``--isf <filename>``
     - ``.txt`` file listing filenames to be provided to input ports of the pipeline. This file should have one port per line, with format ``<port_name>=<filename>``, e.g. ``my_input_port=MyInputFilename.xml``. If the ``--isf_args`` argument is used, any values for input ports passed to that will override values in this file.
     - no
     - None (all input ports will take default values provided in the \<pipeline\_name\>_isf.txt file in ``SHE_Pipeline/auxdir/SHE_Pipeline``, unless overridden through use of the ``isf_args`` argument.))
   * - ``--config <filename>``
     - ``.txt`` file containing configuration options to be used for one or more task within the pipeline, ``.xml`` data product or pointing to such a text file, or .json listfile (Cardinality 0-1) either pointing to such a data product or empty. The text file should contain one option per line, in the format ``<option>=<value>``, e.g. ``SHE_Pipeline_profile=True``. If the ``--config_args`` argument is used, any values for options passed to that will override values in this file.
     - no
     - None (equivalent to providing an empty listfile, which results in default values being used for all options)
   * - ``--server_config <filename>``
     - ``.conf`` file containing the configuration of a pipeline server to use for this run. This should not be supplied in conjunction with ``--server_url``, as that will submit a run to a running pipeline server, which will already have its own configuration set up.
     - no
     - Not used, unless ``--use_debug_server_config`` is supplied, in which case ``SHE_Pipeline/auxdir/SHE_Pipeline/debug_server_config.txt`` will be used.


**Output Arguments**

N/A - The names of output files from the pipeline run are determined from the names of the output ports in the Pipeline Script. See the `documentation for the respective pipelines <pipelines.html>`__ for information on their outputs.


**Options**


.. list-table::
   :widths: 15 50 10 25
   :header-rows: 1

   * - Argument
     - Description
     - Required
     - Default
   * - ``--pipeline <pipeline_name>``)
     - The name of the pipeline to be run. The following values are allowed. which call one of the primary pipelines: ``analysis`` (`SHE Analysis pipeline <pip_analysis.html>`__), ``reconciliation`` (`SHE Reconciliation pipeline <pip_reconciliation.html>`__), ``calibration`` (`Shear Calibration pipeline <pip_shear_calibration.html>`__), ``validation`` (`SHE Global Validation pipeline <pip_global_validation.html>`__). Additionally, the following values are allowed, which call special pipelines used for debugging and development purposes: ``analysis_after_remap``, ``analysis_with_validation``, ``analysis_after_remap_with_validation``, ``calibration_residuals``, ``scaling_experiments``.
     - yes
     - N/A
   * - ``--cluster`` (``store true``)
     - If set, will enable a workaround for a bug present on some clusters, which otherwise would result in the pipeline server's user running the pipeline not having necessary write access to files in the workdir.
     - no
     - False
   * - ``--server_url <server_url>``
     - The URL of the pipeline server to submit this run to. Not used if the argument ``--use_debug_server_config`` is provided, which triggers a local run.
     - no
     - ``http://ial:50000``
   * - ``--isf_args <port_1> <file_1> [<port_2> <file_2> ...]``
     - A list of paired items, where the first item of each pair is the name of the input port, and the second is the filename for it, e.g. ``--isf_args ksb_training_data my_ksb_training_data.xml lensmc_training_data my_lensmc_training_data.xml``. Using this argument will result in a new ISF file being created and used with these values overriding those in the file provided with the ``--isf`` argument and/or the default ISF for this pipeline.
     - no
     - None (if the ``--isf`` file is provided, will use input ports from that. Any input ports unspecified by that will use default filenam values provided in the \<pipeline\_name\>_isf.txt file in ``SHE_Pipeline/auxdir/SHE_Pipeline``)
   * - ``--config_args <option_1> <value_1> [<option_2> <value_2> ...]``
     - A list of paired items, where the first item of each pair is the name of the configuration option, and the second is the value for it, e.g. ``--config_args SHE_CTE_ObjectIdSplit_batch_size 10 SHE_CTE_ObjectIdSplit_max_batches 2``. Using this argument will result in a new ISF file being created and used with these values overriding those in the file provided with the ``--config`` argument.
     - no
     - None (if the ``--config`` file is provided, will use options from that. Otherwise, configuration options will take default values defined by the executables to which they are relevant.)
   * - ``--plan_args <option_1> <value_1> [<option_2> <value_2> ...]``
     - Can only be used when the Calibration pipeline is triggered. A list of paired items, where the first item of each pair is the name of an option in the simulation plan, and the second is the value for it, e.g. ``--plan_args MSEED_MIN 1 MSEED_MAX 16 NSEED_MIN 1 NSEED_MAX 16 NUM_GALAXIES 16``. Using this argument will result in a new simulation plan file being created and used with these values overriding those in the file provided to the ``simulation_plan`` input port.
     - no
     - None (The file provided to the ``simulation_plan`` input port will be used unmodified.)


**Inputs**


``isf``:

**Description:** The primary input to any Euclid pipeline is a "data" file. To avoid ambiguity with other uses of "data," we'll be using the old name for it: an Interface Specification File (ISF) here. The file which is provided to the IAL pipeline runner is a text file which lists options for the pipeline run, plus the names of input ports to the pipeline and the files they correspond to, e.g.:

.. code:: text

   workdir=/home/user/workspace/workdir
   logdir=logs
   pkgRepository=/cvmfs/euclid-dev.in2p3.fr/CentOS7/EDEN-2.1/opt/euclid/SHE_IAL_Pipelines/8.2/InstallArea/x86_64-conda_cos6-gcc73-o2g/auxdir/SHE_Shear_Analysis
   pipelineDir=/cvmfs/euclid-dev.in2p3.fr/CentOS7/EDEN-2.1/opt/euclid/SHE_IAL_Pipelines/8.2/InstallArea/x86_64-conda_cos6-gcc73-o2g/auxdir/SHE_Shear_Analysis
   edenVersion=Eden-2.1-dev

   ksb_training_data=test_ksb_training.xml
   lensmc_training_data=test_lensmc_training.xml
   pipeline_config=bias_measurement_config.txt
   mdb=mdb-SC8.xml

When provided to the IAL pipeline_runner.py script, the ISF is required to have both options for the run and input arguments in it, but the SHE_Pipeline_Run program is more flexible. It fills in the options based on:

* ``workdir``: Provided at command-line
* ``logdir``: Always "logs"
* ``pkgRepository`` and ``pipelineDir``: Installed location of the auxdir for the pipeline being run
* ``edenVersion``: Eden-2.1-dev if a develop version of code is being run, Eden-2.1 if a release version of code is being run

For the input ports, it takes, in order of descending priority:

#. Values provided at the command-line with the ``--isf_args`` option
#. Values in an ISF provided at the command-line with the ``--isf`` option
#. Values in the default ISF for the chosen pipeline

A call to SHE_Pipeline_run will thus look like:

.. code:: bash

   E-Run SHE_IAL_Pipelines 8.2 SHE_Pipeline_Run  --pipeline <pipeline> --workdir <workdir> [--isf <isf>] [--isf_args <isf_args>]

Here, ``<isf>`` is the filename of the non-default ISF to use for input ports, and can be either absolute or relative to the work directory. ``<isf_args>`` is a list of paired items, where the first item of each pair is the name of the input port, and the second is the filename for it, e.g. ``--isf_args ksb_training_data my_ksb_training_data.xml lensmc_training_data my_lensmc_training_data.xml``.

This program also allows for special keywords to be used in filenames within either the supplied ISF or provided ``--isf_args``: AUX/, CONF/, and WEB/, or for the filenames to be fully-qualified, relative to the current directory, or relative to the workdir, as `detailed above <filename_keywords_>`_.

The program will take any data product filenames provided as input, search for them, and symlink them to the work directory prior to starting the pipeline. For each data product specified as input, it will also attempt to locate any data containers (i.e. the files which contain the actual data) it points to. It searches in the same directory as the data product, the "data" subdirectory of the directory where the product is, its parent directory, and the "data" subdirectory of the parent directory, and then the above locations to try to find it. To ensure these files are found, the best practice is to always store them in the same directories as their corresponding products.

Once the program has found and sorted all input files, it will create an ISF to pass to the IAL ``pipeline_runner.py`` script with the new (symlinked) locations of all input files.

**Source:** A default ISF for each pipeline may be copied from the folder ``SHE_Pipeline/auxdir/SHE_Pipeline`` of this project and modified as desired.

.. _she_pipeline_run_config:

``config``:

**Description:**  The Euclid IAL pipeline runner only allows filenames to be passed as input arguments to tasks within each pipeline. This means that other types of arguments (e.g. ``--num_threads 4``) can't be passed directly to tasks. Instead, these arguments must be stored in a file, and this file's filename passed to the task. The name of a file to use for this can be provided with the ``--config`` argument. This should be one of the following:

#. The word "None" (without quotes), which signals that default values for all configuration parameters shall be used.
#. The filename of an empty ``.json`` listfile, which similarly indicates the use of all default values.
#. The filename of a ``.txt`` file in the workdir listing configuration parameters and values for executables in the current pipeline run. This shall have the one or more lines, each with the format ``SHE_MyProject_config_parameter = config_value``.
#. The filename of a ``.xml`` data product of format DpdSheAnalysisConfig, pointing to a text file as described above. The format of this data product is described in detail in the Euclid DPDD at https://euclid.esac.esa.int/dm/dpdd/latest/shedpd/dpcards/she\_analysisconfig.html.
#. The filename of a ``.json`` listfile which contains the filename of a ``.xml`` data product as described above.

Any of the latter three options may be used for equivalent functionality.

To aid this without requiring the user to write a file, this program has the functionality to set such arguments at the command-line through the ``--config_args`` option:

.. code:: bash

   E-Run SHE_IAL_Pipelines 8.2 SHE_Pipeline_Run --pipeline <pipeline> --workdir <workdir> --config <config> --config_args <config_args>

When ``--config_args`` is used, the helper script will override any arguments also present in the file provided to the ``--config`` argument, check all arguments for validity,  write a pipeline configuration file, and provide this file as input to the pipeline's ``pipeline_config`` input port. Each task within a SHE pipeline which makes use of any arguments passed this way is set up to read in this file and parse it for relevant arguments.

The ``--config_args`` argument takes a list of paired items. The first item of each pair is the name of an argument relevant to one or more tasks in the pipeline, and the second is the value for that argument, e.g. ``--config_args SHE_CTE_ObjectIdSplit_batch_size 10 SHE_CTE_ObjectIdSplit_max_batches 2``. Arguments with spaces in them must be enclosed in quotes, e.g. ``--config_args SHE_CTE_EstimateShear_methods "KSB REGAUSS"``.

See the documentation for specific programs for details on what configuration options are allowed for each program.

**Source:** One of the following:

#. May be generated manually, creating the ``.txt`` file with your text editor of choice.
#. Retrieved from the EAS, querying for a desired product of type DpdSheAnalysisConfig.
#. Specified in full through the use of the ``--config_args`` argument

``server_config``:

This file determines the setup for a pipeline server used for local runs. This is a text file in a standard configuration format, with one option per line, and each line having the format ``<option>=<value>``, e.g.:

.. code:: text

   pipelinerunner.messaging.socketType=ipc
   pipelinerunner.messaging.subSocketBindAddress=${PIPELINERUNNER_RUNID}_sub.sock
   pipelinerunner.messaging.pubSocketBindAddress=${PIPELINERUNNER_RUNID}_pub.sock

   pipelinerunner.pilots.genericLight.CPUcores=2
   pipelinerunner.pilots.genericLight.rssInMB=4132
   pipelinerunner.pilots.genericLight.walltimeInMin=4320
   pipelinerunner.pilots.genericLight.maxInstances=0
   pipelinerunner.pilots.genericLight.starveModeInPercent=0



**Outputs**

Outputs are determined by which pipeline is run. See documentation of the individual pipelines and their executables for information on output files.


.. _she_pipeline_run_example:

**Example**

In this section, we will provide some examples of using this program to trigger a local run of the SHE Shear Calibration pipeline. Examples of runs of other pipelines can be found in their respective documentation.

First, it is necessary to set up the input data for the pipeline run. This can be done expediently by recursively symlinking the contents of the directory containing example input data for the SHE Shear Calibration pipeline provided on SDC-UK's WebDAV server. Assuming that this project is installed at $HOME/Work/Projects/SHE_IAL_Pipelines, the WebDAV server is mounted at /mnt/webdav (if not already mounted, follow the  `instructions here <guide_webdav.html>`__), and the pipeline workdir will be $HOME/test_workdir, this can be done through:

.. code:: bash

   $HOME/Work/Projects/SHE_IAL_Pipelines/SHE_Pipeline/scripts/clone_workdir.sh /mnt/webdav/PF-SHE/example_data/Shear_Cal_template_workdir/ $HOME/test_workdir

This uses the ``clone_workdir.sh`` script, which symbolically links the contents of a template work directory and its sub-directories.

An example pipeline run can then be triggered through calling:

.. code:: bash

   E-Run SHE_IAL_Pipelines 8.2 SHE_Pipeline_Run --pipeline calibration --workdir $HOME/test_workdir --plan_args MSEED_MIN 1 MSEED_MAX 2 NSEED_MIN 1 NSEED_MAX 2 NUM_GALAXIES 2

This call uses default values for all input ports, which match the filenames provided in the template workdir, and default values for all pipeline configuration options. It overrides the default simulation plan with the arguments provided in the command-line, which tells the pipeline to run two batches of simulations, each simulating two galaxies. See documentation for the SHE Shear Calibration pipeline for further details on how the simulation plan and arguments for it functions.

This same pipeline run can also be triggered through the following command, which explicitly states the names of input files and pipeline configuration options:

.. code:: bash

   E-Run SHE_IAL_Pipelines 8.2 SHE_Pipeline_Run --pipeline calibration --workdir $HOME/test_workdir --isf_args config_template AUX/SHE_GST_PrepareConfigs/SensitivityEp0Pp0Sp0Template.conf ksb_training_data test_ksb_training.xml lensmc_training_data test_lensmc_training.xml momentsml_training_data None regauss_training_data=test_regauss_training.xml mdb sample_mdb-SC8.xml --config_args SHE_CTE_CleanupBiasMeasurement_cleanup True SHE_CTE_EstimateShear_methods "KSB LensMC MomentsML REGAUSS" SHE_CTE_MeasureBias_webdav_archive False SHE_CTE_MeasureStatistics_webdav_archive False --plan_args MSEED_MIN 1 MSEED_MAX 2 NSEED_MIN 1 NSEED_MAX 2 NUM_GALAXIES 2

``SHE_Pipeline_RunBiasParallel``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``SHE_Pipeline_RunBiasParallel`` is a replacement for the ``SHE_Pipeline_Run`` program, designed to run the SHE Shear Calibration pipeline directly, without reliance on the IAL pipeline runner. This was found to be necessary within the Shear Sensitivity Testing programme due to the IAL pipeline runner facing load limits.

By design, this program shares a common interface with ``SHE_Pipeline_Run`` as much as possible, and so this section will only detail the ways in which this program differs.


**Removed command-line arguments**

The following lists the command-line arguments that are used for ``SHE_Pipeline_Run``, but not ``SHE_Pipeline_RunBiasParallel``, along with reasoning for their removal.


.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Removed Argument
     - Reasoning
   * - ``--pipeline``
     - This program is designed only for the Calibration pipeline, and cannot run other pipelines. This would be equivalent to specifying ``--pipeline calibration``.
   * - ``--cluster``, ``--server_url``, and ``--server_config``
     - This program always runs the pipeline locally, and not through a pipeline server. As such, these arguments, which relate to running on a server, are not relevant to it.


**Example**

See the `section for examples <she_pipeline_run_example_>`_ of the ``SHE_Pipeline_Run`` program for set-up instructions of an example run. Rather than using the command presented there, this program can be used instead through a command such as:

.. code:: bash

   E-Run SHE_IAL_Pipelines 8.2 SHE_Pipeline_RunBiasParallel --workdir $HOME/test_workdir --plan_args MSEED_MIN 1 MSEED_MAX 2 NSEED_MIN 1 NSEED_MAX 2 NUM_GALAXIES 2
