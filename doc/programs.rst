Programs
========


Main Programs Available
-----------------------

-  `SHE_Pipeline_Run <SHE_Pipeline_Run_>`_ : Triggers a run of a desired SHE pipeline
-  `SHE_Pipeline_RunBiasParallel <SHE_Pipeline_RunBiasParallel_>`_ : Executes the SHE Shear Calibration pipeline locally, without use of the IAL pipeline runner


Running the software
--------------------


``SHE_Pipeline_Run``
~~~~~~~~~~~~~~~~~~~~

    ``(Optional) a more careful description of what the program does``
    

**Running the Program on EDEN/LODEEN**

To run the ``SHE_Pipeline_Run`` program with Elements use the following command:

.. code:: bash

    E-Run SHE_IAL_Pipelines 8.2 SHE_Pipeline_Run --pipeline <pipeline> --workdir <workdir> [--cluster] [--server_url <serverurl>] [--server_config <server_config>] [--isf <isf>] [--isf_args <isf_args>] [--config <config>] [--config_args <config_args>] [--plan_args <plan_args>] [--log-file <filename>] [--log-level <value>]

with the following options:


**Common Elements Arguments**
>\ ``This boilerplate section describes the standard arguments which are common to all Elements executables.``

+------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+
| **Argument**                 | **Description**                                                                                                                                                                                                                                                                                                                                                                                            | **Required**   | **Default**   |
+==============================+============================================================================================================================================================================================================================================================================================================================================================================================================+================+===============+
| --workdir ``<path>``         | Name of the working directory, where input data is stored and output data will be created.                                                                                                                                                                                                                                                                                                                 | yes            | N/A           |
+------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+
| --log-file ``<filename>``    | Name of a filename to store logging data in, relative to the workdir. If not provided, logging data will only be output to the terminal. Note that this will only contain logs directly from the run of this executable. Logs of executables called during the pipeline execution will be stored in the "logs" directory of the workdir.   | no             | None          |
+------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+
| --log-level ``<filename>``   | Minimum severity level at which to print logging information. Valid values are DEBUG, INFO, WARNING, and ERROR. Note that this will only contain logs directly from the run of this executable. The log level of executables called during pipeline execut will be set based on the configuration of the pipeline server (normally INFO).                                                                                                                                                                      | no             | INFO          |
+------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+


**Input Arguments**

The following arguments expect a filename to be provided. This filename should be either:

#. Relative to the workdir or current directory
#. Fully-qualified
#. Prefixed with one of the following special prefixes to indicate where it can be found:
   * "AUX/" - Search for the file within the path defined by the environmental variable "ELEMENTS_AUX_PATH", which is the combination of the "auxdir" folders of all projects used within the pipeline.
   * "CONF/" - Search for the file within the path defined by the environmental variable "ELEMENTS_CONF_PATH", which is the combination of the "conf" folders of all projects used within the pipeline.
   * "WEB/" - Search for the file on the SDC-UK WebDAV file system (see instructions to mount here: SDC-UK_webdav), relative to the PF-SHE directory on it. This file will be downloaded and the path to the locally downloaded version will be used. Note that pipeline runs on any cluster will not have internet access, so this can only be used for local runs.

+-------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+----------------------------------------------------+
| **Argument**                        | **Description**                                                                                                                                                    | **Required**   | **Default**                                        |
+=====================================+====================================================================================================================================================================+================+====================================================+
| ``--isf <filename>``          | ``.txt`` file listing filenames to be provided to input ports of the pipeline. This file should have one port per line, with format ``<port_name>=<filename>``, e.g. ``my_input_port=MyInputFilename.xml``. If the ``--isf_args`` argument is used, any values for input ports passed to that will override values in this file. | no            | None (all input ports will take default values provided in the \<pipeline\_name\>_isf.txt file in SHE\_Pipeline/auxdir/SHE\_Pipeline, unless overridden through use of the ``isf_args`` argument.)) |
+-------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+----------------------------------------------------+
| ``--config <filename>``   | ``.txt`` file containing configuration options to be used for one or more task within the pipeline, ``.xml`` data product or pointing to such a text file, or .json listfile (Cardinality 0-1) either pointing to such a data product or empty. The text file should contain one option per line, in the format ``<option>=<value>``, e.g. ``SHE_Pipeline_profile=True``. If the ``--config_args`` argument is used, any values for options passed to that will override values in this file.   | no             | None (equivalent to providing an empty listfile, which results in default values being used for all options)   |
+-------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+----------------------------------------------------+
| ``--server_config <filename>``   | ``.conf`` file containing the configuration of a pipeline server to use for this run. This should not be supplied in conjunction with ``--server_url``, as that will submit a run to a running pipeline server, which will already have its own configuration set up. | no | Not used, unless ``--use_debug_server_config`` is supplied, in which case  SHE\_Pipeline/auxdir/SHE\_Pipeline/debug\_server\_config.txt will be used. |
+-------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+----------------------------------------------------+


**Output Arguments**

N/A - The names of output files from the pipeline run are determined from the names of the output ports in the Pipeline Script.


**Options**

+--------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+
| **Options**                    | **Description**                                                                                                                                     | **Required**   | **Default**   |
+================================+=====================================================================================================================================================+================+===============+
| ``--cluster`` (``store true``)    | If set, will enable a workaround for a bug present on some clusters, which otherwise would result in the pipeline server's user running the pipeline not having necessary write access to files in the workdir. | no             | False         |
+--------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+
| ``--server_url <server_url>``    | The URL of the pipeline server to submit this run to. Not used if the argument ``--use_debug_server_config`` is provided, which triggers a local run. | no | ``http://ial:50000`` |
+--------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+
| ``--isf_args <port_1> <file_1> [<port_2> <file_2> ...]``    | A list of paired items, where the first item of each pair is the name of the input port, and the second is the filename for it, e.g. ``--isf_args ksb_training_data my_ksb_training_data.xml lensmc_training_data my_lensmc_training_data.xml``. Using this argument will result in a new ISF file being created and used with these values overriding those in the file provided with the ``--isf`` argument and/or the default ISF for this pipeline. | no | None (if the ``--isf`` file is provided, will use input ports from that. Any input ports unspecified by that will use default filenam values provided in the \<pipeline\_name\>_isf.txt file in SHE\_Pipeline/auxdir/SHE\_Pipeline) |
+--------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+
| ``--config_args <option_1> <value_1> [<option_2> <value_2> ...]``    | A list of paired items, where the first item of each pair is the name of the configuration option, and the second is the value for it, e.g. ``--config_args SHE_CTE_ObjectIdSplit_batch_size 10 SHE_CTE_ObjectIdSplit_max_batches 2``. Using this argument will result in a new ISF file being created and used with these values overriding those in the file provided with the ``--config`` argument. | no | None (if the ``--config`` file is provided, will use options from that. Otherwise, configuration options will take default values defined by the executables to which they are relevant.) |
+--------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+
| ``--plan_args <option_1> <value_1> [<option_2> <value_2> ...]``    | Can only be used when the Calibration pipeline is triggered. A list of paired items, where the first item of each pair is the name of an option in the simulation plan, and the second is the value for it, e.g. `` --plan_args MSEED_MIN 1 MSEED_MAX 16 NSEED_MIN 1 NSEED_MAX 16 NUM_GALAXIES 16.``. Using this argument will result in a new simulation plan file being created and used with these values overriding those in the file provided to the ``simulation_plan`` input port. | no | None (The file provided to the ``simulation_plan`` input port will be used unmodified.) |
+--------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+

    ``Any required files should be explicity explained in the Inputs section below``


**Inputs**

>\ ``Describe in detail what inputs are necessary for running this processing element as well as where they are expected to come from``


``psf_list``
............

**Description:** The filename of a ``.json`` listfile in the workdir,
listing the filenames of 1-4 ``.xml`` data products of format
dpdPsfModelImage. Each of these data products will point to a ``.fits``
file containing a binary data table containing necessary data on the
ellipticity of a PSF for each star for each exposure. This data product
and the format of the associated data table are described in detail in
the Euclid DPDD at
https://euclid.esac.esa.int/dm/dpdd/latest/shedpd/dpcards/she\_psfmodelimage.html.

**Source:** Generated by the
``SHE_PSFToolkit_model_psfs`` <https://gitlab.euclid-sgs.uk/PF-SHE/SHE_PSFToolkit>`
executable, most expediently through running the
``SHE Analysis`` <https://gitlab.euclid-sgs.uk/PF-SHE/SHE_IAL_Pipelines>`
pipeline, which calls that program and passes the generated PSFs to an
execution of this program. As this is an intermediate product, it is not
stored in the EAS.


``pipeline_config``
...................

**Description:** One of the following:

1. The word "None" (without quotes), which signals that default values
   for all configuration parameters shall be used.
2. The filename of an empty ``.json`` listfile, which similarly
   indicates the use of all default values.
3. The filename of a ``.txt`` file in the workdir listing configuration
   parameters and values for executables in the current pipeline run.
   This shall have the one or more lines, each with the format
   "SHE\_MyProject\_config\_parameter = config\_value".
4. The filename of a ``.xml`` data product of format
   DpdSheAnalysisConfig, pointing to a text file as described above. The
   format of this data product is described in detail in the Euclid DPDD
   at
   https://euclid.esac.esa.int/dm/dpdd/latest/shedpd/dpcards/she\_analysisconfig.html.
5. The filename of a ``.json`` listfile which contains the filename of a
   ``.xml`` data product as described above.

Any of the latter three options may be used for equivalent
functionality.

The ``.txt`` pipeline configuration file may have any number of
configuration arguments which apply to other executables, in addition to
optionally any of the following which apply to this executable:

+---------------------------------------------------------+-----------------------------------------------------------------------+------------------------------------------------------------------------------------------+
| **Options**                                             | **Description**                                                       | **Default behaviour**                                                                    |
+=========================================================+=======================================================================+==========================================================================================+
| SHE\_MyProject\_GenCatPic\_use\_dog ``<True/False>``    | If set to "True", will generate an image of a dog instead of a cat.   | Will generate a cat picture (equivalent to supplying "False" to this argument).          |
+---------------------------------------------------------+-----------------------------------------------------------------------+------------------------------------------------------------------------------------------+
| SHE\_MyProject\_GenCatPic\_set\_tie ``<regular/bow>``   | Will add the selected tie (``regular`` or ``bow``) to the picture.    | No tie will be added to the picture (equivalent to supplying "None" to this argument).   |
+---------------------------------------------------------+-----------------------------------------------------------------------+------------------------------------------------------------------------------------------+

If both these arguments are supplied in the pipeline configuration file
and the equivalent command-line arguments are set, the command-line
arguments will take precedence.

**Source:** One of the following:

1. May be generated manually, creating the ``.txt`` file with your text
   editor of choice.
2. Retrieved from the EAS, querying for a desired product of type
   DpdSheAnalysisConfig.
3. If run as part of a pipeline triggered by the
   ``SHE_Pipeline_Run`` <https://gitlab.euclid-sgs.uk/PF-SHE/SHE_IAL_Pipelines>`__
   helper script, may be created automatically by providing the argument
   ``--config_args ...`` to it (see documentation of that executable for
   further information).


**Outputs**

>\ ``Describe in detail what output filenames are necessary for running this program, and what they should be expected to look like. The DPDD description of any data product should contain all information necessary to understand it. If anything is non-standard about the generated output, or you want to give some quick details, do so here.``


``cat_pic``
...........

**Description:** The desired filename of the data product for the output
cat image. The data product will be an ``.xml`` file, so this filename
should end with ``.xml``.

**Details:** The generated data product will be of type DpdSheCatImage,
which is detailed in full on the DPDD at
https://euclid.esac.esa.int/dm/dpdd/latest/shedpd/dpcards/she\_catimage.html.
This product provides the filename of a generated ``.png`` cat image in
the attribute Data.DataContainer.FileName. This filename is generated to
be fully-compliant with Euclid file naming standards. You can easily get
this filename from the product with a command such as
``grep \.png cat_pic.xml``.


**Example**

>\ ``Describe here an example that any user can run out of the box to try the code and what is the expected output, if it can be reasonably run alone.``

The following example will generate picture of a cat with a bow tie in
the ``aux/CAT/pictures/`` folder:

.. code:: bash

    E-Run SHE_MyProject 0.1 SHE_MyProjectGenCatPic --workdir=AUX/SHE_MyProject/pictures/ --pipeline_config=AUX/SHE_MyProject/example_config.xml --psf_list=AUX/SHE_MyProject/example_psf.fits --use_tie=bow

    ``Or, in the case that it is over-onerous to run an example (e.g. due to the reliance on intermediate data generated by a pipeline run which is not normally available outside of such a run), instead point to an example of running a pipeline which will call this executable.``

This program is designed to be run on intermediate data generated within
an execution of the
``SHE Analysis`` <https://gitlab.euclid-sgs.uk/PF-SHE/SHE_IAL_Pipelines>`__
pipeline. Please see the documentation of that pipeline for an example
run. After that pipeline has been run once, this program can be re-run
on the generated intermediate data. The command used for the execution
of this program will be stored near the top of the log file for its
original execution, which can be found in the folder
"she\_gen\_cat\_pic" within the workdir after execution.

``SHE_Pipeline_RunBiasParallel``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    ``TODO - fill in doc``
