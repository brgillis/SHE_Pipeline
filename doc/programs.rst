Programs
========

Main Programs Available
-----------------------

    ``Describe here each executable Elements program provided by this project.``

-  `SHE_MyProject_GenCatPic <SHE_MyProject_GenCatPic_>`_ : downloads
   the picture of a cat
-  `SHE_MyProject_ShowCatPic <SHE_MyProject_ShowCatPic_>`_ : shows
   the user the picture of a cat

Running the software
--------------------

    ``for each of the codes described previously, the aim here is to describe each option, input, and output of the program as well as how to run it using Elements.``

``SHE_MyProject_GenCatPic``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

    ``(Optional) a more careful description of what the program does``

    ``Describe how one can call the program with Elements, include any necessary options with optional ones in [square brackets]. These arguments are to later be described in detail in the applicable arguments section below``

**Running the Program on EDEN/LODEEN**

To run the SHE\_MyProject\_GenCatPic processing element on Elements use
the following command:

.. code:: bash

    E-Run SHE_MyProject 0.1 SHE_MyProject_GenCatPic --workdir <dir> --psf_list <filename> --aux_data <filename> [--log-file <filename>] [--log-level <value>] [--pipeline_config <filename>] [--aux_data <filename>] [--cat_pic <filename>] [--use_dog] [--set_tie <value>]

with the following options:

**Common Elements Arguments**
>\ ``This boilerplate section describes the standard arguments which are common to all Elements executables.``

+------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+
| **Argument**                 | **Description**                                                                                                                                                                                                                                                                                                                                                                                            | **Required**   | **Default**   |
+==============================+============================================================================================================================================================================================================================================================================================================================================================================================================+================+===============+
| --workdir ``<path>``         | Name of the working directory, where input data is stored and output data will be created.                                                                                                                                                                                                                                                                                                                 | yes            | N/A           |
+------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+
| --log-file ``<filename>``    | Name of a filename to store logging data in, relative to the workdir. If not provided, logging data will only be output to the terminal. When run via the pipeline runner, this will be set to a file in the directory ``<workdir>/logs/<task_name>/`` with a name based off of the command used to call this executable, such as "E\_Run\_SHE\_MyProject\_0.1\_SHE\_MyProject\_GenCatPic\_retry\_0.out"   | no             | None          |
+------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+
| --log-level ``<filename>``   | Minimum severity level at which to print logging information. Valid values are DEBUG, INFO, WARNING, and ERROR. When run via the pipeline runner, this will be set based on the configuration of the pipeline server (normally INFO).                                                                                                                                                                      | no             | INFO          |
+------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+

**Input Arguments**
>\ ``Describe each of the input arguments which can be used when running the code, specifying the filenames of input data relative to the workdir.``

+-------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+----------------------------------------------------+
| **Argument**                        | **Description**                                                                                                                                                    | **Required**   | **Default**                                        |
+=====================================+====================================================================================================================================================================+================+====================================================+
| --psf\_list ``<filename>``          | ``.json`` listfile (Cardinality 1-4) listing data products for PSF ``.fits`` files.                                                                                | yes            | N/A                                                |
+-------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+----------------------------------------------------+
| --pipeline\_config ``<filename>``   | ``.xml`` data product or pointing to configuration file (described below), or .json listfile (Cardinality 0-1) either pointing to such a data product, or empty.   | no             | None (equivalent to providing an empty listfile)   |
+-------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+----------------------------------------------------+
| --aux\_data ``<filename>``          | ``.xml`` data product describing the auxiliary information necessary for execution.                                                                                | yes            | N/A                                                |
+-------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+----------------------------------------------------+

**Output Arguments**
>\ ``Describe each of the output arguments which can be used when running the code, specifying the desired filenames of output data relative to the workdir, which will be created by the program upon successful execution.``

+-----------------------------+---------------------------------------------------------------------------------------------------------+----------------+----------------+
| **Argument**                | **Description**                                                                                         | **Required**   | **Default**    |
+=============================+=========================================================================================================+================+================+
| --cat\_pic ``<filename>``   | Desired filename for ``.xml`` data product pointing to a ``.png`` image of the generated cat picture.   | no             | cat\_pic.xml   |
+-----------------------------+---------------------------------------------------------------------------------------------------------+----------------+----------------+

**Options**
>\ ``Describe any arguments which can be provided to the executable when run directly (not through the pipeline runner, which disallows such arguments).``

+--------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+
| **Options**                    | **Description**                                                                                                                                     | **Required**   | **Default**   |
+================================+=====================================================================================================================================================+================+===============+
| --use\_dog (``store true``)    | If set, will generate an image of a dog instead of a cat.                                                                                           | no             | false         |
+--------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------+
| --set\_tie ``<regular/bow>``   | If given, user should specify which tie to use: ``regular`` or ``bow``. If not provided, neither a tie nor a bowtie will be added to the picture.   | no             | None          |
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

``SHE_MyProject_ShowCatPic``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    ``Same structure as before: how to run the code on Elements, what are the options for the command line with descriptions and what each external file and a simple example for the user to run``
