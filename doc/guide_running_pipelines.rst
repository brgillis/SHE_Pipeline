.. _running_pipelines:

Running Pipelines
=================

This is a truncated guide to running pipelines, giving a general overview of how to run pipelines and why different approaches should be chosen. Full documentation of Euclid pipelines can be found on the `Euclid Redmine <https://euclid.roe.ac.uk/projects/codeen-users/wiki/DevCorner>`__, and documentation for the ``SHE_Pipeline_Run`` helper program can be found `here <programs.html#she_pipeline_run>`__.

At the most basic level, all Euclid pipelines are run by the IAL (Interface Abstraction Layer) pipeline runner. A run can be triggered in multiple ways:

#. Calling the IAL pipeline runner's `pipeline_runner.py script <https://euclid.roe.ac.uk/projects/sgsial/wiki/Running_PipelineRunner_from_CVMFS_2_2#Starting-the-Pipeline-Runner-in-Console-Mode>`__ directly to trigger a run (either locally or on a remote pipeline server)
#. Using the `SHE_Pipeline_Run <programs.html#she_pipeline_run>`__ helper program to trigger a run (either locally or on a remote pipeline server)
#. Ingesting a Pipeline Processing Order (PPO) into the Euclid Archive Service (EAS) and setting its `ProcessingStatus` attribute to ``ALLOCATED``, which will result in the pipeline being run on a desired server.
#. Triggering the pipeline via the ``COORS`` gui, which will create a PPO, then ingest it as above.

The most important consideration in which method to use to trigger a pipeline is if it's to be run locally or on a remote pipeline server. If it's to be run locally, then this requires the use of one of the former two methods to trigger it (using either the ``pipeline_runner.py`` script or the ``SHE_Pipeline_Run`` program). For deciding between these two methods, the ``SHE_Pipeline_Run`` helper program is recommended, as it greatly reduces the amount of setup needed for each pipeline run. However, this relies on some information about pipelines which is stored in this project, and so it can only be used for pipelines it recognizes. For other pipelines, the ``pipeline_runner.py`` script should be used.

When running a pipeline on a remote server, it is also possible to run it via PPO or COORS. Before going into these, let us first outline the different considerations in running a pipeline via a script or PPO/COORS. Running via a script (either ``pipeline_runner.py`` or ``SHE_Pipeline_Run``) requires that all input data is already downloaded and prepared in a desired work directory ("workdir"). When running via PPO/COORS, this is handled for you, with the data being automatically downloaded from the EAS. Instead, it is necessary to either write either a PPO (if submitting a PPO manually) or write a Pipeline Definition and fill in the query for input data (if run via COORS); this will result in a PPO which specifies the unique set of data to be downloaded and passed as input to the pipeline.

Running via COORS is the final goal of pipeline development, as it is the least-onerous way to trigger multiple runs of a pipeline on varying sets of input data. This works best when the code is stable, as it can be onerous to deploy the necessary changes to update the version of the pipeline being run. When the code is under active development, it is much easier to prepare a fixed set of input data to test the pipeline on, and trigger runs of it via script. In short, the logic is:

* Varying input data, stable code: Trigger via COORS
* Varying code, stable input data: Trigger via script (``SHE_Pipeline_Run`` if possible, ``pipeline_runner.py`` otherwise)

This leaves the final option: ingesting a PPO directly. This is normally more burdensome than triggering a run via COORS, but there are a few situations where it can be useful. The first is if you aren't able to use COORS. It might be down, or you might not have the rights to trigger pipelines with it. The second is if you want to run a copy of a pipeline run previously triggered by COORS (possibly with slight modifications, such as running it on a different pipeline server). This can be useful for testing purposes, and possibly more convenient the re-running via COORS (especially if you write a script to aid with this). These are narrow use-cases however, and so this method of triggering pipeline runs is not used as frequently as the others.
