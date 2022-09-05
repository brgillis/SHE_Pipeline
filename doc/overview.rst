Overview
========

.. contents::

This repository contains pipeline control and helper code for all OU-SHE pipelines.

Software identification
-----------------------

-  Processing Element Name: PF-SHE
-  Project Name: SHE\_IAL\_Pipelines
-  Profile: develop
-  Version: 9.2 (01/09/2022)

Contributors
------------

Active Contributors
~~~~~~~~~~~~~~~~~~~

-  Bryan Gillis (b.gillis@roe.ac.uk)
-  Nick Cross (njc@roe.ac.uk)
-  Gordon Gibb (Gordon.Gibb@ed.ac.uk)
-  Richard Rollins (`@rrollins <https://gitlab.euclid-sgs.uk/rrollins>`_)

Other Contributors
~~~~~~~~~~~~~~~~~~


Purpose
-------

This repository contains pipeline control code for all OU-SHE pipelines, helper programs to aid in triggering pipeline runs, and utility scripts.

Pipeline control code is contained in the ``auxdir`` of the ``SHE_Pipeline`` module, to be compliant with the deployment standards for pipelines. The control code is split into subfolders for each pipeline (some of which contain multiple pipeline scripts to allow for variant versions). This includes Package Definitions, Pipeline Definitions, Pipeline Scripts, and python modules which contain reusable pipeline blocks.

The ``SHE_Pipeline`` module defines executable programs to aid in triggering pipeline runs, most notably ``SHE_Pipeline_Run``, which can be used to trigger any known SHE pipeline.

The ``scripts`` directory of the ``SHE_Pipeline`` module contains utility scripts to aid in pipeline development and running.

Relevant Documents
------------------

    ``TO-DO: Get links for these documents from Keith.``

-  `RSD <https://euclid.roe.ac.uk/attachments/download/54815>`__
-  `SDD <https://euclid.roe.ac.uk/attachments/download/54782/EUCL-IFA-DDD-8-002.pdf>`__
-  `VP/STS <https://euclid.roe.ac.uk/attachments/download/54785/EUCL-CEA-PL-8-001_v1.44-Euclid-SGS-SHE-Validation_Plan_STS.pdf>`__
-  `STP/STR <https://euclid.roe.ac.uk/attachments/download/54784/EUCL-IFA-TP-8-002_v1-0-0.pdf>`__
