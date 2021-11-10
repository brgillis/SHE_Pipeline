Dependencies
============

Internal Euclid Dependencies
----------------------------

    ``Describe here any dependencies on Euclid projects managed by PF-SHE. Most direct dependencies should be at the top, with progressively more indirect dependencies toward the bottom, or alphabetically when otherwise equal.``
    ``Where possible, please add links to repositories or relevant gitlab codes``

-  `SHE\_CTE 8.3 <https://gitlab.euclid-sgs.uk/PF-SHE/SHE_CTE>`__
-  `SHE\_LensMC 3.3 <https://gitlab.euclid-sgs.uk/PF-SHE/SHE_LensMC>`__
-  `SHE\_MomentsML
   8.2 <https://gitlab.euclid-sgs.uk/PF-SHE/SHE_MomentsML>`__
-  `SHE\_PPT 8.11 <https://gitlab.euclid-sgs.uk/PF-SHE/SHE_PPT>`__
-  etc

External Euclid Dependencies
----------------------------

    ``Describe here any dependencies on Euclid projects managed outside PF-SHE. Most direct dependencies should be at the top, with progressively more indirect dependencies toward the bottom, or alphabetically when otherwise equal.``
    ``Where possible, please add links to repositories or relevant gitlab codes``

-  `EL\_Utils
   1.1.0 <https://gitlab.euclid-sgs.uk/EuclidLibs/EL_Utils>`__
-  `ST\_DataModelTools
   8.0.5 <https://gitlab.euclid-sgs.uk/ST-DM/ST_DataModelTools>`__
-  `ST\_DataModelBindings
   8.0.5 <https://gitlab.euclid-sgs.uk/ST-DM/ST_DataModelBindings>`__
-  `ST\_DataModel
   8.0.5 <https://gitlab.euclid-sgs.uk/ST-DM/ST_DataModel>`__
-  `Elements 5.15 <https://gitlab.euclid-sgs.uk/ST-TOOLS/Elements>`__
-  etc

Configuration
-------------

    ``Describes the version of EDEN which this code runs on, and lists the versions of relevant packages in EDEN which are used by this project. In the case where one package depends on another (e.g. astropy depends on numpy), the dependant package should be listed first (astropy before numpy), or alphabetically when otherwise equal.``

**EDEN 2.1**

::

    - astropy 3.2.1
    - numpy 1.17.2
    - etc

Dependant Projects
------------------

    ``Add here a list of all projects which depend on this project either directly or indirectly. These are the projects which will be at-risk of disruption due to changes in this project. Most direct dependants should be listed first (e.g. they call a function provided by this project), followed by more indirect dependants (e.g. they call a function provided by project B, which calls a function provided by this project), with dependants listed alphabetically when otherwise equal.``
    ``Where possible, please add links to repositories or relevant gitlab codes``

-  `SHE\_LensMC <https://gitlab.euclid-sgs.uk/PF-SHE/SHE_LensMC>`__
-  `SHE\_MomentsML <https://gitlab.euclid-sgs.uk/PF-SHE/SHE_MomentsML>`__
-  `SHE\_CTE <https://gitlab.euclid-sgs.uk/PF-SHE/SHE_CTE>`__
-  `SHE\_IAL\_Pipelines <https://gitlab.euclid-sgs.uk/PF-SHE/SHE_IAL_Pipelines>`__
-  etc

Dependant Pipelines
-------------------

    ``Add here a list of all pipelines which use code from this project either directly or indirectly. These are the pipelines which will be at-risk of disruption due to changes in this project. Pipelines should be listed alphabetically.``
    ``Where possible, please add links to repositories or relevant gitlab codes``

-  `SHE
   Analysis <https://gitlab.euclid-sgs.uk/PF-SHE/SHE_IAL_Pipelines/-/blob/develop/SHE_Pipeline/auxdir/SHE_Shear_Analysis/PipScript_SHE_Shear_Analysis.py>`__
-  `Shear
   Calibration <https://gitlab.euclid-sgs.uk/PF-SHE/SHE_IAL_Pipelines/-/blob/develop/SHE_Pipeline/auxdir/SHE_Shear_Calibration/PipScript_SHE_Shear_Calibration.py>`__
-  `SHE Global
   Validation <https://gitlab.euclid-sgs.uk/PF-SHE/SHE_IAL_Pipelines/-/blob/develop/SHE_Pipeline/auxdir/SHE_Global_Validation/PipDef_SHE_Global_Validation.xml>`__
-  etc.