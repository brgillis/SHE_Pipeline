CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_IAL_Pipelines 9.2.1 USE SHE_CTE 9.0.2
                                           SHE_GST 9.0.3
                                           SHE_MER 9.0.3
                                           SHE_PSFToolkit 9.1.3
                                           SHE_Validation 9.0.3
                                           ST_PipelineChecker 1.2.3
                                           )

# Indirect dependencies
# ---------------------
# SHE_MomentsML 9.0.2
# SHE_LensMC 9.0.2
# SHE_PPT 9.1.3
