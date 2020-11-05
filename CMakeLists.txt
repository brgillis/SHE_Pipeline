CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_IAL_Pipelines 8.1 USE SHE_CTE 8.1
                                           SHE_GST 8.0
                                      	   SHE_MER 8.1
                                      	   SHE_PSFToolkit 8.3
                                      	   ST_PipelineChecker 0.1.1
                                      	   )

# Indirect dependencies
# ---------------------
# SHE_MomentsML 1.9
# SHE_BFD 0.7
# SHE_LensMC 2.9

