CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_IAL_Pipelines 8.2 USE SHE_CTE 8.2
                                           SHE_GST 8.1
                                      	   SHE_MER 8.3
                                      	   SHE_PSFToolkit 8.5
                                      	   SHE_Validation 0.2
                                      	   ST_PipelineChecker 0.5
                                      	   )

# Indirect dependencies
# ---------------------
# SHE_MomentsML 8.1
# SHE_LensMC 3.2
# SHE_PPT 8.7

