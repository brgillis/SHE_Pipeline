CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_IAL_Pipelines 9.1 USE SHE_CTE 9.1
                                           SHE_GST 9.1
                                      	   SHE_MER 9.1
                                      	   SHE_PSFToolkit 9.2
                                      	   SHE_Validation 9.1
                                      	   ST_PipelineChecker 1.2.3
                                      	   )

# Indirect dependencies
# ---------------------
# SHE_MomentsML 9.1
# SHE_LensMC 9.1
# SHE_PPT 9.2
