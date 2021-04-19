CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_IAL_Pipelines 8.1.3 USE SHE_CTE 8.1.0
                                           SHE_GST 8.0.0
                                      	   SHE_MER 8.2.2
                                      	   SHE_PSFToolkit 8.4.0
                                      	   SHE_Validation 0.1.0
                                      	   ST_PipelineChecker 0.4.0
                                      	   )

# Indirect dependencies
# ---------------------
# SHE_MomentsML 8.1.0
# SHE_LensMC 3.2.0
# SHE_PPT 8.7.0

