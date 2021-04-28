CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_IAL_Pipelines 8.1.9 USE SHE_CTE 8.1.5
                                             SHE_GST 8.0.2
                                      	     SHE_MER 8.2.5
                                      	     SHE_PSFToolkit 8.4.2
                                      	     SHE_Validation 0.1.2
                                      	     ST_PipelineChecker 0.4.0
                                      	     )

# Indirect dependencies
# ---------------------
# SHE_MomentsML 8.1.2
# SHE_LensMC 3.2.2
# SHE_PPT 8.7.2

