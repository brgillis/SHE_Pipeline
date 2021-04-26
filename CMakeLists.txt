CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_IAL_Pipelines 8.1.7 USE SHE_CTE 8.1.3
                                             SHE_GST 8.0.1
                                      	     SHE_MER 8.2.4
                                      	     SHE_PSFToolkit 8.4.1
                                      	     SHE_Validation 0.1.1
                                      	     ST_PipelineChecker 0.4.0
                                      	     )

# Indirect dependencies
# ---------------------
# SHE_MomentsML 8.1.1
# SHE_LensMC 3.2.1
# SHE_PPT 8.7.1

