CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_IAL_Pipelines 8.1.42 USE SHE_CTE 8.1.22
                                              SHE_GST 8.0.13
                                      	      SHE_MER 8.2.20
                                      	      SHE_PSFToolkit 8.6.5
                                      	      SHE_Validation 8.0.5
                                      	      ST_PipelineChecker 1.0.1
                                      	      )

# Indirect dependencies
# ---------------------
# SHE_MomentsML 8.1.14
# SHE_LensMC 3.2.14
# SHE_PPT 8.9.4

