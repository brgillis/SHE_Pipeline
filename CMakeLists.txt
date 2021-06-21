CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_IAL_Pipelines 8.1.40 USE SHE_CTE 8.1.20
                                              SHE_GST 8.0.12
                                      	      SHE_MER 8.2.19
                                      	      SHE_PSFToolkit 8.6.4
                                      	      SHE_Validation 8.0.4
                                      	      ST_PipelineChecker 1.0.1
                                      	      )

# Indirect dependencies
# ---------------------
# SHE_MomentsML 8.1.12
# SHE_LensMC 3.2.13
# SHE_PPT 8.9.3

