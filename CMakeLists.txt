CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_IAL_Pipelines 8.1.24 USE SHE_CTE 8.1.10
                                              SHE_GST 8.0.7
                                      	      SHE_MER 8.2.12
                                      	      SHE_PSFToolkit 8.5.8
                                      	      SHE_Validation 0.1.7
                                      	      ST_PipelineChecker 1.0.1
                                      	      )

# Indirect dependencies
# ---------------------
# SHE_MomentsML 8.1.7
# SHE_LensMC 3.2.7
# SHE_PPT 8.8.4

