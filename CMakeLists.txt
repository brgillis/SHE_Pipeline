CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_IAL_Pipelines 8.1.37 USE SHE_CTE 8.1.18
                                              SHE_GST 8.0.11
                                      	      SHE_MER 8.2.16
                                      	      SHE_PSFToolkit 8.6.2
                                      	      SHE_Validation 8.0.2
                                      	      ST_PipelineChecker 1.0.1
                                      	      )

# Indirect dependencies
# ---------------------
# SHE_MomentsML 8.1.11
# SHE_LensMC 3.2.12
# SHE_PPT 8.9.2

