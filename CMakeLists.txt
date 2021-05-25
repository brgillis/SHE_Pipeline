CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_IAL_Pipelines 8.1.22 USE SHE_CTE 8.1.9
                                              SHE_GST 8.0.6
                                      	      SHE_MER 8.2.11
                                      	      SHE_PSFToolkit 8.5.6
                                      	      SHE_Validation 0.1.6
                                      	      ST_PipelineChecker 1.0.1
                                      	      )

# Indirect dependencies
# ---------------------
# SHE_MomentsML 8.1.6
# SHE_LensMC 3.2.6
# SHE_PPT 8.8.3

