CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_Pipeline 9.3 USE SHE_CTE 9.1
                                      SHE_GST 9.1
                                      )

# Indirect dependencies
# ---------------------
# SHE_PPT 9.2.1
