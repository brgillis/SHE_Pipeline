CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_Pipeline 0.4 USE Elements 5.2.2 SHE_GST 1.6 SHE_CTE 0.6 SHE_PPT 0.8)
