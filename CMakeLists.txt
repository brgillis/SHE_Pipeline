CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_Pipeline 0.7 USE Elements 5.2.2 SHE_GST 1.7 SHE_CTE 0.7 SHE_PPT 1.1 SHE_MER 0.3 SHE_PSFToolkit 2.3)
