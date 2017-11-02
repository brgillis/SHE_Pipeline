CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#===============================================================================
# Load macros and functions for Elements-based projects
#===============================================================================

find_package(ElementsProject)

#===============================================================================
# Declare project name and version
# Example with dependency:
#                         elements_project(MyProject 1.0 USE Element 3.9)
#===============================================================================

elements_project(SHE_Pipeline 0.1.2 USE Elements 4.0 SHE_CTE 0.2 SHE_GST 1.2 SHE_PPT 1.0)
