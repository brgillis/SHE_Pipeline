CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_Pipeline 0.8.10 USE Elements 5.2.2
                                        SHE_CTE 0.8.7
                                        SHE_GST 1.8.4
                                        SHE_MER 0.4.5
                                        SHE_PPT 1.2.1
                                        SHE_PSFToolkit 2.4)
# Indirect dependencies
# ---------------------
# SHE_MomentsML 1.8.5
# SHE_BFD 0.6.1
# SHE_LensMC 2.8.8
