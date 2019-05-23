CMAKE_MINIMUM_REQUIRED(VERSION 2.8.5)

#---------------------------------------------------------------
# Load macros and functions for Elements-based projects
find_package(ElementsProject)
#---------------------------------------------------------------

# Declare project name and version
elements_project(SHE_Pipeline 0.8.2 USE Elements 5.2.2
                                        SHE_CTE 0.8.2
                                        SHE_GST 1.8.1
                                        SHE_MER 0.4.2
                                        SHE_PPT 1.0.2)
# Indirect dependencies
# ---------------------
# SHE_MomentsML 1.8.1
# SHE_BFD 0.6
# SHE_LensMC 2.8.4
