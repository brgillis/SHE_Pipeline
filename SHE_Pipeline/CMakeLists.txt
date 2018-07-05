# Declare this directory as an Elements module
elements_subdir(SHE_Pipeline)

# Instruction for Python module installation
elements_install_python_modules()

# Install python programs
elements_add_python_program(SHE_Pipeline_Run SHE_Pipeline.RunPipeline)
elements_add_python_program(SHE_Pipeline_Configure SHE_Pipeline.ConfigurePipeline)

# Install the configuration files
# elements_install_conf_files()

# Install the auxiliary files
elements_install_aux_files()

# Install the scripts
elements_install_scripts()
