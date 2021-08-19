# Declare this directory as an Elements module
elements_subdir(SHE_Pipeline)

# Declare dependencies
elements_depends_on_subdirs(ST_PipelineUnitTesting)

# Instruction for Python module installation
elements_install_python_modules()

# Install python programs
elements_add_python_program(SHE_Pipeline_Run SHE_Pipeline.RunPipeline)
elements_add_python_program(SHE_Pipeline_RunBiasParallel SHE_Pipeline.RunBiasPipelineParallel)

# Install the configuration files
elements_install_conf_files()

# Install the auxiliary files
elements_install_aux_files()

# Install the scripts
elements_install_scripts()
