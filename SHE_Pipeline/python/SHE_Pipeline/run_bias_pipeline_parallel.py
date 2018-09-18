""" @file run_bias_pipeline_parallel.py

    Created Aug 2018

    Main executable for running bias pipeline in parallel
"""

__updated__ = "2018-09-18"

# Copyright (C) 2012-2020 Euclid Science Ground Segment
#
# This library is free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation; either version 3.0 of the License, or (at your option)
# any later version.
#
# This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along with this library; if not, write to
# the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

from collections import namedtuple
import math
import multiprocessing
import os
import time

from astropy.io import fits
from astropy.table import Table
import numpy

import SHE_GST_GalaxyImageGeneration.GenGalaxyImages as ggi
from SHE_GST_GalaxyImageGeneration.generate_images import generate_images
from SHE_GST_GalaxyImageGeneration.run_from_config import run_from_args
import SHE_GST_PrepareConfigs.write_configs as gst_prep_conf
import SHE_GST_cIceBRGpy
from SHE_PPT import products
from SHE_PPT.file_io import (find_file, find_aux_file, get_allowed_filename,
                             read_xml_product, read_listfile, write_listfile,
                             read_pickled_product)
from SHE_PPT.logging import getLogger
from SHE_Pipeline.pipeline_utilities import get_relpath
import SHE_Pipeline.pipeline_utilities as pu
import SHE_Pipeline.run_pipeline as rp
import subprocess as sbp


default_workdir = "/home/user/Work/workspace"
default_logdir = "logs"
default_cluster_workdir = "/workspace/lodeen/workdir"

non_filename_args = ("workdir", "logdir", "pkgRepository", "pipelineDir")

ERun_CTE = "E-Run SHE_CTE 0.5 "
ERun_GST = "E-Run SHE_GST 1.5 "
ERun_MER = "E-Run SHE_MER 0.1 "
ERun_Pipeline = "E-Run SHE_Pipeline 0.3 "

def she_prepare_configs(simulation_plan,config_template,
        simulation_configs,workdir):
    """ Runs SHE_GST Prepare configurations
    Sets up simulations using simulation plan and configuration
    template. 
    Creates *cache.bin files
    """
    
    logger=getLogger(__name__)
    
    gst_prep_conf.write_configs_from_plan(
        plan_filename=get_relpath(simulation_plan,workdir),
            template_filename=get_relpath(config_template,workdir),
            listfile_filename=get_relpath(simulation_configs,workdir),
            workdir=workdir)
    logger.info("Prepared configurations")
    return

def she_simulate_images(config_files,pipeline_config,data_images,
    stacked_data_image, psf_images_and_tables,segmentation_images,
    stacked_segmentation_image,detections_tables,details_table,
    workdir,logdir,simNo):
    """ Runs SHE_GST_GenGalaxyImages code, creating images, segmentations
    catalogues etc.
    """
    logger=getLogger(__name__)
    cmd=(ERun_GST + "SHE_GST_GenGalaxyImages --config_files %s "
        "--pipeline_config %s --data_images %s --stacked_data_image %s "
        "--psf_images_and_tables %s --segmentation_images %s "
        "--stacked_segmentation_image %s --detections_tables %s "
        "--details_table %s --workdir %s "
        "--log-file %s/%s/she_simulate_images.out 2> /dev/null" 
        % (get_relpath(config_files,workdir),
           get_relpath(pipeline_config,workdir),
           get_relpath(data_images,workdir),
           get_relpath(stacked_data_image,workdir),
           get_relpath(psf_images_and_tables,workdir),
           get_relpath(segmentation_images,workdir),
           get_relpath(stacked_segmentation_image,workdir),
           get_relpath(detections_tables,workdir),
           get_relpath(details_table,workdir),
           workdir, workdir, logdir))
    
    
    # warnings out put as stdOut/stdErr --> send to log file..
    # Why is it not E-Run.err??
    logger.info("Executing command: " + cmd)
    try:
        sbp.check_call(cmd,shell=True)
    except Exception as e:
        logger.error("Execution failed with error: " + str(e))
        raise
    logger.info("Finished command execution successfully")
    return
 
def she_estimate_shear(data_images,stacked_image,
    psf_images_and_tables, segmentation_images,
    stacked_segmentation_image, detections_tables,
    bfd_training_data, ksb_training_data,
    lensmc_training_data,momentsml_training_data,
    regauss_training_data, pipeline_config,
    shear_estimates_product,workdir,logdir,sim_no):
    """ Runs the SHE_CTE_EstimateShear method that calculates 
    the shear using 5 methods: BFD, KSB, LensMC, MomentsML and REGAUSS
    
    @todo: use defined options for which Methods to use...
    # It is in the pipeline config file...
    # Do checks for consistency (earlier)
    """
    
    logger=getLogger(__name__)
    
    # Check to see if training data exists.
    # @TODO: Simplify, avoid repetitions
    shear_method_arg_string=""
    if bfd_training_data and bfd_training_data!='None':
        shear_method_arg_string+=" --bfd_training_data %s" % get_relpath(
            bfd_training_data,workdir)
    if ksb_training_data and ksb_training_data!='None':
        shear_method_arg_string+=" --ksb_training_data %s" % get_relpath(
            ksb_training_data,workdir)
    if lensmc_training_data and lensmc_training_data!='None':
        shear_method_arg_string+=" --lensmc_training_data %s" % get_relpath(
            lensmc_training_data,workdir)
    if momentsml_training_data and momentsml_training_data!='None':
        shear_method_arg_string+=" --momentsml_training_data %s" % get_relpath(
            momentsml_training_data,workdir)
    if regauss_training_data and regauss_training_data!='None':
        shear_method_arg_string+=" --regauss_training_data %s" % get_relpath(
            regauss_training_data,workdir)
        
        
    cmd=(ERun_CTE + "SHE_CTE_EstimateShear --data_images %s "
        "--stacked_image %s --psf_images_and_tables %s "
        "--segmentation_images %s --stacked_segmentation_image %s "
        "--detections_tables %s%s --pipeline_config %s "
        "--shear_estimates_product %s --workdir %s "
        "--log-file %s/%s/she_estimate_shear.out 2> /dev/null"  %
        (get_relpath(data_images,workdir),
         get_relpath(stacked_image,workdir),
         get_relpath(psf_images_and_tables,workdir),
         get_relpath(segmentation_images,workdir), 
         get_relpath(stacked_segmentation_image,workdir), 
         get_relpath(detections_tables,workdir),
         shear_method_arg_string,
         get_relpath(pipeline_config,workdir),
         get_relpath(shear_estimates_product,workdir),
         workdir,workdir,logdir))
    
    logger.info("Executing command: " + cmd)
    try:
        sbp.check_call(cmd,shell=True)
    except Exception as e:
        logger.error("Execution failed with error: " + str(e))
        raise
    logger.info("Finished command execution successfully")
    return

def she_measure_statistics(details_table, shear_estimates,
    pipeline_config,shear_bias_statistics,workdir,logdir, sim_no):
    """ Runs the SHE_CTE_MeasureStatistics method on shear 
    estimates to get shear bias statistics.
    """
    logger=getLogger(__name__)
    
    cmd=(ERun_CTE + "SHE_CTE_MeasureStatistics --details_table %s "
        "--shear_estimates %s --pipeline_config %s --shear_bias_statistics %s "
        "--workdir %s "
        "--log-file %s/%s/she_measure_statistics.out 2> /dev/null" 
        % (get_relpath(details_table,workdir), 
           get_relpath(shear_estimates,workdir), 
           get_relpath(pipeline_config,workdir),
           get_relpath(shear_bias_statistics,workdir),
           workdir,workdir,logdir))
    
    logger.info("Executing command: " + cmd)
    try:
        sbp.check_call(cmd,shell=True)
    except Exception as e:
        logger.error("Execution failed with error: " + str(e))
        raise
    logger.info("Finished command execution successfully")
    
    return

def she_cleanup_bias_measurement(simulation_config,data_images, 
    stacked_data_image, psf_images_and_tables, segmentation_images,
    stacked_segmentation_image, detections_tables, details_table,
    shear_estimates, shear_bias_statistics_in, pipeline_config,
    shear_bias_measurements,workdir,logdir,sim_no):
    """ Runs the SHE_CTE_CleanupBiasMeasurement code on shear_bias_statistics.
    Returns shear_bias_measurements
    """
    logger=getLogger(__name__)
    
    cmd=(ERun_CTE + "SHE_CTE_CleanupBiasMeasurement --simulation_config %s "
        "--data_images %s --stacked_data_image %s --psf_images_and_tables %s "
        "--segmentation_images %s --stacked_segmentation_image %s "
        "--detections_tables %s --details_table %s --shear_estimates %s "
        "--shear_bias_statistics_in %s --pipeline_config %s "
        "--shear_bias_statistics_out %s --workdir %s "
        "--log-file %s/%s/she_cleanup_bias_measurement.out 2> /dev/null"  % (
        get_relpath(simulation_config,workdir),
        get_relpath(data_images,workdir), 
        get_relpath(stacked_data_image,workdir), 
        get_relpath(psf_images_and_tables,workdir), 
        get_relpath(segmentation_images,workdir),
        get_relpath(stacked_segmentation_image,workdir), 
        get_relpath(detections_tables,workdir), 
        get_relpath(details_table,workdir),
        get_relpath(shear_estimates,workdir), 
        get_relpath(shear_bias_statistics_in,workdir), 
        get_relpath(pipeline_config,workdir),
        get_relpath(shear_bias_measurements,workdir),workdir,workdir,logdir))
    
    logger.info("Executing command: " + cmd)
    try:
        sbp.check_call(cmd,shell=True)
    except Exception as e:
        logger.error("Execution failed with error: " + str(e))
        raise
    logger.info("Finished command execution successfully")
    return


def she_measure_bias(shear_bias_measurement_list,pipeline_config,
    shear_bias_measurement_final,workdir,logdir):
    """ Runs the SHE_CTE_MeasureBias on a list of shear_bias_measurements from
    all simulation runs.
    
    """
    logger=getLogger(__name__)
    cmd=(ERun_CTE + "SHE_CTE_MeasureBias --shear_bias_statistics %s "
        "--pipeline_config %s --shear_bias_measurements %s --workdir %s "
        "--log-file %s/%s/she_measure_bias.out 2> /dev/null" 
        % (get_relpath(shear_bias_measurement_list,workdir),
           get_relpath(pipeline_config,workdir),
           get_relpath(shear_bias_measurement_final,workdir),
           workdir,workdir,logdir))
    
    logger.info("Executing command: " + cmd)
    try:
        sbp.check_call(cmd,shell=True)
    except Exception as e:
        logger.error("Execution failed with error: " + str(e))
        raise
    logger.info("Finished command execution successfully")
    return

def she_print_bias(workdir,shear_bias_measurement_final,logdir):
    """ Runs the SHE_CTE_PrintBias on the final shear bias measurements
    file
    """
    logger = getLogger(__name__)
        
    cmd=(ERun_CTE+" SHE_CTE_PrintBias --workdir %s "
         "--shear_bias_measurements %s "
         "--log-file %s/%s/she_print_bias.out"  % (workdir,
                get_relpath(shear_bias_measurement_final,workdir),
                workdir,logdir)) 
    logger.info("Executing command: " + cmd)
    try:
        sbp.check_call(cmd,shell=True)
    except Exception as e:
        logger.error("Execution failed with error: " + str(e))
        raise
    logger.info("Finished command execution successfully")
    return


def check_args(args):
    """Checks arguments for validity and fixes if possible.
    Modified from similar function in run_pipeline
    
    One big modification is that it checks that number of threads
    has been set and if not uses multiprocessing to do so
    and then sets up the multiple directory structure for each
    thread.
    
    @return dirStruct
    @rType  list(namedtuple)
    """

    logger = getLogger(__name__)

    logger.debug('# Entering SHE_Pipeline_RunBiasParallel check_args()')

    pipeline='bias_measurement'
    # Does the pipeline we want to run exist?
    pipeline_filename = os.path.join(rp.get_pipeline_dir(), "SHE_Pipeline_pkgdef/"+ pipeline + ".py")
    if not os.path.exists(pipeline_filename):
        logger.error("Pipeline '" + pipeline_filename + "' cannot be found. Expected location: " +
                     pipeline_filename)

    # If no ISF is specified, check for one in the AUX directory
    if args.isf is None:
        try:
            args.isf = find_aux_file("SHE_Pipeline/" + pipeline + "_isf.txt")
        except Exception:
            logger.error("No ISF file specified, and cannot find one in default location (" +
                         "AUX/SHE_Pipeline/" + pipeline + "_isf.txt).")
            raise

    # If no config is specified, check for one in the AUX directory
    if args.config is None:
        try:
            args.config = find_aux_file("SHE_Pipeline/" + pipeline + "_config.txt")
        except Exception:
            logger.error("No config file specified, and cannot find one in default location (" +
                         "AUX/SHE_Pipeline/" + pipeline + "_config.txt).")
            raise

    # Check that we have an even number of ISF arguments
    if args.isf_args is None:
        args.isf_args = []
    if not len(args.isf_args) % 2 == 0:
        raise ValueError("Invalid values passed to 'args': Must be a set of paired arguments.")

    # Check that we have an even number of pipeline_config arguments
    if args.config_args is None:
        args.config_args = []
    if not len(args.config_args) % 2 == 0:
        raise ValueError("Invalid values passed to 'config_args': Must be a set of paired arguments.")

    # Use the default workdir if necessary
    if args.workdir is None:
        if args.cluster:
            args.workdir = default_cluster_workdir
        else:
            args.workdir = default_workdir
        logger.info('No workdir supplied at command-line. Using default workdir: ' + args.workdir)


    # Use the default logdir if necessary
    if args.logdir is None:
        args.logdir = default_logdir
        logger.info('No logdir supplied at command-line. Using default logdir: ' + args.logdir)
    qualified_logdir = os.path.join(args.workdir, args.logdir)
    
    # Set up the workdir and app_workdir the same way

    #if args.workdir == args.app_workdir:
    workdirs = (args.workdir,)
    #else:
    #    workdirs = (args.workdir), args.app_workdir,)

    if args.number_threads == 0:
        # @TODO: change to multiprocessing.cpu_count()?
        args.number_threads = str(multiprocessing.cpu_count()-1)
    if not args.number_threads.isdigit():
        raise ValueError("Invalid values passed to 'number-threads': Must be an integer.")

    # @TODO: Be careful, workdir and app_workdir...
    # make sure number threads is valid 
    # @FIXME: Check this...
    nThreads= max(1,min(int(args.number_threads),multiprocessing.cpu_count()))
    
    dirStruct = pu.create_thread_dir_struct(args,workdirs,int(nThreads))
    
    if not os.path.exists(qualified_logdir):
        # Can we create it?
        try:
            os.mkdir(qualified_logdir)
        except Exception as e:
            logger.error("logdir (" + qualified_logdir + ") does not exist and cannot be created.")
            raise e
    if args.cluster:
        os.chmod(qualified_logdir, 0o777)

    # Check that pipeline specific args are only provided for the right pipeline
    if args.plan_args is None:
        args.plan_args = []
    if not len(args.plan_args) % 2 == 0:
        raise ValueError("Invalid values passed to 'plan_args': Must be a set of paired arguments.")
    
    return dirStruct


def create_batches(args,sim_config_list,workdirList):
    """ Uses the simulation configuration list and the 
    workdir (thread) list to split the simulations into
    batches.
    
    @return batchList
    @rType list(namedtuple)
    """
    # Overwrite values as necessary
    # @TODO: Do we do this multiple times and save multiple times - or add multiple lines?
    BatchTuple=namedtuple("Batch", "batch_no nThreads min_sim_no max_sim_no")
    
    sim_config_list=read_listfile(os.path.join(args.workdir,sim_config_list))
    number_simulations=len(sim_config_list)
    
    number_batches = math.ceil(number_simulations/len(workdirList))
    batchList=[]
    
                  
    
    for batch_no in range(number_batches):
        
        nThreads=len(workdirList)
        min_sim_no = len(workdirList)*batch_no
        max_sim_no = len(workdirList)*(batch_no+1)
        if max_sim_no>number_simulations:
            nThreads = number_simulations-min_sim_no
            max_sim_no = number_simulations
            
        batchList.append(BatchTuple(batch_no,nThreads,min_sim_no,max_sim_no))
    

    return batchList


def get_sim_no(thread_no,batch):
    """ Returns simulation number calculated from thread_no and batch tuple
    @return: simulation_number
    @rType:  int
    """
    
    return batch.min_sim_no+thread_no
    

def create_simulate_measure_inputs(args, config_filename,workdir,sim_config_list,
        simulation_no):
    """Function to create a new ISF for this run by adjusting workdir and logdir, and overwriting any
       values passed at the command-line.
       
       More importantly, does symlinks to current thread to link correct
       files to different threads. Based on run_pipeline function
    
    @return: Simulation inputs
    @rtype:  namedtuple
    
    """
    
    InputsTuple =namedtuple("SMInputs","simulation_config bfd_training_data "
        "ksb_training_data lensmc_training_data momentsml_training_data "
        "regauss_training_data pipeline_config")

    logger = getLogger(__name__)

    # Find the base ISF we'll be creating a modified copy of
    # @TODO: include batch_no in name
    base_isf = find_file(args.isf, path=args.workdir)
    new_isf_filename = get_allowed_filename("ISF", str(os.getpid()),
        extension=".txt", release="00.03")
    qualified_isf_filename = os.path.join(workdir.workdir, 
        new_isf_filename)

    # Set up the args we'll be replacing or setting

    args_to_set = {}
    args_to_set["workdir"] = workdir.workdir
    args_to_set["logdir"] = workdir.logdir
    args_to_set["pkgRepository"] = rp.get_pipeline_dir()
    args_to_set["pipelineDir"] = os.path.join(rp.get_pipeline_dir(), 
                                              "SHE_Pipeline_pkgdef")
    args_to_set["pipeline_config"] = config_filename

    arg_i = 0
    while arg_i < len(args.isf_args):
        
        args_to_set[args.isf_args[arg_i]] = args.isf_args[arg_i + 1]
        arg_i += 2
        
    
    with open(base_isf, 'r') as fi:
        # Check each line to see if values we'll overwrite are specified in it,
        # and only write out lines with other values
        for line in fi:
            split_line = line.strip().split('=')
            # Add any new args here to the list of args we want to set
            if not (split_line[0] in args_to_set) and len(split_line) > 1:
                args_to_set[split_line[0]] = split_line[1]
     

 
    simulation_config = read_listfile(os.path.join(
        args.workdir,sim_config_list))[simulation_no]
    args_to_set['simulation_config']=simulation_config
    
    # Search path is root workdir
    search_path = args.workdir
    
    # Inputs for thread
    simulateInputs = InputsTuple(*[
        args_to_set['simulation_config'],
        args_to_set['bfd_training_data'],
        args_to_set['ksb_training_data'],
        args_to_set['lensmc_training_data'],
        args_to_set['momentsml_training_data'],
        args_to_set['regauss_training_data'],
        args_to_set['pipeline_config']])
    
    
    for input_port_name in args_to_set:
        # Skip ISF arguments that don't correspond to input ports
        if input_port_name in non_filename_args:
            continue
        if 'simulation_plan' in input_port_name:
            continue
        
        filename = args_to_set[input_port_name]
        # Skip if None
        if filename is None or filename == "None":
            continue

        if 'TEST-%s' % simulation_no in filename:
            # File for this simulation
            pass
        elif 'TEST-' in filename:
            continue


        # Find the qualified location of the file
        try:
            qualified_filename = find_file(filename, path=search_path)
        except RuntimeError as e:
            raise RuntimeError("Input file " + filename + " cannot be found in path " + search_path)

        # Symlink the filename from the "data" directory within the workdir
        new_filename = os.path.join("data", os.path.split(filename)[1])
        try:
            if not qualified_filename == os.path.join(workdir.workdir, new_filename):
                os.symlink(qualified_filename, os.path.join(workdir.workdir, new_filename))
        except FileExistsError as e:
            try:
                os.remove(os.path.join(workdir.workdir, new_filename))
                try:
                    os.unlink(os.path.join(workdir.workdir, new_filename))
                except Exception as _:
                    pass
            except Exception as _:
                pass
            if not qualified_filename == os.path.join(workdir.workdir, new_filename):
                os.symlink(qualified_filename, os.path.join(workdir.workdir, new_filename))

        # Update the filename in the args_to_set to the new location
        args_to_set[input_port_name] = new_filename

        # Now, go through each data file of the product and symlink those from the workdir too

        # Skip (but warn) if it's not an XML data product
        if qualified_filename[-4:] != ".xml":
            logger.warn("Input file " + filename + " is not an XML data product.")
            continue

        p = read_xml_product(qualified_filename)
        if not hasattr(p, "get_all_filenames"):
            raise NotImplementedError("Product " + str(p) + " has no \"get_all_filenames\" method - it must be " +
                                      "initialized first.")
        data_filenames = p.get_all_filenames()
        if len(data_filenames) == 0:
            continue

        # Set up the search path for data files
        data_search_path = (os.path.split(qualified_filename)[0] + ":" +
                            os.path.split(qualified_filename)[0] + "/..:" +
                            os.path.split(qualified_filename)[0] + "/../data:" + search_path)

        # Search for and symlink each data file
        for data_filename in data_filenames:

            # Find the qualified location of the data file
            try:
                qualified_data_filename = find_file(data_filename, path=data_search_path)
            except RuntimeError as e:
                raise RuntimeError("Data file " + data_filename + " cannot be found in path " + data_search_path)

            # Symlink the data file within the workdir
            if os.path.exists(os.path.join(workdir.workdir, data_filename)):
                os.remove(os.path.join(workdir.workdir, data_filename))
                try:
                    os.unlink(os.path.join(workdir.workdir, data_filename))
                except Exception as _:
                    pass
            if not qualified_data_filename == os.path.join(workdir.workdir, 
                                                           data_filename):
                os.symlink(qualified_data_filename, os.path.join(workdir.workdir, 
                                                                 data_filename))

        # End loop "for data_filename in data_filenames:"

    # End loop "for input_port_name in args_to_set:"


    # Write out the new ISF
    with open(qualified_isf_filename, 'w') as fo:
        # Write out values we want set specifically
        for arg in args_to_set:
            fo.write(arg + "=" + args_to_set[arg] + "\n")

    # Symlink to *.bin files...
    binaryConfigFiles = [fName for fName in os.listdir(args.workdir) 
                         if fName.endswith('bin')]
    for binConfFile in binaryConfigFiles:
        newConfFileName=os.path.join(workdir.workdir,binConfFile)
        if not os.path.exists(newConfFileName):
            os.symlink(os.path.join(args.workdir,binConfFile),newConfFileName)
    return simulateInputs





def she_simulate_and_measure_bias_statistics(simulation_config,
        bfd_training_data, ksb_training_data,
        lensmc_training_data, momentsml_training_data,
        regauss_training_data,pipeline_config,workdirTuple,
        simulation_no,logdir):
    """ Parallel processing parts of bias_measurement pipeline
    
    """
    # several commands...
    # @FIXME: check None types.
    
    logger = getLogger(__name__)
    
    workdir=workdirTuple.workdir 
    
    
     
    data_image_list = os.path.join('data','data_images.json')
    stacked_data_image =  os.path.join('data','stacked_image.xml')
    psf_images_and_tables = os.path.join('data','psf_images_and_tables.json')
    segmentation_images = os.path.join('data','segmentation_images.json')
    stacked_segmentation_image = os.path.join('data','stacked_segm_image.xml')
    detections_tables=os.path.join('data','detections_tables.json')
    details_table=os.path.join('data','details_table.xml')
    
    she_simulate_images(simulation_config, pipeline_config, data_image_list,
        stacked_data_image,psf_images_and_tables,segmentation_images,
        stacked_segmentation_image,detections_tables,details_table,
        workdir,logdir,simulation_no) 
    
    
    shear_estimates_product = os.path.join('data','shear_estimates_product.xml')
    
    she_estimate_shear(data_images=data_image_list,
        stacked_image=stacked_data_image,
        psf_images_and_tables=psf_images_and_tables,
        segmentation_images=segmentation_images,
        stacked_segmentation_image=stacked_segmentation_image,
        detections_tables=detections_tables,
        bfd_training_data=bfd_training_data,
        ksb_training_data=ksb_training_data,
        lensmc_training_data=lensmc_training_data,
        momentsml_training_data=momentsml_training_data,
        regauss_training_data=regauss_training_data,
        pipeline_config=pipeline_config,
        shear_estimates_product=shear_estimates_product,
        workdir=workdir, logdir=logdir, sim_no=simulation_no)


    shear_bias_statistics = os.path.join('data','shear_bias_statistics.xml')
    
    she_measure_statistics(details_table=details_table,
        shear_estimates=shear_estimates_product,
        pipeline_config=pipeline_config,
        shear_bias_statistics=shear_bias_statistics,
        workdir=workdir, logdir=logdir, sim_no=simulation_no)

    shear_bias_measurements = os.path.join('data',
        'shear_bias_measurements_sim%s.xml' % simulation_no)
    
    
    #ii=0
    #maxNTries=5
    #hasRun = False
    #while not hasRun and ii<maxNTries:
    #    if os.path.exists(shear_bias_statistics):

    she_cleanup_bias_measurement(simulation_config=simulation_config,
        data_images=data_image_list, stacked_data_image=stacked_data_image,
        psf_images_and_tables=psf_images_and_tables,
        segmentation_images=segmentation_images,
        stacked_segmentation_image=stacked_segmentation_image,
        detections_tables=detections_tables,
        details_table=details_table,
        shear_estimates=shear_estimates_product,
        shear_bias_statistics_in=shear_bias_statistics,  
        pipeline_config=pipeline_config,
        shear_bias_measurements=shear_bias_measurements,
        workdir=workdir, logdir=logdir, sim_no=simulation_no)
            
    logger.info("Completed parallel pipeline stage, she_simulate_and_measure_bias_statistics")                                                     

    return 

def run_pipeline_from_args(args):
    """Main executable to run parallel pipeline.
    """

    logger = getLogger(__name__)
    
    # Check for pickled arguments, and override if found
    if args.pickled_args is not None:
        qualified_pickled_args_filename = find_file(args.pickled_args,args.workdir)
        args = read_pickled_product(qualified_pickled_args_filename)

    # Check the arguments
    workdirList=check_args(args) # add argument there..
    #if len(args.plan_args) > 0:
    sim_plan_table,sim_plan_tablename=rp.create_plan(args, retTable=True)
    
    # Create the pipeline_config for this run
    config_filename = rp.create_config(args)
    # Create the ISF for this run
    #qualified_isf_filename = rp.create_isf(args, config_filename)
    
    shear_bias_measurement_listfile = os.path.join(
        args.workdir,'data','shear_bias_measurement_list.json')
    
    # prepare configuration
    
    simulation_configs=os.path.join('data','sim_configs.json')
    
    # @FIXME: sim configuration template
    base_isf = find_file(args.isf, path=args.workdir)
    # read get 
    args_to_set={}
    with open(base_isf, 'r') as fi:
        # Check each line to see if values we'll overwrite are specified in it,
        # and only write out lines with other values
        for line in fi:
            split_line = line.strip().split('=')
            # Add any new args here to the list of args we want to set
            if not (split_line[0] in args_to_set) and len(split_line) > 1:
                args_to_set[split_line[0]] = split_line[1]
    
    if not ('config_template' in args_to_set and 
            os.path.exists(find_file(args_to_set['config_template']))):
        raise Exception("configuration template not found") 
    
    config_template=find_file(args_to_set['config_template'])
    
    logger.info("Preparing configurations")
    she_prepare_configs(sim_plan_tablename,
        config_template,simulation_configs,args.workdir)
    
    batches=create_batches(args,simulation_configs,workdirList)
    
    logger.info("Running parallel part of pipeline in %s batches and %s threads" 
                % (len(batches),len(workdirList)))
    for batch in batches:
        # Move data to threads
        #insert_data_to_threads(args,batch,workdirList,sim_table)
        # Update_isf_...
        
        
        # Create the pipeline_config for this run
        # @TODO: Do we need multiple versions of this, one for each thread?
        prodThreads=[]
        
        for threadNo in range(batch.nThreads):
            workdir = workdirList[threadNo]
            # logdir?
            simulation_no=get_sim_no(threadNo,batch)
            # Create the ISF for this run
            # @TODO:  do we need multiple versions of this, one for each thread
            
            # @FIXME: Don't really need ISF - that is for the pipeline runner..
            simulate_measure_inputs = create_simulate_measure_inputs(args, 
                config_filename,workdir,simulation_configs,simulation_no)
            
        
            #simulation_config =
            #bfd_training...
            
            # @TODO: Is it better to run each process separately?
            
            prodThreads.append(multiprocessing.Process(target=she_simulate_and_measure_bias_statistics,
                args=(simulate_measure_inputs.simulation_config,
                      simulate_measure_inputs.bfd_training_data,
                      simulate_measure_inputs.ksb_training_data,
                      simulate_measure_inputs.lensmc_training_data,
                      simulate_measure_inputs.momentsml_training_data,
                      simulate_measure_inputs.regauss_training_data,
                      simulate_measure_inputs.pipeline_config,
                      workdir,simulation_no,args.logdir)))
        
        if prodThreads:
            pu.runThreads(prodThreads)
            
        logger.info("Run batch %s in parallel, now to merge outputs from threads" % batch.batch_no)
        mergeOutputs(workdirList,batch,shear_bias_measurement_listfile)
        # Clean up 
        logger.info("Cleaning up batch files..")   
        pu.cleanup(batch,workdirList)
    

    # Run final process
    shear_bias_measurement_final=os.path.join(args.workdir,'data','shear_bias_measurements_final.xml')
    
    logger.info("Running final she_measure_bias to calculate "
        "final shear: output in %s" % shear_bias_measurement_final)
    she_measure_bias(shear_bias_measurement_listfile,config_filename,
        shear_bias_measurement_final,args.workdir,args.logdir)
    logger.info("Pipeline completed!")
    
    # @TODO: option for print_bias
    logger.info("Running SHE_CTE PrintBias to calculate bias values")
    she_print_bias(args.workdir,shear_bias_measurement_final,args.logdir)
    
    logger.info("Tests completed!")
    
    return


def mergeOutputs(workdirList,batch,
        shear_bias_measurement_listfile):
    """ Merge outputs from different threads at the end of each 
    batch. Updates .json file
    
    
    """
    
    newList=[]
    for workdir in workdirList:
        thread_no = int(workdir.workdir.split('thread')[1].split('/')[0])
        if thread_no<batch.nThreads:
            sim_no=get_sim_no(thread_no,batch)
            # @TODO: root of this in one place
            shear_bias_measfile=os.path.join(workdir.workdir,'data',
                'shear_bias_measurements_sim%s.xml' % sim_no)
            if os.path.exists(shear_bias_measfile):
                
                newList.append(shear_bias_measfile)
                
    sbml_list=[]
    if os.path.exists(shear_bias_measurement_listfile):
        sbml_list=read_listfile(shear_bias_measurement_listfile)
    sbml_list.extend(newList)
    write_listfile(shear_bias_measurement_listfile,sbml_list)
    
    
    # What are the main outputs needed for 2nd part?
    # rename? shear_bias_measurements, 
    
    # All the shear_bias_measurements -- collate into .json file
    
    return

