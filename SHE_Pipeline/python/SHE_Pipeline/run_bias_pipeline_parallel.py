""" @file run_pipeline.py

    Created 4 July 2018

    Main executable for running pipelines in parallel
"""

__updated__ = "2018-08-16"

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

import os
import math
import time
import numpy
import multiprocessing
from collections import namedtuple
from astropy.table import Table
from astropy.io import fits
import subprocess as sbp
from   subprocess import Popen, PIPE, STDOUT




from SHE_PPT import products
from SHE_PPT.file_io import (find_file, find_aux_file, get_allowed_filename, 
                             read_xml_product, read_listfile, write_listfile)
from SHE_PPT.logging import getLogger
import SHE_Pipeline.run_pipeline as rp
import SHE_GST_PrepareConfigs.write_configs as gst_prep_conf
from SHE_GST_GalaxyImageGeneration.run_from_config import run_from_args
import SHE_GST_cIceBRGpy
from SHE_GST_GalaxyImageGeneration.generate_images import generate_images
import SHE_GST_GalaxyImageGeneration.GenGalaxyImages as ggi

#from SHE_Pipeline_pkgdef.package_definition import (she_prepare_configs, she_simulate_images, she_estimate_shear,
#                                                    she_measure_statistics, she_measure_bias,
#                                                    she_cleanup_bias_measurement)

default_workdir = "/home/user/Work/workspace"
default_logdir = "logs"
default_cluster_workdir = "/workspace/lodeen/workdir"

non_filename_args = ("workdir", "logdir", "pkgRepository", "pipelineDir")


ERun_CTE = "E-Run SHE_CTE 0.5 "
ERun_GST = "E-Run SHE_GST 1.5 "
ERun_MER = "E-Run SHE_MER 0.1 "


def she_prepare_configs(simulation_plan,config_template,
        pipeline_config,simulation_configs,workdir):
    """
    """
    
    gst_prep_conf.write_configs_from_plan(
        plan_filename=simulation_plan,
            template_filename=config_template,
            listfile_filename=simulation_configs,
            workdir=workdir)
    
    #cmd = (ERun_GST + "SHE_GST_PrepareConfigs --simulation_plan %s "
    #        "--config_template %s --pipeline_config %s --simulation_configs %s "
    #        "--workdir %s"
    #        % (simulation_plan,config_template,
    #    pipeline_config,simulation_configs,workdir))
    
    # Needs to complete before return!
    #sbp.call(cmd,shell=True)
    #isComplete=False
    #while not isComplete:
    #    if os.path.exists(os.path.join(workdir,simulation_configs)):
    #        isComplete=True
    #    else:
    #        time.sleep(60)
    return

def she_simulate_images(config_files,pipeline_config,data_images,
    stacked_data_image, psf_images_and_tables,segmentation_images,
    stacked_segmentation_image,detections_tables,details_table,
    workdir):
    """
    """
    #SHE_GST_cIceBRGpy.set_workdir(workdir)
    
    #args_parser=ggi.defineSpecificProgramOptions()
    #gen_image_args=ArgsTuple(*[config_files,pipeline_config,data_images,
    #stacked_data_image, psf_images_and_tables,segmentation_images,
    #stacked_segmentation_image,detections_tables,details_table,
    #workdir])
    #args=("--config_files %s "
    #    "--pipeline_config %s --data_images %s --stacked_data_image %s "
    #    "--psf_images_and_tables %s --segmentation_images %s "
    #    "--stacked_segmentation_image %s --detections_tables %s "
    #    "--details_table %s --workdir %s" 
    #    % (config_files,pipeline_config,data_images,
    #    stacked_data_image,psf_images_and_tables,segmentation_images,
    #    stacked_segmentation_image,detections_tables,details_table,
    #    workdir))
    
    #args_parser.parse_args(args)
    
    #run_from_args(generate_images, args_parser)
    
    
    # ,data_images,
    #    stacked_data_image,psf_images_and_tables,segmentation_images,
    #    stacked_segmentation_image,detection_table,details_table):   
    cmd=(ERun_GST + "SHE_GST_GenGalaxyImages --config_files %s "
        "--pipeline_config %s --data_images %s --stacked_data_image %s "
        "--psf_images_and_tables %s --segmentation_images %s "
        "--stacked_segmentation_image %s --detections_tables %s "
        "--details_table %s --workdir %s" 
        % (config_files,pipeline_config,data_images,
        stacked_data_image,psf_images_and_tables,segmentation_images,
        stacked_segmentation_image,detections_tables,details_table,
        workdir))
    
    external_process_run(cmd, raiseOnError=False)
    #sbp.call(cmd,shell=True)
    #isComplete=False
    #while not isComplete:
    #    if os.path.exists(os.path.join(workdir,data_images)):
    #        isComplete=True
    #    else:
    #        time.sleep(60)
    return
 
def she_estimate_shear(data_images,stacked_image,
    psf_images_and_tables, segmentation_images,
    stacked_segmentation_image, detections_tables,
    bfd_training_data, ksb_training_data,
    lensmc_training_data,momentsml_training_data,
    regauss_training_data, pipeline_config,
    shear_estimates_product,workdir):
    """
    """
    # Optional data???
    
    cmd=(ERun_CTE + "SHE_CTE_EstimateShear --data_images %s "
        "--stacked_image %s --psf_images_and_tables %s "
        "--segmentation_images %s --stacked_segmentation_image %s "
        "--detections_tables %s --bfd_training_data %s --ksb_training_data %s "
        "--lensmc_training_data %s --momentsml_training_data %s "
        "--regauss_training_data %s --pipeline_config %s "
        "--shear_estimates_product %s --workdir %s" %
        (data_images,stacked_image,psf_images_and_tables,
         segmentation_images,   stacked_segmentation_image, detections_tables,
         bfd_training_data, ksb_training_data, lensmc_training_data,
         momentsml_training_data,regauss_training_data,pipeline_config,
         shear_estimates_product,workdir))
     
    external_process_run(cmd, raiseOnError=False)
    #sbp.call(cmd,shell=True)
    #isComplete=False
    #while not isComplete:
    #    if os.path.exists(os.path.join(workdir,shear_estimates_product)):
    #        isComplete=True
    #    else:
    #        time.sleep(60)
    return

def she_measure_statistics(details_table, shear_estimates,
    pipeline_config,shear_bias_statistics,workdir):
    """
    
    """
    
    cmd=(ERun_CTE + "SHE_CTE_MeasureStatistics --details_table %s "
        "--shear_estimates %s --pipeline_config %s --shear_bias_statistics %s "
        "--workdir %s"
        % (details_table, shear_estimates, pipeline_config,shear_bias_statistics,
           workdir))
    
    external_process_run(cmd, raiseOnError=False)
    
    #sbp.call(cmd,shell=True)
    #isComplete=False
    #while not isComplete:
    #    if os.path.exists(os.path.join(workdir,shear_bias_statistics)):
    #        isComplete=True
    #    else:
    #        time.sleep(60)
    return

def she_cleanup_bias_measurement(simulation_config,data_images, 
    stacked_data_image, psf_images_and_tables, segmentation_images,
    stacked_segmentation_image, detections_tables, details_table,
    shear_estimates, shear_bias_statistics_in, pipeline_config,
    shear_bias_measurements,workdir):
    """
    """
    
    cmd=(ERun_CTE + "SHE_CTE_CleanupBiasMeasurement --simulation_config %s "
        "--data_images %s --stacked_data_image %s --psf_images_and_tables %s "
        "--segmentation_images %s --stacked_segmentation_image %s "
        "--detections_tables %s --details_table %s --shear_estimates %s "
        "--shear_bias_statistics_in %s --pipeline_config %s "
        "--shear_bias_statistics_out %s --workdir %s" % (simulation_config,data_images, 
        stacked_data_image, psf_images_and_tables, segmentation_images,
        stacked_segmentation_image, detections_tables, details_table,
        shear_estimates, shear_bias_statistics_in, pipeline_config,
        shear_bias_measurements,workdir))
    
    external_process_run(cmd, raiseOnError=False)
    #sbp.call(cmd,shell=True)
    #isComplete=False
    #while not isComplete:
    #    if os.path.exists(os.path.join(workdir,shear_bias_measurements)):
    #        isComplete=True
    #    else:
    #        time.sleep(60)
    return


def she_measure_bias(shear_bias_measurement_list,pipeline_config,
    shear_bias_measurement_final,workdir):
    """
    """
    cmd=(ERun_CTE + "SHE_CTE_MeasureBias --shear_bias_statistics %s "
        "--pipeline_config %s --shear_bias_measurements %s --workdir %s" 
        % (shear_bias_measurement_list,pipeline_config,
           shear_bias_measurement_final,workdir))
    
    sbp.call(cmd,shell=True)
    external_process_run(cmd, raiseOnError=False)
    #isComplete=False
    #while not isComplete:
    #    if os.path.exists(os.path.join(workdir,shear_bias_measurement_final)):
    #        isComplete=True
    #    else:
    #        time.sleep(60)
    return

def get_pipeline_dir():
    """Gets the directory containing the pipeline packages, using the location of this module.
    """

    logger = getLogger(__name__)

    this_file_name = __name__.replace(".", "/") + '.py'
    pipeline_dir = __file__.replace('/' + this_file_name, '')

    return pipeline_dir


def check_args(args):
    """Checks arguments for validity and fixes if possible.
    """

    logger = getLogger(__name__)

    logger.debug('# Entering SHE_Pipeline_RunBiasParallel check_args()')

    pipeline='bias_measurement'
    # Does the pipeline we want to run exist?
    pipeline_filename = os.path.join(get_pipeline_dir(), "SHE_Pipeline_pkgdef/"+ pipeline + ".py")
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

    # Set up the workdir and app_workdir the same way

    #if args.workdir == args.app_workdir:
    workdirs = (args.workdir,)
    #else:
    #    workdirs = (args.workdir), args.app_workdir,)

    if args.number_threads is None:
        args.number_threads = str(multiprocessing.cpu_count()-1)
    if not args.number_threads.isdigit():
        raise ValueError("Invalid values passed to 'number-threads': Must be an integer.")

    # @TODO: Be careful, workdir and app_workdir...
    # make sure number threads is valid 
    nThreads= max(1,min(int(args.number_threads),multiprocessing.cpu_count()-1))
    
    dirStruct = create_thread_dir_struct(args,workdirs,int(nThreads))
    
    # Check that pipeline specific args are only provided for the right pipeline
    if args.plan_args is None:
        args.plan_args = []
    if not len(args.plan_args) % 2 == 0:
        raise ValueError("Invalid values passed to 'plan_args': Must be a set of paired arguments.")
    
    
        

    return dirStruct





def create_batches(args,sim_config_list,workdirList):
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
    """ Returns simulation no.
    """
    
    return batch.min_sim_no+thread_no

def insert_data_to_threads(args,batch,workdirList,sim_plan_table,isfFileName):
    # @FIXME: do this later, at beginning of batch
    new_plan_filename = get_allowed_filename("SIM-PLAN", str(os.getpid()), extension=".fits", release="00.03")
    
    dataFiles = get_data_file_list(args.workdir)
    
    
    for thread_no in range(batch.nThreads):
       
        # Soft link files from data/cache etc to thread data/cache etc.
        
        # But not simulation plan
        create_soft_links(dataFiles,workdirList[thread_no].workdir)
       
        simulation_no=batch.min_sim_no+thread_no
       
         
        # @FIXME: Update with batch no...
        # thread/batch...
        qualified_new_plan_filename=os.path.join(
            workdirList[thread_no].workdir,
            new_plan_filename) 
        # Update NSEED, MSEED, set NUM_DET=1  
        # Does this need to be updated?                
        batch_simulation_plan_table=update_sim_plan_table(
            sim_plan_table,simulation_no)
        # Write out the new plan
        if not os.path.exists(os.path.dirname(qualified_new_plan_filename)):
            os.mkdir(os.path.dirname(qualified_new_plan_filename))

        batch_simulation_plan_table.write(qualified_new_plan_filename, format="fits")
       
        # copy / modify ISF file
        
        

    return 

def get_data_file_list(main_workdir):
    """ Trawl through directory structure avoiding threads, logdir,
    simulation_plan
    
    """
    final_file_list=[]
    directoryList=['']
    isComplete=False
    while not isComplete:
        new_dirs=[]
        for directory in directoryList:
            file_list,sub_dirs= process_directory(directory,main_workdir)
            final_file_list.extend(file_list)
            new_dirs.extend(sub_dirs)
        if new_dirs:
            directoryList=new_dirs
        else:
            isComplete=True
    return final_file_list

def process_directory(directory,main_workdir):   
    """ Function that returns file_list and sub directories. 
    """ 
    file_list=[]
    sub_dirs=[]
    main_files = os.listdir(os.path.join(main_workdir,directory))
    for filename in main_files:
        
        if 'SIM-PLAN' in filename or 'SHE-ISF' in filename or filename.startswith('thread'):
            continue
        
        if os.path.isdir(os.path.join(main_workdir,directory,filename)):
            sub_dirs.append(os.path.join(directory,filename))
        else:
            file_list.append(os.path.join(directory,filename))
    return file_list,sub_dirs


def create_soft_links(dataFiles,new_workdir):
    """
    """
    for filename in dataFiles:
        new_filename = os.path.join(new_workdir,filename)
        #try:
        os.symlink(filename,new_filename)
        #except:
        #   Exce 
    

def get_seeds(sim_plan_table,simulation_no):
    """
    
     
    """
    # @FIXME: Inefficient. 
    # Do most of this once - create batches..
    # look up simumation_no --> tableRow,seeds...
    
    # Find correct row in table
    # Update NSEED/MSEED values 
    number_simulations=0
    table_row=-1
    mseed=1
    nseed=1
    for row_id in range(len(sim_plan_table['MSEED_MAX'])):
        ns = ((sim_plan_table['MSEED_MAX'][row_id]-
                                sim_plan_table['MSEED_MIN'][row_id])//
                               sim_plan_table['MSEED_STEP'][row_id])+1
        nsimul_prev_rows= number_simulations
        number_simulations+=ns
        if simulation_no<=number_simulations and table_row==-1:
            table_row=row_id
            mseed=sim_plan_table['MSEED_MIN'][row_id]+(simulation_no-nsimul_prev_rows)*sim_plan_table['MSEED_STEP'][row_id]
            nseed=sim_plan_table['NSEED_MIN'][row_id]+(simulation_no-nsimul_prev_rows)*sim_plan_table['NSEED_STEP'][row_id]
    return mseed,nseed,table_row

def create_config(args, workdir, batch):
    """Function to create a new pipeline_config file for this run.
    """

    logger = getLogger(__name__)

    # Find the base config we'll be creating a modified copy of
    base_config = find_file(args.config, path=args.workdir)
    new_config_filename = get_allowed_filename("PIPELINE-CFG", str(os.getpid()), extension=".txt", release="00.03")
    qualified_config_filename = os.path.join(workdir.workdir, 
        new_config_filename.replace('.txt','_batch%s.txt' % batch.batch_no))
    if not os.path.exists(os.path.dirname(qualified_config_filename)):
        os.mkdir(os.path.dirname(qualified_config_filename))
    
    # Set up the args we'll be replacing or setting
    args_to_set = {}

    arg_i = 0
    while arg_i < len(args.config_args):
        args_to_set[args.config_args[arg_i]] = args.config_args[arg_i + 1]
        arg_i += 2

    with open(base_config, 'r') as fi:
        # Check each line to see if values we'll overwrite are specified in it,
        # and only write out lines with other values
        for line in fi:
            split_line = line.strip().split('=')
            # Add any new args here to the list of args we want to set
            key = split_line[0].strip()
            if not (key in args_to_set) and len(split_line) > 1:
                args_to_set[key] = split_line[1].strip()

    # Write out the new config
    with open(qualified_config_filename, 'w') as fo:
        # Write out values we want set specifically
        for arg in args_to_set:
            fo.write(arg + "=" + args_to_set[arg] + "\n")

    return new_config_filename

def isFilename(strValue):
    """
    """
    if len(os.path.dirname(strValue))>0:
        return True
    

def create_simulate_measure_inputs(args, config_filename,workdir,sim_config_list,
        simulation_no):
    """Function to create a new ISF for this run by adjusting workdir and logdir, and overwriting any
       values passed at the command-line.
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
    args_to_set["pkgRepository"] = get_pipeline_dir()
    args_to_set["pipelineDir"] = os.path.join(get_pipeline_dir(), 
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
     

    # Check each filename arg to see if it's already in the workdir, or if we have to move it there

    # Create a search path from the workdir, the root directory (using an empty string), and the current
    # directory
    
    # set up SIM-PLAN
    
    
    
    # Write out the new plan
    #qualified_new_plan_filename = os.path.join(workdir.workdir,
    #        args_to_set['simulation_plan'])
    
    #if not os.path.exists(os.path.dirname(qualified_new_plan_filename)):
    #    os.mkdir(os.path.dirname(qualified_new_plan_filename))
    #batch_simulation_plan_table.write(qualified_new_plan_filename, format="fits")
    
    #simulation_config=create_simulation_config_file(
    #    sim_plan_table,find_file(args_to_set['config_template'],args.workdir),
    #   workdir.workdir,simulation_no,simPlanSimNo)
    
    simulation_config = read_listfile(os.path.join(
        args.workdir,sim_config_list))[simulation_no]
    args_to_set['simulation_config']=simulation_config
    
    search_path = args.workdir
    simulateInputs = InputsTuple(*[
        os.path.join(workdir.workdir,args_to_set['simulation_config']),
        os.path.join(workdir.workdir,args_to_set['bfd_training_data']),
        os.path.join(workdir.workdir,args_to_set['ksb_training_data']),
        os.path.join(workdir.workdir,args_to_set['lensmc_training_data']),
        os.path.join(workdir.workdir,args_to_set['momentsml_training_data']),
        os.path.join(workdir.workdir,args_to_set['regauss_training_data']),
        os.path.join(workdir.workdir,args_to_set['pipeline_config'])])
    
    
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

    return simulateInputs

def create_simulation_config_file(sim_plan_table,config_template,workdir,
        simulation_no,simPlanSimNo):
    """ Replaces values in template and writes out config file
    """

    mseed=simPlanSimNo.mseed[simulation_no]
    nseed=simPlanSimNo.nseed[simulation_no]
    tableRow=simPlanSimNo.table_row[simulation_no]
    
    replaceDict={'SEED':mseed,
                 'NOISESEED':nseed,
                 'SUPPRESSNOISE':sim_plan_table['SUP_NOISE'][tableRow],
                 'NUMDETECTORS':sim_plan_table['NUM_DETECTORS'][tableRow],
                 'NUMGALAXIES':sim_plan_table['NUM_GALAXIES'][tableRow],
                 'RENDERBACKGROUND':sim_plan_table['RENDER_BKG'][tableRow],
                 }
    

    simulation_config_file = os.path.join(workdir,'data',
        os.path.basename(config_template.replace('Template','SimConfig')))
    
    lines = open(config_template).readlines()
    outLines=[]
    for line in lines:
        if '$REPLACEME' in line:
            keyVal=[('$REPLACEME_%s' % repKey,replaceDict[repKey]) 
                    for repKey in replaceDict if '$REPLACEME_%s' % repKey in line]
            for key,val in keyVal:
                line=line.replace(key,"%s" % val)
                    
        outLines.append(line)
            
    open(simulation_config_file,'w').writelines(outLines)
    return simulation_config_file

def execute_pipeline(pipeline, isf):
    """Sets up and calls a command to execute the pipeline.
    """

    logger = getLogger(__name__)

    #cmd = ('pipeline_runner.py --pipeline=' + pipeline + '.py --data=' + isf) # + ' --serverurl="' + serverurl + '"')
    logger.info("Calling pipeline with command: '" + cmd + "'")

    # Do not use subprocess - replace. 
    # System call, or pipeline_runner directly run - but pipeline runner seems to be
    # a python 2.7 code?
    # What does subprocess.call with shell=True do - env var?
    #sbp.call(cmd, shell=True)

    bm.she_simulate_and_measure_bias_statistics(simulation_config,
                                             bfd_training_data,
                                             ksb_training_data,
                                             lensmc_training_data,
                                             momentsml_training_data,
                                             regauss_training_data,
                                             pipeline_config)
    
    
    

    return


def create_thread_dir_struct(args,workdirList,number_threads):
    """ @obsolete ???
    """
    logger = getLogger(__name__)

    # Creates directory structure
    DirStruct = namedtuple("Directories","workdir logdir app_workdir app_logdir")
    # @FIXME: Do the create multiple threads here
    for workdir_base in workdirList:

        # Does the workdir exist?
        if not os.path.exists(workdir_base):
            # Can we create it?
            try:
                os.mkdir(workdir_base)
            except Exception as e:
                logger.error("Workdir base (" + workdir_base + ") does not exist and cannot be created.")
                raise e
        if args.cluster:
            os.chmod(workdir_base, 0o777)
        # Does the cache directory exist within the workdir?
        cache_dir = os.path.join(workdir_base, "cache")
        if not os.path.exists(cache_dir):
            # Can we create it?
            try:
                os.mkdir(cache_dir)
            except Exception as e:
                logger.error("Cache directory (" + cache_dir + ") does not exist and cannot be created.")
                raise e
        if args.cluster:
            os.chmod(cache_dir, 0o777)

        # Does the data directory exist within the workdir?
        data_dir = os.path.join(workdir_base, "data")
        if not os.path.exists(data_dir):
            # Can we create it?
            try:
                os.mkdir(data_dir)
            except Exception as e:
                logger.error("Data directory (" + data_dir + ") does not exist and cannot be created.")
                raise e
        if args.cluster:
            os.chmod(data_dir, 0o777)    
    
    # Now make multiple threads below...
        
    directStrList=[]        
    for thread_no in range(number_threads):
        thread_dir_list=[]
        for workdir_base in workdirList:
            workdir=os.path.join(workdir_base,'thread%s' % thread_no) 
            if not os.path.exists(workdir):
                try:
                   os.mkdir(workdir)
                except Exception as e:
                    logger.error("Workdir thread (" + workdir + ") does not exist and cannot be created.")
                    raise e
            if args.cluster:
                os.chmod(workdir, 0o777)
    
            # Does the cache directory exist within the workdir?
            cache_dir = os.path.join(workdir, "cache")
            if not os.path.exists(cache_dir):
                # Can we create it?
                try:
                    os.mkdir(cache_dir)
                except Exception as e:
                    logger.error("Cache directory (" + cache_dir + ") does not exist and cannot be created.")
                    raise e
            if args.cluster:
                os.chmod(cache_dir, 0o777)
    
            # Does the data directory exist within the workdir?
            data_dir = os.path.join(workdir, "data")
            if not os.path.exists(data_dir):
                # Can we create it?
                try:
                    os.mkdir(data_dir)
                except Exception as e:
                    logger.error("Data directory (" + data_dir + ") does not exist and cannot be created.")
                    raise e
            if args.cluster:
                os.chmod(data_dir, 0o777)
    
            # Does the logdir exist?
            qualified_logdir = os.path.join(workdir, args.logdir)
            if not os.path.exists(qualified_logdir):
                # Can we create it?
                try:
                    os.mkdir(qualified_logdir)
                except Exception as e:
                    logger.error("logdir (" + qualified_logdir + ") does not exist and cannot be created.")
                    raise e
            if args.cluster:
                os.chmod(qualified_logdir, 0o777)
            thread_dir_list.extend((workdir,qualified_logdir))
        if len(workdirList)==1:
            thread_dir_list.extend((None,None))
        directStrList.append(DirStruct(*thread_dir_list))
    return directStrList

def she_simulate_and_measure_bias_statistics(simulation_config,
        bfd_training_data, ksb_training_data,
        lensmc_training_data, momentsml_training_data,
        regauss_training_data,pipeline_config,workdir,
        simulation_no):
    """ Function that runs parallel parts of bias_measurement pipeline
    """
    # several commands...
    # @FIXME: check None types.
    
    logger = getLogger(__name__)

     
    data_image_list = os.path.join('data','data_images.json')
    stacked_data_image =  os.path.join('data','stacked_image.xml')
    psf_images_and_tables = os.path.join('data','psf_images_and_tables.json')
    segmentation_images = os.path.join('data','segmentation_images.json')
    stacked_segmentation_image = os.path.join('data','stacked_segm_image.xml')
    detections_tables=os.path.join('data','detections_tables.json')
    details_table=os.path.join('data','details_table.xml')
    
    she_simulate_images(simulation_config, pipeline_config, data_image_list,
        stacked_data_image,psf_images_and_tables,segmentation_images,
        stacked_segmentation_image,detections_tables,details_table,workdir) 
    

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
        workdir=workdir)


    shear_bias_statistics = os.path.join('data','shear_bias_statistics.xml')
    
    she_measure_statistics(details_table=details_table,
        shear_estimates=shear_estimates_product,
        pipeline_config=pipeline_config,
        shear_bias_statistics=shear_bias_statistics,
        workdir=workdir)

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
        workdir=workdir)
    #        hasRun=True
    #    else:
    #        time.sleep(60)
    #    ii+=1
            
    logger.info("Completed parallel pipeline stage, she_simulate_and_measure_bias_statistics")                                                     

    return 

def run_pipeline_from_args(args):
    """Main executable to run pipelines.
    """

    logger = getLogger(__name__)

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
    logger.info("Preparing configurations")
    
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
    she_prepare_configs(sim_plan_tablename,config_template,
        config_filename,simulation_configs,args.workdir)
    
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
                      workdir.workdir,simulation_no)))
        
        if prodThreads:
            runThreads(prodThreads)
        logger.info("Run batch %s in parallel, now to merge outputs from threads" % batch.batch_no)
        mergeOutputs(workdirList,batch,shear_bias_measurement_listfile)
        # Clean up 
        logger.info("Cleaning up batch files..")   
        cleanup(batch,workdirList)
    

    # Run final process
    shear_bias_measurement_final=os.path.join(args.workdir,'data','shear_bias_measurements_final.xml')
    
    logger.info("Running final she_measure_bias to calculate "
        "final shear: output in %s" % shear_bias_measurement_final)
    she_measure_bias(shear_bias_measurement_listfile,config_filename,
        shear_bias_measurement_final,args.workdir)
    logger.info("Parallel pipeline completed!")
    
    
    return


def mergeOutputs(workdirList,batch,
        shear_bias_measurement_listfile):
    """ Merge outputs from different threads
    
    
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

def cleanup(batch,workdirList):
    """
    Remove sim links and batch setup files ready for the next batch.
    Remove intermediate products...
    
    """
    # workdir:
    #*.bin
    
    
    
    
    pass


def runThreads(threads):
    """ Executes given list of thread processes.
    """
     
    logger = getLogger(__name__)
    try:
        for thread in threads:
            thread.start()
    finally:
        threadFail = False
        for thread in threads:
            if threadFail:
                thread.terminate()
                thread.join()
            else:
                thread.join()
                threadFail = thread.exitcode
                if threadFail:
                    logger.info("<ERROR> Thread failed. Terminating all"
                                      " other running threads.")

        if threadFail:
            raise Exception("Forked processes failed. Please check stdout.")
 
def external_process_run(command, stdIn='', raiseOnError=True, parseStdOut=True, cwd=None,
        env=None, close_fds=True, isVerbose=True, _isIterable=False,
        ignoreMsgs=None):
    """
    Run the given external program. Unless overridden, an exception is thrown
    on error and the output is logged.

    @param command:      Command string to execute. Use a single string with
                         all arguments to run in shell, use a list of the
                         command with arguments as separate elements to not
                         run in a shell (faster if shell not needed).
    @type  command:      str or list(str)
    @param stdIn:        Optionally supply some input for stdin.
    @type  stdIn:        str
    @param raiseOnError: If True, if the external process sends anything to
                         stdErr then an exception is raised and the complete
                         programme is logged. Otherwise, stdErr is just always
                         redirected to stdOut.
    @type  raiseOnError: bool
    @param parseStdOut:  If True, stdout is captured, not print to screen, and
                         returned by this function, otherwise stdout is left
                         alone and will be sent to terminal as normal.
    @type  parseStdOut:  bool
    @param cwd:          Run the external process with this directory as its
                         working directory.
    @type  cwd:          str
    @param env:          Environment variables for the external process.
    @type  env:          dict(str:str)
    @param close_fds:    If True, close all open file-like objects before
                         executing external process.
    @type  close_fds:    bool
    @param isVerbose:    If False, don't log the full command that was
                         executed, even when Logger is in verbose mode.
    @type  isVerbose:    bool
    @param _isIterable:  Return an iterable stdout. NB: Use the L{out()}
                         function instead of this option.
    @type  _isIterable:  bool
    @param ignoreMsgs:   List of strings that if they appear in stderr should
                         override the raiseOnError if it is set to True.
    @type  ignoreMsgs:   list(str)

    @return: Messages sent to stdout if parsed, otherwise an iterable file
             object for stdout if _isIterable, else a return a code.
    @rtype:  list(str) or file or int

    """
    logger = getLogger(__name__)
    cmdStr = (command if isinstance(command, str) else ' '.join(command))
    if isVerbose:
        logger.info(cmdStr)

    parseStdOut = parseStdOut or _isIterable
    parseStdErr = raiseOnError and not _isIterable
    isMemError = False
    while True:
        try:
            proc = Popen(command, shell=isinstance(command, str),
                         stdin=(PIPE if stdIn else None),
                         stdout=(PIPE if parseStdOut else None),
                         stderr=(PIPE if parseStdErr else STDOUT),
                         close_fds=close_fds, cwd=cwd, env=env)
        except OSError as error:
            if "[Errno 12] Cannot allocate memory" not in str(error):
                raise
            if not isMemError:
                logger.info("Memory allocation problem; delaying...")
                isMemError = True
                close_fds = True
            time.sleep(60)
        else:
            if isMemError:
                Logger.addMessage("Problem fixed; continuing...")
            break

    if stdIn:
        proc.stdin.write(stdIn + '\n')
        proc.stdin.flush()

    if _isIterable:
        return proc.stdout

    stdOut = []
    stdErr = []
    try:
        if parseStdOut:
            # Calling readlines() instead of iterating through stdout ensures
            # that KeyboardInterrupts are handled correctly.
            stdOut = [line.strip() for line in proc.stdout.readlines()]
        if raiseOnError:
            stdErr = [line.strip() for line in proc.stderr]

        if not parseStdOut and not raiseOnError:
            return proc.wait()
#    except KeyboardInterrupt:#        # Block future keyboard interrupts until process has finished cleanly
#        with utils.noInterrupt():
#            Logger.addMessage("KeyboardInterrupt - %s interrupted, "
#              "waiting for process to end cleanly..." %
#              os.path.basename(command.split()[0]))
#            if parseStdOut:
#                print(''.join(proc.stdout))
#            if parseStdErr:
#                print(''.join(proc.stderr))
#            proc.wait()
#        raise
    except IOError as error:
        # Sometimes a KeyboardInterrupt is translated into an IOError - I think
        # this may just be due to a bug in PyFITS messing with signals, as only
        # seems to happen when the PyFITS ignoring KeyboardInterrupt occurs.
        if "Interrupted system call" in str(error):
            raise KeyboardInterrupt
        raise

    # If the stdErr messages are benign then ignore them
    if stdErr and ignoreMsgs:
        for stdErrStr in stdErr[:]:
            if any(msg in stdErrStr for msg in ignoreMsgs):
                stdErr.remove(stdErrStr)

    if stdErr:
        if raiseOnError and (not isVerbose):
            logger.info(cmdStr)

        for line in stdOut:
            logger.info(line)

        for line in stdErr:
            logger.info('# ' + str(line))

        if raiseOnError:
            cmd = cmdStr.split(';')[-1].split()[0]
            if cmd == "python":
                cmd = ' '.join(cmdStr.split()[:2])

            raise Exception(cmd + " failed", stdErr)

    return stdOut  