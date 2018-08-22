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



from SHE_PPT import products
from SHE_PPT.file_io import find_file, find_aux_file, get_allowed_filename, read_xml_product
from SHE_PPT.logging import getLogger
import SHE_Pipeline.run_pipeline as rp

default_workdir = "/home/user/Work/workspace"
default_logdir = "logs"
default_cluster_workdir = "/workspace/lodeen/workdir"

non_filename_args = ("workdir", "logdir", "pkgRepository", "pipelineDir", "pipeline_config")


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

    # Use the default app_workdir if necessary
    if args.app_workdir is None:
        if args.cluster:
            args.app_workdir = default_cluster_workdir
        else:
            args.app_workdir = default_workdir
        logger.info('No app_workdir supplied at command-line. Using default app_workdir: ' + args.app_workdir)

    # Use the default logdir if necessary
    if args.logdir is None:
        args.logdir = default_logdir
        logger.info('No logdir supplied at command-line. Using default logdir: ' + args.logdir)

    # Set up the workdir and app_workdir the same way

    if args.workdir == args.app_workdir:
        workdirs = (args.workdir,)
    else:
        workdirs = (args.workdir, args.app_workdir,)

    if args.number_threads is None:
        args.number_threads = multiprocessing.cpu_count()-1
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





def create_batches(simulation_plan_table,workdirList):
    # Overwrite values as necessary
    # @TODO: Do we do this multiple times and save multiple times - or add multiple lines?
    BatchTuple=namedtuple("Batch", "batch_no nThreads min_sim_no max_sim_no") 
    
    #keyVals= [(key,simulation_plan_table[key]) for key in simulation_plan_table]
    #print("KV: ",keyVals)    
    
    # @TODO: Improve
    number_simulations = numpy.sum(((simulation_plan_table['NSEED_MAX']-
                                simulation_plan_table['NSEED_MIN'])//
                               simulation_plan_table['NSEED_STEP'])+1)
    
    number_batches = math.ceil(number_simulations/len(workdirList))
    batchList=[]
    

    for batch_no in range(number_batches):
        
        max_thread_no=len(workdirList)-1
        min_sim_no = len(workdirList)*batch_no
        max_sim_no = len(workdirList)*(batch_no+1)
        if max_sim_no>number_simulations:
            max_thread_no = number_simulations-min_sim_no
            max_sim_no = number_simulations
            
        batchList.append(BatchTuple(batch_no,max_thread_no,min_sim_no,max_sim_no))
    

    return batchList


def insert_data_to_threads(args,batch,workdirList,sim_table):
    # @FIXME: do this later, at beginning of batch
    new_plan_filename = get_allowed_filename("SIM-PLAN", str(os.getpid()), extension=".fits", release="00.03")

    dataFiles = get_data_file_list(args.workdir, new_plan_filename)
    
    
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
            simulation_plan_table,simulation_no)
        # Write out the new plan
        if not os.path.exists(os.path.dirname(qualified_new_plan_filename)):
            os.mkdir(os.path.dirname(qualified_new_plan_filename))

        batch_simulation_plan_table.write(qualified_new_plan_filename, format="fits")
       
    

    return 

def get_data_file_list(main_workdir, sim_plan):
    """ Trawl through directory structure avoiding threads, logdir,
    simulation_plan
    
    """
    final_file_list=[]
    directoryList=['']
    isComplete=False
    while not isComplete:
        new_dirs=[]
        for directory in directoryList:
            file_list,sub_dirs= process_directory(directory,main_workdir,sim_plan)
            final_file_list.extend(file_list)
            new_dirs.extend(sub_dirs)
        if new_dirs:
            directoryList=new_dirs
        else:
            isComplete=True
    return final_file_list

def process_directory(directory,main_workdir,sim_plan):   
    """ Function that returns file_list and sub directories. 
    """ 
    file_list=[]
    sub_dirs=[]
    main_files = os.listdir(os.path.join(main_workdir,directory))
    for filename in main_files:
        if filename.endswith(sim_plan) or filename.startswith('thread'):
            pass
        if os.path.isdir(filename):
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
    

def update_sim_plan_table(sim_plan_table,simulation_no):
    """
    
    # Update NSEED, MSEED 
    """
    # Find correct row in table
    # Update NSEED/MSEED values 
    number_simulations=0
    table_row=-1
    min_mseed=1
    min_nseed=1
    for row_id in range(len(sim_plan_table['MSEED_MAX'])):
        ns = ((sim_plan_table['MSEED_MAX'][row_id]-
                                sim_plan_table['MSEED_MIN'][row_id])//
                               sim_plan_table['MSEED_STEP'][row_id])+1
        number_simulations+=ns
        if simulation_no<=number_simulations and table_row==-1:
            table_row=row_id
            min_mseed=sim_plan_table['MSEED_MIN'][row_id]+(simulation_no-ns)*sim_plan_table['MSEED_STEP'][row_id]
            min_nseed=sim_plan_table['NSEED_MIN'][row_id]+(simulation_no-ns)*sim_plan_table['NSEED_STEP'][row_id]
            
    batch_sim_plan_tab=sim_plan_table[table_row:table_row+1]
    batch_sim_plan_tab['MSEED_MIN']=min_mseed
    batch_sim_plan_tab['NSEED_MIN']=min_nseed
    batch_sim_plan_tab['MSEED_MAX']=min_mseed+sim_plan_table['MSEED_STEP']
    batch_sim_plan_tab['NSEED_MAX']=min_nseed+sim_plan_table['NSEED_STEP']
    return batch_sim_plan_tab

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
    
def convertToBatchFilename(filename,batch):
    """
    """
    if '_batch%s' % batch.batch_no in filename:
        return filename
    parts=filename.split('.')
    root='.'.join(parts[:-1])
    ext=parts[-1]
    return root+'_batch%s.%s' % (batch.batch_no,ext)

def create_isf(args,
               config_filename,workdir,batch):
    """Function to create a new ISF for this run by adjusting workdir and logdir, and overwriting any
       values passed at the command-line.
    """

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
    search_path = args_to_set["workdir"] + ":" + os.path.abspath(
        os.path.curdir) + ":"

    
    for input_port_name in args_to_set:
        print("IPN: ",input_port_name,args_to_set[input_port_name], non_filename_args)
        # Skip ISF arguments that don't correspond to input ports
        if input_port_name in non_filename_args:
            continue

        filename = args_to_set[input_port_name]
        print("FN: ",filename)
        # Skip if None
        if filename is None or filename == "None":
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

    return qualified_isf_filename


def execute_pipeline(pipeline, isf, serverurl):
    """Sets up and calls a command to execute the pipeline.
    """

    logger = getLogger(__name__)

    cmd = ('pipeline_runner.py --pipeline=' + pipeline + '.py --data=' + isf + ' --serverurl="' + serverurl + '"')
    logger.info("Calling pipeline with command: '" + cmd + "'")

    # Do not use subprocess - replace. 
    # System call, or pipeline_runner directly run - but pipeline runner seems to be
    # a python 2.7 code?
    # What does subprocess.call with shell=True do - env var?
    sbp.call(cmd, shell=True)

    return


def create_thread_dir_struct(args,workdirList,number_threads):
    """
    """
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
    
def run_pipeline_from_args(args):
    """Main executable to run pipelines.
    """

    logger = getLogger(__name__)

    # Check the arguments
    workdirList=check_args(args) # add argument there..
    #if len(args.plan_args) > 0:
    sim_table=rp.create_plan(args, retTable=True)
    batches=create_batches(sim_table,workdirList)
    # Create the pipeline_config for this run
    config_filename = rp.create_config(args)
    
    # Create the ISF for this run
    qualified_isf_filename = rp.create_isf(args, config_filename)

    # Find how many files in 
    
    
    for batch in batches:
        insert_data_to_threads(args,batch,workdirList,sim_table)
        exit()
        # Create the pipeline_config for this run
        # @TODO: Do we need multiple versions of this, one for each thread?
        prodThreads=[]
        for threadNo in range(batch.nThreads):
            workdir = workdirList[threadNo]
            # logdir?
            config_filename = create_config(args,workdir,batch)
    
            # Create the ISF for this run
            # @TODO:  do we need multiple versions of this, one for each thread
            qualified_isf_filename = create_isf(args, config_filename,workdir,batch)
    
            # @TODO: Do we want serverurl? isf and serverurl
            prodThreads.append(multiprocessing.Process(target=execute_pipeline,
                args=('bias_measurement',qualified_isf_filename,
                    args.serverurl)))
        
        if prodThreads:
            runThreads(prodThreads,logger)
        logger.info("Run batch %s in parallel, now to merge outputs from threads" % batch.batch_no)
        mergeOutputs(workdirList,batch)   
        cleanup(batch)
    # Final merge?
    
    return


def mergeOutputs(workdirInfo,batch):
    """ Merge outputs from different threads
    
    
    """
    pass

def runThreads(threads,logger):
    """ Executes given list of thread processes.
    """
     
    try:
        for thread in threads:
            thread.start()
    finally:
        threadFail = False
        for ii,thread in enumerate(threads):
            logger.info("...%s" % threadFail)
            if threadFail:
                thread.terminate()
                thread.join()
            else:
                thread.join()
                threadFail = thread.exitcode
                if threadFail:
                    # @FIXME: No - we don't want to do it this way
                    logger.info("<ERROR> Thread %s failed. Terminating all"
                                       " other running threads." % ii)

        if threadFail:
            raise logger.info("Forked processes failed. Please check stdout.")
