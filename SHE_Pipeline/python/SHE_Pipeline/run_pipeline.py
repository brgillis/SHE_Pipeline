""" @file run_pipeline.py

    Created 4 July 2018

    Main executable for running pipelines.
"""

__updated__ = "2018-09-03"

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


import ast
from copy import deepcopy
import os
from shutil import copyfile
from time import sleep

from astropy.table import Table

from SHE_PPT import products
from SHE_PPT.file_io import (find_file, find_aux_file, get_allowed_filename, read_xml_product,
                             read_pickled_product, write_pickled_product)
from SHE_PPT.logging import getLogger
import subprocess as sbp


default_workdir = "/home/user/Work/workspace"
default_logdir = "logs"
default_cluster_workdir = "/workspace/lodeen/workdir"

non_filename_args = ("workdir", "logdir", "pkgRepository", "pipelineDir", "pipeline_config")

known_config_args = ("SHE_CTE_CleanupBiasMeasurement_cleanup",

                     "SHE_CTE_EstimateShear_methods",

                     "SHE_CTE_MeasureBias_archive_dir",
                     "SHE_CTE_MeasureBias_webdav_archive",
                     "SHE_CTE_MeasureBias_webdav_dir",

                     "SHE_CTE_MeasureStatistics_archive_dir",
                     "SHE_CTE_MeasureStatistics_webdav_archive",
                     "SHE_CTE_MeasureStatistics_webdav_dir",)

known_output_filenames = {"bias_measurement":"she_measure_bias/shear_bias_measurements.xml"}


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

    logger.debug('# Entering SHE_Pipeline_Run check_args()')

    # Is a pipeline specified at all?
    if args.pipeline is None:
        raise IOError("Pipeline must be specified at command-line, e.g with --pipeline bias_measurement")

    # Does the pipeline we want to run exist?
    pipeline_filename = os.path.join(get_pipeline_dir(), "SHE_Pipeline_pkgdef/" + args.pipeline + ".py")
    if not os.path.exists(pipeline_filename):
        logger.error("Pipeline '" + pipeline_filename + "' cannot be found. Expected location: " +
                     pipeline_filename)

    # If no ISF is specified, check for one in the AUX directory
    if args.isf is None:
        try:
            args.isf = find_aux_file("SHE_Pipeline/" + args.pipeline + "_isf.txt")
        except Exception:
            logger.error("No ISF file specified, and cannot find one in default location (" +
                         "AUX/SHE_Pipeline/" + args.pipeline + "_isf.txt).")
            raise

    # If no config is specified, check for one in the AUX directory
    if args.config is None:
        try:
            args.config = find_aux_file("SHE_Pipeline/" + args.pipeline + "_config.txt")
        except Exception:
            logger.error("No config file specified, and cannot find one in default location (" +
                         "AUX/SHE_Pipeline/" + args.pipeline + "_config.txt).")
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

    # Check that all config args are recognized
    for i in range(len(args.config_args) // 2):
        test_arg = args.config_args[2 * i]
        if test_arg not in known_config_args:
            raise ValueError("Config argument \"" + test_arg + "\" not recognized. Allowed arguments are: " +
                             str(known_config_args))

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

    # Use the default local_workdir if necessary
    if args.local_workdir is None:
        if args.cluster:
            args.local_workdir = default_cluster_workdir
        else:
            args.local_workdir = default_workdir
        logger.info('No local_workdir supplied at command-line. Using default local_workdir: ' + args.local_workdir)

    # Use the default logdir if necessary
    if args.logdir is None:
        args.logdir = default_logdir
        logger.info('No logdir supplied at command-line. Using default logdir: ' + args.logdir)

    # Set up the workdir and app_workdir the same way

    if args.workdir == args.app_workdir:
        workdirs = (args.workdir,)
    else:
        workdirs = (args.workdir, args.app_workdir,)

    for workdir in workdirs:

        # Does the workdir exist?
        if not os.path.exists(workdir):
            # Can we create it?
            try:
                os.mkdir(workdir)
            except Exception as e:
                logger.error("Workdir (" + workdir + ") does not exist and cannot be created.")
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

    # Check that pipeline specific args are only provided for the right pipeline
    if args.plan_args is None:
        args.plan_args = []
    if len(args.plan_args) > 0:
        if not args.pipeline == "bias_measurement":
            raise ValueError("plan_args can only be provided for the Bias Measurement pipeline.")
    if not len(args.plan_args) % 2 == 0:
        raise ValueError("Invalid values passed to 'plan_args': Must be a set of paired arguments.")

    return


def create_plan(args, retTable=False):
    """Function to create a new simulation plan for this run.
    """

    logger = getLogger(__name__)

    # Find the base plan we'll be creating a modified copy of

    new_plan_filename = get_allowed_filename("SIM-PLAN", str(os.getpid()), extension=".fits", release="00.03")
    qualified_new_plan_filename = os.path.join(args.workdir, new_plan_filename)

    # Check if the plan is in the ISF args first
    plan_filename = None
    if len(args.isf_args) > 0:
        arg_i = 0
        while arg_i < len(args.isf_args):
            if args.isf_args[arg_i] == "simulation_plan":
                plan_filename = args.isf_args[arg_i + 1]
                # And replace it here with the new name
                args.isf_args[arg_i + 1] = new_plan_filename
                break
            arg_i += 1
    if plan_filename is None:
        # Check for it in the base ISF
        base_isf = find_file(args.isf, path=args.workdir)

        with open(base_isf, 'r') as fi:
            # Check each line to see if it's the simulation plan
            for line in fi:
                split_line = line.strip().split('=')
                # Add any new args here to the list of args we want to set
                if split_line[0].strip() == "simulation_plan":
                    plan_filename = split_line[1].strip()
                    # And add it to the end of the isf args
                    args.isf_args.append("simulation_plan")
                    args.isf_args.append(new_plan_filename)

    if plan_filename is None:
        # Couldn't find it
        raise IOError("Cannot determine simulation_plan filename.")

    qualified_plan_filename = find_file(plan_filename, path=args.workdir)

    # Set up the args we'll be replacing

    args_to_set = {}

    arg_i = 0
    while arg_i < len(args.plan_args):
        args_to_set[args.plan_args[arg_i]] = args.plan_args[arg_i + 1]
        arg_i += 2

    # Read in the plan table
    simulation_plan_table = None
    try:
        simulation_plan_table = Table.read(qualified_plan_filename, format="fits")
    except Exception as _e2:
        # Not a known table format, maybe an ascii table?
        try:
            simulation_plan_table = Table.read(qualified_plan_filename, format="ascii")
        except IOError as _e3:
            pass
    # If it's still none, we couldn't identify it, so raise the initial exception
    if simulation_plan_table is None:
        raise TypeError("Unknown file format for simulation plan table in " + qualified_plan_filename)

    # Overwrite values as necessary
    for key in args_to_set:
        simulation_plan_table[key] = args_to_set[key]

    # Write out the new plan
    simulation_plan_table.write(qualified_new_plan_filename, format="fits")
    
    if retTable:
        return simulation_plan_table,qualified_new_plan_filename
    else:
        return

def create_config(args):
    """Function to create a new pipeline_config file for this run.
    """

    logger = getLogger(__name__)

    # Find the base config we'll be creating a modified copy of
    base_config = find_file(args.config, path=args.workdir)
    new_config_filename = get_allowed_filename("PIPELINE-CFG", str(os.getpid()), extension=".txt", release="00.03")
    qualified_config_filename = os.path.join(args.workdir, new_config_filename)

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


def create_isf(args,
               config_filename,
               pickled_args_filename):
    """Function to create a new ISF for this run by adjusting workdir and logdir, and overwriting any
       values passed at the command-line.
    """

    logger = getLogger(__name__)

    # Find the base ISF we'll be creating a modified copy of
    base_isf = find_file(args.isf, path=args.workdir)
    new_isf_filename = get_allowed_filename("ISF", str(os.getpid()), extension=".txt", release="00.03")
    qualified_isf_filename = os.path.join(args.workdir, new_isf_filename)

    # Set up the args we'll be replacing or setting

    args_to_set = {}
    args_to_set["workdir"] = args.workdir
    args_to_set["logdir"] = args.logdir
    args_to_set["pkgRepository"] = get_pipeline_dir()
    args_to_set["pipelineDir"] = os.path.join(get_pipeline_dir(), "SHE_Pipeline_pkgdef")
    if config_filename is not None:
        args_to_set["pipeline_config"] = config_filename
    if pickled_args_filename is not None:
        args_to_set["pickled_args"] = pickled_args_filename

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
    search_path = args_to_set["workdir"] + ":" + os.path.abspath(os.path.curdir) + ":"

    for input_port_name in args_to_set:

        # Skip ISF arguments that don't correspond to input ports
        if input_port_name in non_filename_args:
            continue

        filename = args_to_set[input_port_name]

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
            if not qualified_filename == os.path.join(args.workdir, new_filename):
                os.symlink(qualified_filename, os.path.join(args.workdir, new_filename))
        except FileExistsError as e:
            try:
                os.remove(os.path.join(args.workdir, new_filename))
                try:
                    os.unlink(os.path.join(args.workdir, new_filename))
                except Exception as _:
                    pass
            except Exception as _:
                pass
            if not qualified_filename == os.path.join(args.workdir, new_filename):
                os.symlink(qualified_filename, os.path.join(args.workdir, new_filename))

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
            if os.path.exists(os.path.join(args.workdir, data_filename)):
                os.remove(os.path.join(args.workdir, data_filename))
                try:
                    os.unlink(os.path.join(args.workdir, data_filename))
                except Exception as _:
                    pass
            if not qualified_data_filename == os.path.join(args.workdir, data_filename):
                os.symlink(qualified_data_filename, os.path.join(args.workdir, data_filename))

        # End loop "for data_filename in data_filenames:"

    # End loop "for input_port_name in args_to_set:"

    # Write out the new ISF
    with open(qualified_isf_filename, 'w') as fo:
        # Write out values we want set specifically
        for arg in args_to_set:
            fo.write(arg + "=" + args_to_set[arg] + "\n")

    return qualified_isf_filename


def execute_pipeline(pipeline, isf, serverurl, workdir, wait, max_wait, poll_interval):
    """Sets up and calls a command to execute the pipeline.
    """

    logger = getLogger(__name__)
    
    # If we're waiting, we'll need to store the output in a temporary file
    if wait:
        output_filename = get_allowed_filename("RUN-PIP-OUTPUT", str(os.getpid()), extension=".txt", release="00.03")
        qualified_output_filename = os.path.join(workdir,output_filename)
        output_tail = " > " + qualified_output_filename
    else:
        output_tail = ""

    cmd = ('pipeline_runner.py --pipeline=' + pipeline + '.py --data=' + isf + ' --serverurl="' + serverurl + '"'
           + output_tail)
    logger.info("Calling pipeline with command: '" + cmd + "'")
    sbp.call(cmd, shell=True)
    
    # If we're waiting, print the output for records, then poll for when it's finished
    if wait:
        cmd = "cat " + qualified_output_filename
        sbp.call(cmd, shell=True)
        
        # Get the run ID
        ex_head = "Run submitted to server with id "
        with open(qualified_output_filename,'r') as fo:
            run_id = None
            for line in fo:
                if ex_head in line:
                    run_id = line.replace(ex_head,"").strip()[0:-1]
                    break
            if run_id is None:
                raise RuntimeError("Cannot determine runid from output in " + qualified_output_filename)
            else:
                logger.info("Run id is '" + run_id + "'")
        
        # Periodically poll for the status
        
        time_elapsed = 0
        while time_elapsed < max_wait:
            sleep(poll_interval)
            time_elapsed += poll_interval
            
            cmd = 'curl -H "Accept: application/json" "'+serverurl+'/runs/'+run_id+'/status"'
            logger.debug('Polling with command: ' + cmd)
            status_line=sbp.run(cmd,shell=True,stdout=sbp.PIPE).stdout.decode('utf-8')
            logger.debug("Full status is: " + status_line)
            state = ast.literal_eval(status_line)["executionStatus"]
            if state=="COMPLETED":
                logger.info("Pipeline execution completed.")
                break
            elif state=="ERROR":
                logger.error("Pipeline ended in error")
                break
            else:
                logger.debug("Pipeline in state: " + state)
                
        if time_elapsed >= max_wait:
            logger.error("Pipeline timed out.")

    return

def create_pickled_args(args,
                        controlled_run=False):
    """Function to create pickled args for when calling a meta pipeline.
    """
    
    local_args = deepcopy(args)
    
    # Set up the args we'll want for the local run
    local_args.workdir = args.local_workdir
    local_args.parent_workdir = args.workdir
    local_args.serverurl = args.local_serverurl
    local_args.isf = args.local_isf
    local_args.config = args.local_config
    
    if not controlled_run:
        local_args.wait = not args.no_local_wait
        local_args.pipeline = args.pipeline.replace("meta_","")
        
    
    pickled_args_filename = os.path.join(args.workdir,get_allowed_filename("PICKLED-ARGS", str(os.getpid()),
                                                              extension=".bin", release="00.03"))
    
    write_pickled_product(local_args, pickled_args_filename)
    
    return pickled_args_filename

def run_pipeline_from_args(args):
    """Main executable to run pipelines.
    """

    logger = getLogger(__name__)
    
    # Check for pickled arguments, and override if found
    if args.pickled_args is not None:
        qualified_pickled_args_filename = find_file(args.pickled_args,args.workdir)
        args = read_pickled_product(qualified_pickled_args_filename)

    # Check the arguments
    check_args(args)
    
    # Are we doing a meta run?
    meta_run = args.pipeline[0:4]=="meta"
    controlled_run = args.pipeline[0:10]=="controlled"
    if meta_run or controlled_run:
        config_filename = None
        
        pickled_args_filename = create_pickled_args(args,controlled_run=controlled_run)
    else:

        # If necessary, update the simulation plan
        if len(args.plan_args) > 0:
            create_plan(args)
    
        # Create the pipeline_config for this run
        config_filename = create_config(args)
        
        pickled_args_filename = None

    # Create the ISF for this run
    qualified_isf_filename = create_isf(args, config_filename, pickled_args_filename)

    # Try to call the pipeline
    try:
        execute_pipeline(pipeline=args.pipeline,
                         isf=qualified_isf_filename,
                         serverurl=args.serverurl,
                         workdir=args.workdir,
                         wait=args.wait,
                         max_wait=args.max_wait,
                         poll_interval=args.poll_interval)
    except Exception as e:
        # Cleanup the ISF on non-exit exceptions
        try:
            os.remove(new_isf_filename)
        except Exception as e:
            logger.warn("Failsafe exception block triggered with exception: " + str(e))
        raise

    return
