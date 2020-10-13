""" @file run_pipeline.py

    Created 4 July 2018

    Main executable for running pipelines.
"""

__updated__ = "2020-10-13"

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

import _pickle
from copy import deepcopy
import os
from shutil import copyfile
from time import sleep
import xml.sax._exceptions

from SHE_PPT import products
from SHE_PPT.file_io import (find_file, find_aux_file, get_allowed_filename, read_xml_product,
                             read_pickled_product, write_pickled_product, read_listfile, write_listfile)
from SHE_PPT.logging import getLogger
from SHE_PPT.pipeline_utility import write_config, read_config
from astropy.table import Table
from sklearn import pipeline

import SHE_Pipeline
from SHE_Pipeline.pipeline_info import pipeline_info_dict
import subprocess as sbp

default_workdir = "/home/" + os.environ['USER'] + "/Work/workspace"
default_logdir = "logs"
default_cluster_workdir = "/workspace/lodeen/workdir"
default_server_config = "/cvmfs/euclid-dev.in2p3.fr/CentOS7/INFRA/CONFIG/GENERIC/latest/ppo/lodeen-ial.properties"

default_eden_version_master = "Eden-2.1"
default_eden_version_dev = "Eden-2.1-dev"

non_filename_args = ("workdir", "logdir", "pkgRepository", "pipelineDir", "pipeline_config", "edenVersion")

known_output_filenames = {"bias_measurement": "she_measure_bias/she_bias_measurements.xml"}

pipeline_runner_exec = "/cvmfs/euclid-dev.in2p3.fr/CentOS7/INFRA/1.1/opt/euclid/ST_PipelineRunner/2.1.4/bin/pipeline_runner.py"

logger = getLogger(__name__)


def is_dev_version():
    """Determines if we're running a develop version of the code.
    """

    n_digits = len(SHE_Pipeline.__version__.split("."))

    if n_digits == 2:
        return True
    elif n_digits == 3:
        return False
    else:
        raise RuntimeError("Cannot determine if version (" +
                           SHE_Pipeline.__version__ + ") is develop or master version.")


def get_pipeline_dir():
    """Gets the directory containing the pipeline packages, using the location of this module.
    """

    this_file_name = __name__.replace(".", "/") + '.py'
    pipeline_dir = __file__.replace('/' + this_file_name, '')

    return pipeline_dir


def check_args(args):
    """Checks arguments for validity and fixes if possible.
    """

    logger.debug('# Entering SHE_Pipeline_Run check_args()')

    # Is a pipeline specified at all?
    if args.pipeline is None:
        raise IOError("Pipeline must be specified at command-line, e.g with --pipeline bias_measurement")
    elif not args.pipeline in pipeline_info_dict:
        err_string = ("Unknown pipeline specified to be run: " + args.pipeline + ". Allowed pipelines are: ")
        for allowed_pipeline in pipeline_info_dict:
            err_string += "\n  " + allowed_pipeline
        raise ValueError(err_string)

    chosen_pipeline_info = pipeline_info_dict[args.pipeline]

    # Does the pipeline we want to run exist?
    if not os.path.exists(chosen_pipeline_info.qualified_pipeline_script):
        logger.error("Pipeline '" + pipeline_filename + "' cannot be found. Expected location: " +
                     chosen_pipeline_info.qualified_pipeline_script)

    # If no ISF is specified, use the default for this pipeline
    if args.isf is None:
        try:
            args.isf = chosen_pipeline_info.qualified_isf
        except Exception:
            logger.error("No ISF file specified, and cannot find one in default location (" +
                         chosen_pipeline_info.qualified_isf + "_isf.txt).")
            raise

    # If no config is specified, use the default for this pipeline
    if args.config is None:
        try:
            args.config = chosen_pipeline_info.qualified_config
        except Exception:
            logger.warning("No config file specified, and cannot find one in default location (" +
                           chosen_pipeline_info.qualified_config + "). Will run with no " +
                           "configuration parameters set.")

    # Check that we have an even number of ISF arguments
    if args.isf_args is None:
        args.isf_args = []
    if not len(args.isf_args) % 2 == 0:
        raise ValueError("Invalid values passed to 'isf_args': Must be a set of paired arguments.")

    # Check that we have an even number of pipeline_config arguments
    if args.config_args is None:
        args.config_args = []
    if not len(args.config_args) % 2 == 0:
        raise ValueError("Invalid values passed to 'config_args': Must be a set of paired arguments.")

    config_keys = chosen_pipeline_info.config_keys

    # Check that all config args are recognized
    for i in range(len(args.config_args) // 2):
        test_arg = args.config_args[2 * i]
        if not config_keys.is_allowed_value(test_arg):
            err_string = ("Config argument \"" + test_arg + "\" not recognized. Allowed arguments are: ")
            for allowed_key in config_keys:
                err_string += "\n  " + allowed_key.value
            raise ValueError(err_string)

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

    # Does the workdir exist?
    if not os.path.exists(args.workdir):
        # Can we create it?
        try:
            os.mkdir(args.workdir)
        except Exception as e:
            logger.error("Workdir (" + args.workdir + ") does not exist and cannot be created.")
            raise e
    if args.cluster:
        os.chmod(args.workdir, 0o777)

    # Does the cache directory exist within the workdir?
    cache_dir = os.path.join(args.workdir, "cache")
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
    data_dir = os.path.join(args.workdir, "data")
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
    qualified_logdir = os.path.join(args.workdir, args.logdir)
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
    if len(args.plan_args) > 0 and not args.pipeline == "bias_measurement":
        raise ValueError("plan_args can only be provided for the Bias Measurement pipeline.")
    if not len(args.plan_args) % 2 == 0:
        raise ValueError("Invalid values passed to 'plan_args': Must be a set of paired arguments.")

    return chosen_pipeline_info


def create_plan(args, retTable=False):
    """Function to create a new simulation plan for this run.
    """

    # Find the base plan we'll be creating a modified copy of

    new_plan_filename = get_allowed_filename("SIM-PLAN", str(os.getpid()),
                                             extension=".fits", version=SHE_Pipeline.__version__)
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
        return simulation_plan_table, qualified_new_plan_filename
    else:
        return


def create_config(args, config_keys):
    """Function to create a new pipeline_config file for this run.
    """

    # Find and read in the base config we'll be creating a modified copy of
    args_to_set = read_config(args.config, workdir=args.workdir, config_keys=config_keys)

    # Set up the filename for the new config file
    new_config_filename = get_allowed_filename(
        "PIPELINE-CFG", str(os.getpid()), extension=".txt", version=SHE_Pipeline.__version__)

    arg_i = 0
    while arg_i < len(args.config_args):
        args_to_set[args.config_args[arg_i]] = args.config_args[arg_i + 1]
        arg_i += 2

    # Write out the new config
    write_config(config_dict=args_to_set,
                 config_filename=new_config_filename,
                 workdir=args.workdir,
                 config_keys=config_keys)

    return new_config_filename


def create_isf(args,
               config_filename,
               chosen_pipeline_info):
    """Function to create a new ISF for this run by adjusting workdir and logdir, and overwriting any
       values passed at the command-line.
    """

    # Find the base ISF we'll be creating a modified copy of
    base_isf = find_file(args.isf, path=args.workdir)
    new_isf_filename = get_allowed_filename("ISF", str(
        os.getpid()), extension=".txt", version=SHE_Pipeline.__version__)
    qualified_isf_filename = os.path.join(args.workdir, new_isf_filename)

    # Set up the args we'll be replacing or setting

    args_to_set = {}
    args_to_set["workdir"] = args.workdir
    args_to_set["logdir"] = args.logdir

    pipeline_dir = os.path.split(chosen_pipeline_info.qualified_pipeline_script)[0]

    args_to_set["pkgRepository"] = pipeline_dir
    args_to_set["pipelineDir"] = pipeline_dir
    if is_dev_version():
        args_to_set["edenVersion"] = default_eden_version_dev
    else:
        args_to_set["edenVersion"] = default_eden_version_master

    if config_filename is not None:
        args_to_set["pipeline_config"] = config_filename

    arg_i = 0
    while arg_i < len(args.isf_args):
        args_to_set[args.isf_args[arg_i]] = args.isf_args[arg_i + 1]
        arg_i += 2

    with open(base_isf, 'r') as fi:
        # Check each line to see if values we'll overwrite are specified in it,
        # and only write out lines with other values
        for line in fi:
            uncommented_line = line.split('#')[0]
            split_line = uncommented_line.strip().split('=')

            # Check if the line is empty or invalid
            if len(split_line) == 0 or (len(split_line) == 1 and split_line[0] == ""):
                # Empty line
                continue
            elif len(split_line) != 2:
                raise ValueError("Invalid line in ISF: " + line)

            arg = split_line[0].strip()
            value = split_line[1].strip()

            # Add any new args here to the list of args we want to set
            if not (arg in args_to_set):
                args_to_set[arg] = value

    # Check each filename arg to see if it's already in the workdir, or if we have to move it there

    # Create a search path from the workdir, the root directory (using an empty string), and the current
    # directory
    search_path = args_to_set["workdir"] + ":" + os.path.abspath(os.path.curdir) + ":"

    # If a dry run, skip updating arguments
    if not args.skip_file_setup:

        for input_port_name in args_to_set:

            # Skip ISF arguments that don't correspond to input ports
            if input_port_name in non_filename_args or input_port_name == "mdb":
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
                if not os.path.abspath(qualified_filename) == os.path.abspath(os.path.join(args.workdir, new_filename)):
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
                if not os.path.abspath(qualified_filename) == os.path.abspath(os.path.join(args.workdir, new_filename)):
                    os.symlink(qualified_filename, os.path.join(args.workdir, new_filename))

            # Update the filename in the args_to_set to the new location
            args_to_set[input_port_name] = new_filename

            # Now, go through each data file of the product and symlink those from the workdir too

            # Skip (but warn) if it's not an XML data product
            if qualified_filename[-4:] == ".xml":
                try:
                    p = read_xml_product(qualified_filename)
                    data_filenames = p.get_all_filenames()
                except (xml.sax._exceptions.SAXParseException, _pickle.UnpicklingError) as e:
                    logger.error("Cannot read file " + qualified_filename + ".")
                    raise
            elif qualified_filename[-5:] == ".json":
                subfilenames = read_listfile(qualified_filename)
                data_filenames = []
                for subfilename in subfilenames:
                    qualified_subfilename = find_file(subfilename, path=search_path)
                    try:
                        p = read_xml_product(qualified_subfilename)
                        data_filenames += p.get_all_filenames()
                    except (xml.sax._exceptions.SAXParseException, _pickle.UnpicklingError) as e:
                        logger.error("Cannot read file " + qualified_filename + ".")
                        raise
            else:
                logger.warn("Input file " + filename + " is not an XML data product.")
                continue

            if len(data_filenames) == 0:
                continue

            # Set up the search path for data files
            data_search_path = (os.path.split(qualified_filename)[0] + ":" +
                                os.path.split(qualified_filename)[0] + "/..:" +
                                os.path.split(qualified_filename)[0] + "/../data:" + search_path)

            # Search for and symlink each data file
            for data_filename in data_filenames:

                if data_filename is None or data_filename == "None" or data_filename == "data/None":
                    continue

                # Find the qualified location of the data file
                try:
                    qualified_data_filename = find_file(data_filename, path=data_search_path)
                except RuntimeError as e:
                    # Try searching for the file without the "data/" prefix
                    try:
                        qualified_data_filename = find_file(
                            data_filename.replace("data/", "", 1), path=data_search_path)
                    except RuntimeError as e:
                        raise RuntimeError("Data file " + data_filename +
                                           " cannot be found in path " + data_search_path)

                # Symlink the data file within the workdir
                if not os.path.abspath(qualified_data_filename) == os.path.abspath(os.path.join(args.workdir, data_filename)):
                    if os.path.exists(os.path.join(args.workdir, data_filename)):
                        os.remove(os.path.join(args.workdir, data_filename))
                        try:
                            os.unlink(os.path.join(args.workdir, data_filename))
                        except Exception as _:
                            pass
                    os.symlink(qualified_data_filename, os.path.join(args.workdir, data_filename))

            # End loop "for data_filename in data_filenames:"

        # End loop "for input_port_name in args_to_set:"

    # Make sure all optional products are provided by a listfile

    for port_name in chosen_pipeline_info.optional_ports:

        if port_name in args_to_set and not (args_to_set[port_name] is None or
                                             args_to_set[port_name] == "None" or
                                             args_to_set[port_name] == "data/None" or
                                             args_to_set[port_name] == ""):

            # If the port is present, ensure it's provided as a listfile
            if args_to_set[port_name][-5:] == ".json":
                # Already a listfile, so skip
                continue
            else:
                file_list = [args_to_set[port_name]]
        else:

            # If the port isn't present or is a null value, pass an empty listfile
            file_list = []

        # If we get to this branch, we need to create a new listfile and set it as the input to the port
        listfile_name = get_allowed_filename(port_name.upper().replace("_", "-"), str(os.getpid()),
                                             extension=".json", version=SHE_Pipeline.__version__)

        write_listfile(os.path.join(args.workdir, listfile_name), file_list)

        args_to_set[port_name] = listfile_name

    # Write out the new ISF
    with open(qualified_isf_filename, 'w') as fo:
        # Write out values we want set specifically
        for arg in args_to_set:
            fo.write(arg + "=" + args_to_set[arg] + "\n")

    return qualified_isf_filename


def execute_pipeline(pipeline_info, isf, serverurl, workdir, server_config, dry_run=False):
    """Sets up and calls a command to execute the pipeline.
    """

    cmd = pipeline_runner_exec + ' --pipeline=' + pipeline_info.pipeline_script + ' --data=' + isf
    if server_config is not None:
        cmd += ' --config=' + server_config
    if serverurl is not None:
        cmd += ' --serverurl="' + serverurl + '"'

    if dry_run:
        logger.info("If this were not a dry run, the following command would now be called:\n" + cmd)
    else:
        logger.info("Calling pipeline with command: '" + cmd + "'")
        sbp.call(cmd, shell=True)

    return


def run_pipeline_from_args(args):
    """Main executable to run pipelines.
    """

    # Check the arguments
    chosen_pipeline_info = check_args(args)

    # If necessary, update the simulation plan
    if len(args.plan_args) > 0:
        create_plan(args)

    # Create the pipeline_config for this run
    config_filename = create_config(args, config_keys=chosen_pipeline_info.config_keys)

    # Create the ISF for this run
    qualified_isf_filename = create_isf(args, config_filename, chosen_pipeline_info=chosen_pipeline_info)

    if args.use_debug_server_config:
        server_config = find_aux_file("SHE_Pipeline/debug_server_config.txt")
    else:
        server_config = args.server_config
        if server_config is None and not args.cluster:
            server_config = default_server_config

    # Try to call the pipeline
    try:
        execute_pipeline(pipeline_info=chosen_pipeline_info,
                         isf=qualified_isf_filename,
                         serverurl=args.serverurl,
                         workdir=args.workdir,
                         server_config=server_config,
                         dry_run=args.dry_run)
    except Exception as e:
        # Cleanup the ISF on non-exit exceptions
        try:
            os.remove(new_isf_filename)
        except Exception as e:
            logger.warn("Failsafe exception block triggered with exception: " + str(e))
        raise

    return
