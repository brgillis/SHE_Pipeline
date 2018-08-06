""" @file run_pipeline.py

    Created 4 July 2018

    Main executable for running pipelines.
"""

__updated__ = "2018-08-02"

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

from SHE_PPT import products
from SHE_PPT.file_io import find_file, find_aux_file, get_allowed_filename, read_xml_product
from SHE_PPT.logging import getLogger
import subprocess as sbp


default_workdir = "/home/user/Work/workspace"
default_logdir = "logs"
default_cluster_workdir = "/workspace/lodeen/workdir"

non_filename_args = ("workdir", "logdir", "pkgRepository", "pipelineDir")


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

    # Check that we have an even number of ISF arguments
    if args.args is None:
        args.args = []
    if not len(args.args) % 2 == 0:
        raise ValueError("Invalid values passed to 'args': Must be a set of paired arguments.")

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

    return


def create_isf(args):
    """Function to create a new ISF for this run by adjusting workdir and logdir.
    """

    logger = getLogger(__name__)

    # Find the base ISF we'll be creating a modified copy of
    base_isf = find_file(args.isf, path=".")
    new_isf_filename = get_allowed_filename("ISF", str(os.getpid()), extension=".txt", release="00.03")
    qualified_isf_filename = os.path.join(args.workdir, new_isf_filename)

    # Set up the args we'll be replacing or setting

    args_to_set = {}
    args_to_set["workdir"] = args.workdir
    args_to_set["logdir"] = args.logdir
    args_to_set["pkgRepository"] = get_pipeline_dir()
    args_to_set["pipelineDir"] = os.path.join(get_pipeline_dir(), "SHE_Pipeline_pkgdef")

    arg_i = 0
    while arg_i < len(args.args):
        args_to_set[args.args[arg_i]] = args.args[arg_i + 1]
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
    search_path = args_to_set["workdir"] + "::.:"

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
        if os.path.exists(os.path.join(args.workdir, new_filename)):
            os.remove(os.path.join(args.workdir, new_filename))
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
            os.symlink(qualified_data_filename, os.path.join(args.workdir, data_filename))

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

    sbp.call(cmd, shell=True)

    return


def run_pipeline_from_args(args):
    """Main executable to run pipelines.
    """

    logger = getLogger(__name__)

    # Check the arguments
    check_args(args)

    # Create the ISF for this run
    qualified_isf_filename = create_isf(args)

    # Try to call the pipeline
    try:
        execute_pipeline(pipeline=args.pipeline,
                         isf=qualified_isf_filename,
                         serverurl=args.serverurl)
    except Exception as e:
        # Cleanup the ISF on non-exit exceptions
        try:
            os.remove(new_isf_filename)
        except Exception:
            pass
        raise e

    return
