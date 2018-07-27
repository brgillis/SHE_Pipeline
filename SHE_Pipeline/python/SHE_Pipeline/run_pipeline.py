""" @file run_pipeline.py

    Created 4 July 2018

    Main executable for running pipelines.
"""

__updated__ = "2018-07-27"

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

from SHE_PPT.file_io import find_file, find_aux_file, get_allowed_filename
from SHE_PPT.logging import getLogger
import subprocess as sbp


default_workdir = "/home/user/Work/workspace"
default_logdir = "logs"
default_cluster_workdir = "/workspace/lodeen/workdir"


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
        logger.info('No workdir supplied at command-line. Using default workdir: ' + default_workdir)
        if args.cluster:
            args.workdir = default_cluster_workdir
        else:
            args.workdir = default_workdir

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

    # Use the default logdir if necessary
    if args.logdir is None:
        args.logdir = default_logdir
        logger.info('No logdir supplied at command-line. Using default logdir: ' + args.logdir)

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
        with open(qualified_isf_filename, 'w') as fo:
            # Check each line to see if values we'll overwrite are specified in it,
            # and only write out lines with other values
            for line in fi:
                if not (line.split('=')[0] in args_to_set):
                    fo.write(line)
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
