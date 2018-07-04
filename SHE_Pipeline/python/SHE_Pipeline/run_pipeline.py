""" @file run_pipeline.py

    Created 4 July 2018

    Main executable for running pipelines.
"""

__updated__ = "2018-07-04"

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

from SHE_PPT.file_io import find_file, get_allowed_filename
from SHE_PPT.logging import getLogger
import subprocess as sbp


default_workdir = "/home/user/Work/workspace"
default_logdir = "logs"


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

    # Use the default workdir if necessary
    if args.workdir is None:
        logger.info('No workdir supplied at command-line. Using default workdir: ' + default_workdir)
        args.workdir = default_workdir

    # Does the workdir exist?
    if not os.path.exists(args.workdir):
        # Can we create it?
        try:
            os.mkdir(args.workdir)
            # Also create a cache directory in it in case we need that too
            os.mkdir(os.path.join(args.workdir, "cache"))
        except Exception as e:
            logger.error("Workdir (" + args.workdir + ") does not exist and cannot be created.")
            raise e

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

    # Does the pipeline we want to run exist?
    pipeline_filename = os.path.join(get_pipeline_dir(), "SHE_Pipeline_pkgdef/" + args.pipeline + ".py")
    if not os.path.exists(pipeline_filename):
        logger.error("Pipeline '" + pipeline_filename + "' cannot be found. Expected location: " +
                     pipeline_filename)

    return


def create_isf(args):
    """Function to create a new ISF for this run by adjusting workdir and logdir.
    """

    logger = getLogger(__name__)

    base_isf = find_file(args.isf, path=".")
    new_isf_filename = get_allowed_filename("ISF", str(os.getpid()), extension=".txt", release="00.03")
    qualified_isf_filename = os.path.join(args.workdir, new_isf_filename)

    with open(base_isf, 'r') as fi:
        with open(qualified_isf_filename, 'w') as fo:
            # Check each line to see if values we'll overwrite are specified in it,
            # and only write out lines with other values
            for line in fi:
                if not (line[0:7] == "workdir" or
                        line[0:6] == "logdir" or
                        line[0:13] == "pkgRepository" or
                        line[0:11] == "pipelineDir"):
                    fo.write(line)
            # Write out new workdir and logdir at the end
            fo.write("workdir=" + args.workdir)
            fo.write("logdir=" + args.logdir)
            fo.write("pkgRepository=" + get_pipeline_dir())
            fo.write("pipelineDir=" + os.path.join(get_pipeline_dir(), "SHE_Pipeline_pkgdef/" + args.pipeline + ".py"))

    return qualified_isf_filename


def execute_pipeline(pipeline, isf, serverurl):
    """Sets up and calls a command to execute the pipeline.
    """

    logger = getLogger(__name__)

    cmd = ('pipeline_runner.py --pipeline=' + pipeline + '.py --data=' + isf + ' --serverurl="' + serverurl + '"')
    logger.info("Calling python with command: '" + cmd + "'")

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

    try:
        execute_pipeline(pipeline=args.pipeline,
                         isf=qualified_isf_filename,
                         serverurl=args.serverurl)
    except Exception as e:
        # Cleanup the ISF on non-exit exceptions
        try:
            # os.remove(new_isf_filename)
            pass
        except Exception:
            pass
        raise e

    return
