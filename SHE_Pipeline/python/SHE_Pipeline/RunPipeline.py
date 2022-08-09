""" @file RunPipeline.py

    Created 4 July 2018

    Main program for calling one of the pipelines.
"""

__updated__ = "2020-07-30"

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

import argparse

import SHE_Pipeline
from EL_PythonUtils.utilities import get_arguments_string
from ElementsKernel.Logging import getLogger
from SHE_Pipeline.run_pipeline import run_pipeline_from_args


def defineSpecificProgramOptions():
    """
    @brief
        Defines options for this program.

    @return
        An ArgumentParser.
    """

    logger = getLogger(__name__)

    logger.debug('#')
    logger.debug('# Entering SHE_Pipeline_Run defineSpecificProgramOptions()')
    logger.debug('#')

    parser = argparse.ArgumentParser()

    # Input arguments
    parser.add_argument('--pipeline', type=str,
                        help='Name of the pipeline (e.g. "sensitivity_testing")')
    parser.add_argument('--isf', type=str,
                        help='Fully-qualified name of input specification file for the pipeline')
    parser.add_argument('--isf_args', type=str, nargs='*',
                        help='Additional arguments to write to the ISF (must be in pairs of port_name file_name)')
    parser.add_argument('--config', type=str,
                        help='Fully-qualified name of pipeline config file for this run')
    parser.add_argument('--config_args', type=str, nargs='*',
                        help='Additional arguments to write to the pipeline_config (must be in pairs of key value)')
    parser.add_argument('--serverurl', type=str, default=None)
    parser.add_argument('--server_config', type=str,
                        default=None,
                        help="The name of the server configuration file to use.")
    parser.add_argument('--use_debug_server_config', action="store_true",
                        help="If set, will use a server configuration file which outputs all logs to stdout, " +
                             "overriding any provided to the --server_config option.")
    parser.add_argument('--cluster', action='store_true',
                        help='Necessary if running on a cluster, causing the pipeline to be executed by another '
                             'user.')
    parser.add_argument('--dry_run', action='store_true',
                        help="If set, will do everything except actually call the pipeline - useful for testing.")
    parser.add_argument('--skip_file_setup', action='store_true',
                        help="If set, will not try to sort out issues with file locations " +
                             "or move AUX files to the work directory.")

    # Input arguments for the bias measurement pipeline
    parser.add_argument('--plan_args', type=str, nargs='*',
                        help='Arguments to write to simulation plan (must be in pairs of key value)')

    parser.add_argument('--workdir', type=str, )
    parser.add_argument('--logdir', type=str, )

    logger.debug('# Exiting SHE_Pipeline_Run defineSpecificProgramOptions()')

    return parser


def mainMethod(args):
    """
    @brief
        The "main" method for this program, execute a pipeline.

    @details
        This method is the entry point to the program. In this sense, it is
        similar to a main (and it is why it is called mainMethod()).
    """

    logger = getLogger(__name__)

    logger.debug('#')
    logger.debug('# Entering SHE_Pipeline_Run mainMethod()')
    logger.debug('#')

    exec_cmd = get_arguments_string(
        args,
        cmd="E-Run SHE_IAL_Pipelines " + SHE_Pipeline.__version__ + " SHE_Pipeline_Run",
        store_true=["profile", "debug", "cluster", "use_debug_server_config", "dry_run", "skip_file_setup"],
        )
    logger.info('Execution command for this step:')
    logger.info(exec_cmd)

    run_pipeline_from_args(args)

    logger.debug('# Exiting SHE_Pipeline_Run mainMethod()')

    return


def main():
    """
    @brief
        Alternate entry point for non-Elements execution.
    """

    parser = defineSpecificProgramOptions()

    args = parser.parse_args()

    mainMethod(args)

    return


if __name__ == "__main__":
    main()
