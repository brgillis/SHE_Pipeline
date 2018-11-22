""" @file RunPipeline.py

    Created 4 July 2018

    Main program for calling one of the pipelines.
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

import argparse

from ElementsKernel.Logging import getLogger
from SHE_PPT.utility import get_arguments_string
from SHE_Pipeline.run_bias_pipeline_parallel import run_pipeline_from_args


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
    parser.add_argument('--isf', type=str,
                        help='Fully-qualified name of input specification file for the pipeline')
    parser.add_argument('--isf_args', type=str, nargs='*',
                        help='Additional arguments to write to the ISF (must be in pairs of port_name file_name)')
    parser.add_argument('--config', type=str,
                        help='Fully-qualified name of pipeline config file for this run')
    parser.add_argument('--config_args', type=str, nargs='*',
                        help='Additional arguments to write to the pipeline_config (must be in pairs of key value)')
    parser.add_argument('--cluster', action='store_true',
                        help='Necessary if running on a cluster, causing the pipeline to be executed by another user.')

    parser.add_argument('--app_workdir', type=str,
                        help="Application work directory. This is the work directory specified in the application " +
                        "configuration file provided to the pipeline server.")
    
    # Input arguments for the bias measurement pipeline
    parser.add_argument('--plan_args', type=str, nargs='*',
                        help='Arguments to write to simulation plan (must be in pairs of key value)')

    parser.add_argument('--number_threads',type=str, default=0,
                        help="Number of threads to use. This might be curtailed if > number available. " +
                        "0 (default) will result in using all but one available cpu.")

    parser.add_argument('--est_shear_only',type=str, default=None,
                        help="Curtail pipeline after shear estimates (1) or do full pipeline (0).")


    parser.add_argument('--workdir', type=str,)
    parser.add_argument('--logdir', type=str,)
    
    # Input arguments for when called by a meta pipeline
    parser.add_argument('--pickled_args', type=str, default=None,
                        help="Pickled file of arguments for this task. If supplied, will override all other arguments.")
    parser.add_argument('--parent_workdir', type=str, default=None,
                        help="Work directory of the parent pipeline.")
    
    # Output arguments for the bias measurement pipeline
    parser.add_argument('--shear_bias_measurements', type=str, default='shear_bias_measurements.xml',
                        help='Desired filename of the final output bias measurements')

    
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

    exec_cmd = get_arguments_string(args, cmd="E-Run SHE_Pipeline 0.5 SHE_Pipeline_RunBiasParallel",
                                    store_true=["profile", "debug", "cluster"])
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
