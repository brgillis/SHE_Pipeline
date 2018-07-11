""" @file RunPipeline.py

    Created 4 July 2018

    Main program for calling one of the pipelines.
"""

__updated__ = "2018-07-11"

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
    parser.add_argument('--isf', type=str,
                        help='Fully-qualified name of input specification file for the pipeline')
    parser.add_argument('--pipeline', type=str,
                        help='Name of the pipeline (e.g. "sensitivity_testing")')
    parser.add_argument('--serverurl', type=str, default="http://localhost:50000")
    parser.add_argument('--cluster', action='store_true',
                        help='Necessary if running on a cluster, causing the pipeline to be executed by another user.')

    parser.add_argument('--workdir', type=str,)
    parser.add_argument('--logdir', type=str,)

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
    
    exec_cmd = get_arguments_string(args, cmd="E-Run SHE_Pipeline 0.3 SHE_Pipeline_Run")
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
