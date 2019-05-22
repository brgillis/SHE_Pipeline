""" @file ConfigurePipeline.py

    Created 5 July 2018

    Main program for configuring the pipeline server.
"""

__updated__ = "2019-04-26"

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

from SHE_PPT.file_io import find_file
from SHE_PPT.utility import get_arguments_string

from ElementsKernel.Logging import getLogger
import SHE_Pipeline
import subprocess as sbp


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
    parser.add_argument('--config', type=str, default='AUX/SHE_Pipeline/euclid_prs_app_lodeen.cfg',
                        help='Name of the configuration file to use')
    parser.add_argument('--serverurl', type=str, default="http://localhost:50000")
    parser.add_argument('--start', action='store_true',
                        help='If set, will start the pipeline run server before configuring')
    parser.add_argument('--restart', action='store_true',
                        help='If set, will restart the pipeline run server before configuring')
    parser.add_argument('--password', type=str, default='password',
                        help='Root user password, needed if starting/restarting the pipeline server.')

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

    exec_cmd = get_arguments_string(args, cmd="E-Run SHE_Pipeline " + SHE_Pipeline.__version__ + " SHE_Pipeline_Configure",
                                    store_true=["profile", "debug", "start", "restart"])
    logger.info('Execution command for this step:')
    logger.info(exec_cmd)

    # Start/restart if desired
    if args.start or args.restart:
        if args.restart:
            scmd = "restart"
        else:
            scmd = "start"
        cmd = "echo " + args.password + " | sudo -S systemctl " + scmd + " euclid-ial-wfm && sleep 1"
        logger.info("Starting/restarting pipeline with cmd:")
        logger.info(cmd)
        sbp.call(cmd, shell=True)

    # Find the config file
    config_file = find_file(args.config, path=".")

    # Configure the server now
    cmd = 'pipeline_runner.py --configure --config=' + config_file + ' --serverurl="' + args.serverurl + '"'
    logger.info("Configuring pipeline with cmd:")
    logger.info(cmd)
    sbp.call(cmd, shell=True)

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
