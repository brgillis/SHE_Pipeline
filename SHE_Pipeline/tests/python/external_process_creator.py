""" @file external_process_creator.py

    Created Aug 2018

    Functions that are run as external_processes to create stdOut/stdErr
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

from collections import namedtuple
import math
import multiprocessing
import os
import time

import argparse

from ElementsKernel.Logging import getLogger

def test_funct(args):
    """ test 
    """
    
    stdOutOpt=args.standout 
    if stdOutOpt=='stdout':
        print("Testing stdout...")
    elif stdOutOpt=='stderr':
        raise Exception("Testing stderr..")

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
    
    # Input arguments for when called by a meta pipeline
    parser.add_argument('--standout', type=str, default=None,
                        help="Parsable string giving standard output/error option")

    
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


    test_funct(args)

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

