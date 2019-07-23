""" @file rm_r.py

    Created 15 July 2019

    Main program for configuring the pipeline server.
"""

__updated__ = "2019-07-15"

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
import os


def recursive_remove(dir_or_file):

    if os.path.isfile(dir_or_file):
        try:
            os.remove(dir_or_file)
        except Exception as _:
            raise ValueError("File " + dir_or_file + " cannot be removed.")
    else:
        # It's a directory, so remove everything inside it first, then remove it

        rootdir = dir_or_file

        all_succeeded = True

        for subdir_or_file in os.listdir(rootdir):
            qualified_subdir_or_file = os.path.join(rootdir, subdir_or_file)
            try:
                recursive_remove(qualified_subdir_or_file)
            except ValueError as _:
                all_succeeded = False

        # Raise an exception if we couldn't remove everything
        if not all_succeeded:
            raise ValueError("Could not remove everything in directory " + str(rootdir))
        else:
            os.rmdir(rootdir)

    return


def main():
    """
    @brief
        Alternate entry point for non-Elements execution.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('root_dir', type=str, default=None, help='Root directory to remove')

    args = parser.parse_args()

    if args.root_dir is None:
        raise ValueError("root_dir to remove must be supplied at command-line, e.g. 'python3 rm_r.py /path/to/dir'")

    if not os.path.exists(args.root_dir):
        raise ValueError(args.root_dir + " does not exist.")

    recursive_remove(args.root_dir)

    return


if __name__ == "__main__":
    main()
