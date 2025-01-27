""" @file run_pipeline_test.py

    Created 23 July 2020

    Tests of running the pipelines.
"""

__updated__ = "2021-08-18"

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
# the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
# Boston, MA 02110-1301 USA

import os

import pytest

from ElementsServices.DataSync import DataSync
from SHE_PPT.constants.test_data import MDB_PRODUCT_FILENAME, SYNC_CONF, SYNC_LOCATION
from SHE_Pipeline.pipeline_info import pipeline_info_dict
from SHE_Pipeline.run_pipeline import run_pipeline_from_args

test_data_location = "/tmp"

LOCAL_TEST_DATA_PATH = "SHE_Pipeline_8_2/test_workdir"


class MockArgs(object):

    def __init__(self,
                 pipeline,
                 workdir,
                 logdir,
                 isf=None,
                 isf_args=None,
                 config=None,
                 config_args=None,
                 serverurl=None,
                 server_config=None,
                 use_debug_server_config=False,
                 cluster=False,
                 dry_run=False,
                 skip_file_setup=False,
                 plan_args=None,
                 ):

        self.pipeline = pipeline
        self.isf = isf
        self.config = config
        self.workdir = workdir
        self.logdir = logdir
        self.serverurl = serverurl
        self.server_config = server_config
        self.use_debug_server_config = use_debug_server_config
        self.cluster = cluster
        self.dry_run = dry_run
        self.skip_file_setup = skip_file_setup

        if isf_args is None:
            self.isf_args = []
        else:
            self.isf_args = isf_args
        if config_args is None:
            self.config_args = []
        else:
            self.config_args = config_args
        if plan_args is None:
            self.plan_args = []
        else:
            self.plan_args = plan_args


class TestRunPipeline():

    @pytest.fixture(autouse=True)
    def setup(self):

        # Download the mock workdir files from WebDAV

        sync = DataSync(SYNC_CONF, f"{SYNC_LOCATION}/test_workdir.txt")
        sync.download()
        qualified_data_images_filename = sync.absolutePath(f"{LOCAL_TEST_DATA_PATH}/{MDB_PRODUCT_FILENAME}")

        assert os.path.isfile(qualified_data_images_filename), f"Cannot find file: {qualified_data_images_filename}"

        # Get the workdir based on where the data images listfile is
        self.workdir = os.path.split(qualified_data_images_filename)[0]
        self.logdir = os.path.join(self.workdir, "logs")

    @pytest.mark.skip(reason="DataModel 9 compatible test data is not yet available")
    def test_dry_run_pipelines(self):
        """ Test that all versions are set up correctly
        """

        for pipeline in pipeline_info_dict:

            if pipeline == "analysis":
                skip_file_setup = False
            else:
                skip_file_setup = True

            test_args = MockArgs(pipeline=pipeline,
                                 workdir=self.workdir,
                                 logdir=self.logdir,
                                 dry_run=True,
                                 skip_file_setup=skip_file_setup)

            run_pipeline_from_args(test_args)
