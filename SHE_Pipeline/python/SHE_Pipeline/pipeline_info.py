""" @file pipeline_info.py

    Created 17 July 2020

    Info about each pipeline's source code locations.
"""
from builtins import None

__updated__ = "2020-07-22"

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
# the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA""" @file RunPipeline.py

import os

from SHE_PPT.file_io import find_aux_file

common_auxdir = "SHE_Pipeline"

package_def_head = "PkgDef_SHE_"
pipeline_script_head = "PipScript_SHE_"
pipeline_def_head = "PipDef_SHE_"
auxdir_head = "SHE_"

config_tail = "_config.txt"
isf_tail = "_isf.txt"


class PipelineInfo(object):

    def __init__(self, lowercase_name, uppercase_name, auxdir=None, package_def=None):

        self.lowercase_name = lowercase_name
        self.uppercase_name = uppercase_name

        if auxdir is not None:
            self.auxdir = auxdir_head + auxdir
        else:
            self.package_def = auxdir_head + uppercase_name

        if package_def is not None:
            self.package_def = package_def_head + package_def + ".py"
        else:
            self.package_def = package_def_head + uppercase_name + ".py"

        self.pipeline_script = pipeline_script_head + uppercase_name + ".py"
        self.pipeline_def = pipeline_def_head + uppercase_name + ".py"

        self.config = lowercase_name + config_tail
        self.isf = lowercase_name + isf_tail

        self._qualified_pipeline_script = None
        self._qualified_package_def = None
        self._qualified_config = None
        self._qualified_isf = None

        return

    @property
    def qualified_pipeline_script(self):

        if self._qualified_pipeline_script is None:
            self._qualified_pipeline_script = find_aux_file(os.path.join(self.auxdir, self.pipeline_script))

        return _self.qualified_pipeline_script

    @property
    def qualified_package_def(self):

        if self._qualified_package_def is None:
            self._qualified_package_def = find_aux_file(os.path.join(self.auxdir, self.package_def))

        return _self.qualified_package_def

    @property
    def qualified_config(self):

        if self._qualified_config is None:
            self._qualified_config = find_aux_file(os.path.join(common_auxdir, self.config))

        return _self.qualified_config

    @property
    def qualified_isf(self):

        if self._qualified_isf is None:
            self._qualified_isf = find_aux_file(os.path.join(common_auxdir, self.isf))

        return _self.qualified_isf


# Set up a dict of pipeline info
pipeline_info = {}

# Shear Analysis pipelines

pipeline_info["analysis"] = PipelineInfo(lowercase_name="analysis",
                                         uppercase_name="Shear_Analysis")

pipeline_info["analysis_after_remap"] = PipelineInfo(lowercase_name="analysis_after_remap",
                                                     uppercase_name="Shear_Analysis_After_Remap",
                                                     auxdir="Shear_Analysis",
                                                     package_def="Shear_Analysis")

pipeline_info["analysiswith_tu_match"] = PipelineInfo(lowercase_name="analysis_with_tu_match",
                                                      uppercase_name="Shear_Analysis_With_TU_Match",
                                                      auxdir="Shear_Analysis",
                                                      package_def="Shear_Analysis")

pipeline_info["analysis_after_remap_with_tu_match"] = PipelineInfo(lowercase_name="analysis_after_remap_with_tu_match",
                                                                   uppercase_name="Shear_Analysis_After_Remap_With_TU_Match",
                                                                   auxdir="Shear_Analysis",
                                                                   package_def="Shear_Analysis")

# Shear Calibration pipelines

pipeline_info["calibration"] = PipelineInfo(lowercase_name="calibration",
                                         uppercase_name="Shear_Calibration")

pipeline_info["bias_measurement"] = PipelineInfo(lowercase_name="bias_measurement",
                                                 uppercase_name="Bias_Measurement",
                                                 auxdir="Shear_Calibration",
                                                 package_def="Shear_Calibration")

# Shear Reconciliation pipeline

pipeline_info["reconciliation"] = PipelineInfo(lowercase_name="reconciliation",
                                         uppercase_name="Shear_Reconciliation")
