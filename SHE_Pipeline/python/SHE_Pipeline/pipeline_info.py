""" @file pipeline_info.py

    Created 17 July 2020

    Info about each pipeline's source code locations.
"""

__updated__ = "2021-07-20"

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
from SHE_PPT.pipeline_utility import (AnalysisConfigKeys, CalibrationConfigKeys,
                                      ReconciliationConfigKeys, AnalysisValidationConfigKeys)

common_auxdir = "SHE_Pipeline"

package_def_head = "PkgDef_SHE_"
pipeline_script_head = "PipScript_SHE_"
pipeline_def_head = "PipDef_SHE_"
auxdir_head = "SHE_"

config_tail = "_config.txt"
isf_tail = "_isf.txt"


class PipelineInfo(object):

    def __init__(self, lowercase_name, uppercase_name, config_keys, auxdir=None, package_def=None, optional_ports=None):

        self.lowercase_name = lowercase_name
        self.uppercase_name = uppercase_name
        self.config_keys = config_keys

        if auxdir is not None:
            self.auxdir = auxdir_head + auxdir
        else:
            self.auxdir = auxdir_head + uppercase_name

        if package_def is not None:
            self.package_def = package_def_head + package_def + ".py"
        else:
            self.package_def = package_def_head + uppercase_name + ".py"

        if optional_ports is not None:
            self.optional_ports = optional_ports
        else:
            self.optional_ports = ()

        self.pipeline_script = pipeline_script_head + uppercase_name + ".py"
        self.pipeline_def = pipeline_def_head + uppercase_name + ".py"

        self.config = lowercase_name + config_tail
        self.isf = lowercase_name + isf_tail

        self._qualified_pipeline_script = None
        self._qualified_package_def = None
        self._qualified_config = None
        self._qualified_isf = None

    @property
    def qualified_pipeline_script(self):

        if self._qualified_pipeline_script is None:
            self._qualified_pipeline_script = find_aux_file(os.path.join(self.auxdir, self.pipeline_script))

        return self._qualified_pipeline_script

    @property
    def qualified_package_def(self):

        if self._qualified_package_def is None:
            self._qualified_package_def = find_aux_file(os.path.join(self.auxdir, self.package_def))

        return self._qualified_package_def

    @property
    def qualified_config(self):

        if self._qualified_config is None:
            self._qualified_config = find_aux_file(os.path.join(common_auxdir, self.config))

        return self._qualified_config

    @property
    def qualified_isf(self):

        if self._qualified_isf is None:
            self._qualified_isf = find_aux_file(os.path.join(common_auxdir, self.isf))

        return self._qualified_isf


# Set up a dict of pipeline info
pipeline_info_dict = {}

# Shear Analysis pipelines

analysis_optional_ports = ("phz_output_cat",
                           "spe_output_cat",
                           "momentsml_training_data",
                           "pipeline_config")

pipeline_info_dict["analysis"] = PipelineInfo(lowercase_name="analysis",
                                              uppercase_name="Shear_Analysis",
                                              config_keys=AnalysisConfigKeys,
                                              optional_ports=analysis_optional_ports)

pipeline_info_dict["analysis_after_remap"] = PipelineInfo(lowercase_name="analysis_after_remap",
                                                          uppercase_name="Shear_Analysis_After_Remap",
                                                          config_keys=AnalysisConfigKeys,
                                                          auxdir="Shear_Analysis",
                                                          package_def="Shear_Analysis",
                                                          optional_ports=analysis_optional_ports)

pipeline_info_dict["analysis_with_tu_match"] = PipelineInfo(lowercase_name="analysis_with_tu_match",
                                                            uppercase_name="Shear_Analysis_With_TU_Match",
                                                            config_keys=AnalysisConfigKeys,
                                                            auxdir="Shear_Analysis",
                                                            package_def="Shear_Analysis",
                                                            optional_ports=analysis_optional_ports)

pipeline_info_dict["analysis_after_remap_with_tu_match"] = PipelineInfo(lowercase_name="analysis_after_remap_with_tu_match",
                                                                        uppercase_name="Shear_Analysis_After_Remap_With_TU_Match",
                                                                        config_keys=AnalysisConfigKeys,
                                                                        auxdir="Shear_Analysis",
                                                                        package_def="Shear_Analysis",
                                                                        optional_ports=analysis_optional_ports)

# Shear Calibration pipelines

pipeline_info_dict["calibration"] = PipelineInfo(lowercase_name="calibration",
                                                 uppercase_name="Shear_Calibration",
                                                 config_keys=CalibrationConfigKeys)

pipeline_info_dict["bias_measurement"] = PipelineInfo(lowercase_name="bias_measurement",
                                                      uppercase_name="Bias_Measurement",
                                                      config_keys=CalibrationConfigKeys,
                                                      auxdir="Shear_Calibration",
                                                      package_def="Shear_Calibration")

# Shear Reconciliation pipeline

pipeline_info_dict["reconciliation"] = PipelineInfo(lowercase_name="reconciliation",
                                                    uppercase_name="Shear_Reconciliation",
                                                    config_keys=ReconciliationConfigKeys)

# Validation pipelines

pipeline_info_dict["global_validation"] = PipelineInfo(lowercase_name="global_validation",
                                                       uppercase_name="Global_Validation",
                                                       config_keys=AnalysisValidationConfigKeys)
