""" @file meta_bias_measurement.py

    Created 23 August 2018

    Pipeline script for the shear bias measurement pipeline.
"""

__updated__ = "2018-08-23"

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

from SHE_Pipeline_pkgdef.package_definition import she_run_bias_pipeline
from euclidwf.framework.workflow_dsl import pipeline

@pipeline(outputs=('shear_bias_measurements',))
def meta_shear_bias_measurement(pickled_args):

    shear_bias_measurements = she_run_bias_pipeline(pickled_args=pickled_args)

    return shear_bias_measurements


if __name__ == '__main__':
    from euclidwf.framework.graph_builder import build_graph
    from euclidwf.utilities import visualizer
    pydron_graph = build_graph(meta_shear_bias_measurement)
    visualizer.visualize_graph(pydron_graph)
