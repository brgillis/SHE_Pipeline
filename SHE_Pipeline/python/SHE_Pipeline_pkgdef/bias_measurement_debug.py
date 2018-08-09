""" @file bias_measurement_debug.py

    Created 13 July 2018

    Pipeline script for the shear bias measurement pipeline, without cleanup step.
"""

__updated__ = "2018-08-09"

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

from SHE_Pipeline_pkgdef.package_definition import (she_prepare_configs, she_simulate_images, she_estimate_shear,
                                                    she_measure_statistics, she_measure_bias)
from euclidwf.framework.workflow_dsl import pipeline, parallel


# Define a body element for the split part of the calibration pipeline
@parallel(iterable="simulation_config")
def she_simulate_and_measure_bias_statistics(simulation_config,
                                             bfd_training_data,
                                             ksb_training_data,
                                             lensmc_training_data,
                                             momentsml_training_data,
                                             regauss_training_data):

    (data_images,
     stacked_data_image,
     psf_images_and_tables,
     segmentation_images,
     stacked_segmentation_image,
     detections_tables,
     details_table) = she_simulate_images(config_files=simulation_config)

    shear_estimates = she_estimate_shear(data_images=data_images,
                                         stacked_image=stacked_data_image,
                                         psf_images_and_tables=psf_images_and_tables,
                                         segmentation_images=segmentation_images,
                                         stacked_segmentation_image=stacked_segmentation_image,
                                         detections_tables=detections_tables,
                                         bfd_training_data=bfd_training_data,
                                         ksb_training_data=ksb_training_data,
                                         lensmc_training_data=lensmc_training_data,
                                         momentsml_training_data=momentsml_training_data,
                                         regauss_training_data=regauss_training_data)

    shear_bias_statistics = she_measure_statistics(details_table=details_table,
                                                   shear_estimates=shear_estimates)

    return shear_bias_statistics


@pipeline(outputs=('shear_bias_measurements',))
def shear_bias_measurement(simulation_plan,
                           config_template,
                           bfd_training_data,
                           ksb_training_data,
                           lensmc_training_data,
                           momentsml_training_data,
                           regauss_training_data):

    simulation_configs = she_prepare_configs(simulation_plan=simulation_plan,
                                             config_template=config_template)

    shear_bias_statistics = she_simulate_and_measure_bias_statistics(
        simulation_config=simulation_configs,
        bfd_training_data=bfd_training_data,
        ksb_training_data=ksb_training_data,
        lensmc_training_data=lensmc_training_data,
        momentsml_training_data=momentsml_training_data,
        regauss_training_data=regauss_training_data)

    shear_bias_measurements = she_measure_bias(shear_bias_statistics=shear_bias_statistics)

    return shear_bias_measurements


if __name__ == '__main__':
    from euclidwf.framework.graph_builder import build_graph
    from euclidwf.utilities import visualizer
    pydron_graph = build_graph(shear_bias_measurement)
    visualizer.visualize_graph(pydron_graph)
