""" @file calibration_pipeline.py

    Created 6 Nov 2017

    Pipeline script for the shear calibration pipeline.
"""

__updated__ = "2018-06-28"

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

from euclidwf.framework.workflow_dsl import pipeline, parallel
from pkgdef.package_definition import (she_prepare_configs, she_simulate_images, she_estimate_shear,
                                       she_measure_statistics, she_measure_bias, she_compile_statistics)

# Define a body element for the split part of the calibration pipeline


@parallel(iterable="simulation_config")
def she_simulate_and_measure_bias_statistics(simulation_config, galaxy_population_priors_table):

    (data_images,
     psf_images_and_tables,
     segmentation_images,
     detections_tables,
     details_table) = she_simulate_images(simulation_config=simulation_config,
                                          galaxy_population_priors_table=galaxy_population_priors_table)

    shear_estimates_product = she_estimate_shear(data_images=data_images,
                                                 psf_images_and_tables=psf_images_and_tables,
                                                 segmentation_images=segmentation_images,
                                                 detections_tables=detections_tables,
                                                 galaxy_population_priors_table=galaxy_population_priors_table, )

    (estimation_statistics_product,
     partial_validation_statistics_product) = she_measure_statistics(details_table=details_table,
                                                                     shear_estimates_product=shear_estimates_product)

    return estimation_statistics_product, partial_validation_statistics_product


@pipeline(outputs=('bias_measurements_product', 'bias_measurements_listfile',
                   'validation_statistics_product', 'validation_statistics_listfile',))
def shear_calibration_pipeline(simulation_config_template,
                               calibration_plan_product,
                               galaxy_population_priors_table):

    simulation_configs_list = she_prepare_configs(simulation_config_template=simulation_config_template,
                                                  calibration_plan_product=calibration_plan_product)

    (estimation_statistics_product,
     partial_validation_statistics_product) = she_simulate_and_measure_bias_statistics(simulation_config=simulation_configs_list,
                                                                                       galaxy_population_priors_table=galaxy_population_priors_table)

    bias_measurements_product = she_measure_bias(estimation_statistics_products=estimation_statistics_product)

    validation_statistics_product = she_compile_statistics(partial_validation_statistics_products=partial_validation_statistics_product,
                                                           bias_measurements_product=bias_measurements_product)

    return (bias_measurements_product, bias_measurements_listfile,
            validation_statistics_product, validation_statistics_listfile)

if __name__ == '__main__':
    from euclidwf.framework.graph_builder import build_graph
    from euclidwf.utilities import visualizer
    pydron_graph = build_graph(shear_calibration_pipeline)
    visualizer.visualize_graph(pydron_graph)
