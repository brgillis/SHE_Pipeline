""" @file analysis_pipeline.py

    Created 29 Aug 2017

    Pipeline script for the shear-estimation-only pipeline.
"""

__updated__ = "2018-07-04"

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

from SHE_Pipeline_pkgdef.package_definition import she_fit_psf, she_estimate_shear, she_validate_shear
from euclidwf.framework.workflow_dsl import pipeline


@parallel(iterable="vis_prod_filenames")
def she_remap_mosaics(mer_tile_listfile, vis_prod_filenames):

    segmentation_image = she_remap_mosaic(mer_tile_listfile=mer_tile_listfile,
                                          vis_prod_filename=vis_prod_filenames,)

    return segmentation_image


@pipeline(outputs=('validated_shear_estimates_table'))
def shear_analysis_pipeline(data_images,
                            stacked_image,
                            mer_tiles,
                            detections_tables,
                            # aocs_time_series_products, # Disabled for now
                            # psf_calibration_products, # Disabled for now
                            galaxy_population_priors_table,
                            # calibration_parameters_product, # Disabled for now
                            ):

    stacked_segmentation_image = she_remap_mosaic(mer_tile_listfile=mer_tiles,
                                                  vis_prod_filename=stacked_image,)

    segmentation_images = she_remap_mosaics(mer_tile_listfile=mer_tiles,
                                            vis_prod_filenames=data_images)

    psf_field_params = she_fit_psf(data_images=data_images,
                                   segmentation_images=segmentation_images,
                                   detections_tables=detections_tables,
                                   # aocs_time_series_products = aocs_time_series_products, # Disabled for now
                                   # psf_calibration_products = psf_calibration_products, # Disabled for now
                                   )

    psf_images_and_tables = she_model_psf(data_images=data_images,
                                          segmentation_images=segmentation_images,
                                          detections_tables=detections_tables,
                                          # aocs_time_series_products = aocs_time_series_products, # Disabled for now
                                          # psf_calibration_products = psf_calibration_products, # Disabled for now
                                          psf_field_params=psf_field_params,
                                          )

    shear_estimates_product = she_estimate_shear(data_images=data_images,
                                                 stacked_image=stacked_image,
                                                 psf_images_and_tables=psf_images_and_tables,
                                                 segmentation_images=segmentation_images,
                                                 stacked_segmentation_image=stacked_segmentation_image,
                                                 detections_tables=detections_tables,
                                                 # bfd_training_data = bfd_training_data, # Disabled for now
                                                 # ksb_training_data = ksb_training_data, # Disabled for now
                                                 # lensmc_training_data = lensmc_training_data, # Disabled for now
                                                 # momentsml_training_data = momentsml_training_data, # Disabled for now
                                                 # regauss_training_data = regauss_training_data, # Disabled for now
                                                 galaxy_population_priors_table=galaxy_population_priors_table,
                                                 # calibration_parameters_product = calibration_parameters_product, #
                                                 # Disabled for now
                                                 )

    cross_validated_shear_estimates_product = she_cross_validate_shear(shear_estimates_product=shear_estimates_product)

    return cross_validated_shear_estimates_product

if __name__ == '__main__':
    from euclidwf.framework.graph_builder import build_graph
    from euclidwf.utilities import visualizer
    pydron_graph = build_graph(shear_analysis_pipeline)
    visualizer.visualize_graph(pydron_graph)
