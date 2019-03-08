""" @file analysis_pipeline.py

    Created 29 Aug 2017

    Pipeline script for the shear-estimation-only pipeline.
"""

__updated__ = "2019-03-08"

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

from SHE_Pipeline_pkgdef.analysis_pkgdef import she_fit_psf, she_estimate_shear, she_validate_shear
from euclidwf.framework.workflow_dsl import pipeline


@parallel(iterable="vis_prod_filenames")
def she_remap_mosaics(mer_tile_listfile, vis_prod_filenames):

    segmentation_image = she_remap_mosaic(mer_tile_listfile=mer_tile_listfile,
                                          vis_prod_filename=vis_prod_filenames,)

    return segmentation_image


@pipeline(outputs=('validated_shear_estimates_table'))
def shear_analysis_pipeline(mdb,
                            vis_image,
                            vis_stacked_image,
                            mer_segmentation_map,
                            mer_catalog,
                            # aocs_time_series_products, # Disabled for now
                            # psf_calibration_products, # Disabled for now
                            # galaxy_population_priors_table, # Disabled for now
                            # calibration_parameters_product, # Disabled for now
                            phz_output_cat,
                            spe_output_cat,
                            ksb_training_data,
                            lensmc_training_data,
                            regauss_training_data,
                            bfd_training_data,
                            momentsml_training_data,
                            ):

    stacked_segmentation_image = she_remap_mosaic(mer_tile_listfile=mer_segmentation_map,
                                                  vis_prod_filename=vis_stacked_image,)

    segmentation_images = she_remap_mosaics(mer_tile_listfile=mer_segmentation_map,
                                            vis_prod_filenames=vis_image)

    psf_field_params = she_fit_psf(data_images=vis_image,
                                   segmentation_images=mer_segmentation_map,
                                   detections_tables=mer_catalog,
                                   mdb=mdb,
                                   # aocs_time_series_products = aocs_time_series_products, # Disabled for now
                                   # psf_calibration_products = psf_calibration_products, # Disabled for now
                                   )

    psf_images_and_tables = she_model_psf(data_images=vis_image,
                                          segmentation_images=mer_segmentation_map,
                                          detections_tables=mer_catalog,
                                          mdb=mdb,
                                          # aocs_time_series_products = aocs_time_series_products, # Disabled for now
                                          # psf_calibration_products = psf_calibration_products, # Disabled for now
                                          psf_field_params=psf_field_params,
                                          )

    shear_estimates_product = she_estimate_shear(data_images=vis_image,
                                                 stacked_image=vis_stacked_image,
                                                 psf_images_and_tables=psf_images_and_tables,
                                                 segmentation_images=mer_segmentation_map,
                                                 stacked_segmentation_image=stacked_segmentation_image,
                                                 detections_tables=mer_catalog,
                                                 bfd_training_data=bfd_training_data,
                                                 ksb_training_data=ksb_training_data,
                                                 lensmc_training_data=lensmc_training_data,
                                                 momentsml_training_data=momentsml_training_data,
                                                 regauss_training_data=regauss_training_data,
                                                 mdb=mdb,
                                                 # galaxy_population_priors_table=galaxy_population_priors_table,
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
