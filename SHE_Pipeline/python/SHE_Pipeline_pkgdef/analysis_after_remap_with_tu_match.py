""" @file analysis_after_remap_with_tu_match.py

    Created 13 May 2019

    Pipeline script for the shear-estimation-only pipeline, starting after the segmentation map reprojection step.
"""

__updated__ = "2019-05-28"

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

from SHE_Pipeline_pkgdef.analysis_pkgdef import (she_fit_psf, she_model_psf,
                                                 she_object_id_split, she_shear_estimates_merge,
                                                 she_estimate_shear, she_cross_validate_shear,
                                                 she_match_to_tu)
from euclidwf.framework.workflow_dsl import pipeline, parallel


@parallel(iterable="object_ids")
def she_model_psf_and_estimate_shear(object_ids,
                                     vis_image,
                                     vis_stacked_image,
                                     segmentation_images,
                                     stacked_segmentation_image,
                                     mer_catalog,
                                     bfd_training_data,
                                     ksb_training_data,
                                     lensmc_training_data,
                                     momentsml_training_data,
                                     regauss_training_data,
                                     psf_field_params,
                                     pipeline_config,
                                     mdb,
                                     ):
    """ Parallel branch, where we model PSFs and estimate shears for a batch of galaxies.
    """

    psf_images_and_tables = she_model_psf(object_ids=object_ids,
                                          data_images=vis_image,
                                          detections_tables=mer_catalog,
                                          mdb=mdb,
                                          psf_field_params=psf_field_params,
                                          )

    shear_estimates_product = she_estimate_shear(object_ids=object_ids,
                                                 data_images=vis_image,
                                                 stacked_image=vis_stacked_image,
                                                 psf_images_and_tables=psf_images_and_tables,
                                                 segmentation_images=segmentation_images,
                                                 stacked_segmentation_image=stacked_segmentation_image,
                                                 detections_tables=mer_catalog,
                                                 bfd_training_data=bfd_training_data,
                                                 ksb_training_data=ksb_training_data,
                                                 lensmc_training_data=lensmc_training_data,
                                                 momentsml_training_data=momentsml_training_data,
                                                 regauss_training_data=regauss_training_data,
                                                 pipeline_config=pipeline_config,
                                                 mdb=mdb,
                                                 )

    return shear_estimates_product


@pipeline(outputs=('cross_validated_shear_estimates', 'shear_estimates', 'matched_catalog'))
def shear_analysis_pipeline(mdb,
                            vis_image,
                            vis_stacked_image,
                            stacked_segmentation_image,
                            segmentation_images,
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
                            pipeline_config,
                            tu_galaxy_catalog,
                            tu_star_catalog
                            ):

    psf_field_params = she_fit_psf(data_images=vis_image,
                                   segmentation_images=segmentation_images,
                                   detections_tables=mer_catalog,
                                   mdb=mdb,
                                   )

    # Create list of object ID lists for each batch to process
    object_ids = she_object_id_split(detections_tables=mer_catalog,
                                     pipeline_config=pipeline_config,)

    # Estimate shear in parallel for each batch
    shear_estimates_products = she_model_psf_and_estimate_shear(object_ids=object_ids,
                                                                vis_image=vis_image,
                                                                vis_stacked_image=vis_stacked_image,
                                                                segmentation_images=segmentation_images,
                                                                stacked_segmentation_image=stacked_segmentation_image,
                                                                mer_catalog=mer_catalog,
                                                                bfd_training_data=bfd_training_data,
                                                                ksb_training_data=ksb_training_data,
                                                                lensmc_training_data=lensmc_training_data,
                                                                momentsml_training_data=momentsml_training_data,
                                                                regauss_training_data=regauss_training_data,
                                                                psf_field_params=psf_field_params,
                                                                pipeline_config=pipeline_config,
                                                                mdb=mdb,
                                                                )

    # Merge shear estimates together
    shear_estimates_product = she_shear_estimates_merge(input_shear_estimates_listfile=shear_estimates_products)

    cross_validated_shear_estimates_product = she_cross_validate_shear(shear_estimates_product=shear_estimates_product)

    matched_catalog = she_match_to_tu(shear_estimates_product=shear_estimates_product,
                                      tu_galaxy_catalog=tu_galaxy_catalog,
                                      tu_star_catalog=tu_star_catalog)

    return cross_validated_shear_estimates_product, shear_estimates_product, matched_catalog


if __name__ == '__main__':
    from euclidwf.framework.graph_builder import build_graph
    from euclidwf.utilities import visualizer
    pydron_graph = build_graph(shear_analysis_pipeline)
    visualizer.visualize_graph(pydron_graph)
