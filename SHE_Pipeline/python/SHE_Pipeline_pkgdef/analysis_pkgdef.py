""" @file analysis_pkgdef.py

    Created 29 Aug 2017

    Package definition for the OU-SHE analysis pipeline.
"""

__updated__ = "2019-07-23"

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

from SHE_Pipeline_pkgdef.magic_values import ERun_CTE, ERun_MER, ERun_PSF
from euclidwf.framework.taskdefs import Executable, Input, Output, ComputingResources

she_remap_mosaic_stack = Executable(command=ERun_MER + "SHE_MER_RemapMosaic",
                                    inputs=[Input("mer_tile_listfile", content_type="listfile"),
                                            Input("vis_prod_filename"),
                                            Input("pipeline_config")],
                                    outputs=[Output("output_filename", mime_type='xml')],
                                    resources=ComputingResources(cores=4, ram=15.9, walltime=2.0))

she_remap_mosaic_exposure = Executable(command=ERun_MER + "SHE_MER_RemapMosaic",
                                       inputs=[Input("mer_tile_listfile", content_type="listfile"),
                                               Input("vis_prod_filename"),
                                               Input("pipeline_config")],
                                       outputs=[Output("output_filename", mime_type='xml')],
                                       resources=ComputingResources(cores=8, ram=15.9, walltime=4.0))

she_fit_psf = Executable(command=ERun_CTE + "SHE_CTE_FitPSFs",
                         inputs=[Input("data_images", content_type="listfile"),
                                 Input("detections_tables", content_type="listfile"),
                                 Input("segmentation_images", content_type="listfile"),
                                 Input("mdb"),
                                 # Input("aocs_time_series_products", content_type="listfile"), # Disabled for now
                                 # Input("psf_calibration_products", content_type="listfile"), # Disabled for now
                                 ],
                         outputs=[Output("psf_field_params", mime_type="json", content_type="listfile")],
                         resources=ComputingResources(cores=8, ram=15.9, walltime=0.5))

she_object_id_split = Executable(command=ERun_CTE + "SHE_CTE_ObjectIdSplit",
                                 inputs=[Input("detections_tables", content_type="listfile"),
                                         Input("pipeline_config"), ],
                                 outputs=[Output("object_ids", mime_type='json')],
                                 resources=ComputingResources(cores=1, ram=1.0, walltime=1.0))

she_model_psf = Executable(command=ERun_PSF + "SHE_PSFToolkit_ModelPSFs",
                           inputs=[Input("data_images", content_type="listfile"),
                                   Input("detections_tables", content_type="listfile"),
                                   Input("psf_field_params"),
                                   Input("object_ids"),
                                   Input("mdb"),
                                   # Input("aocs_time_series_products", content_type="listfile"), # Disabled for now
                                   # Input("psf_calibration_products", content_type="listfile"), # Disabled for now
                                   ],
                           outputs=[Output("psf_images_and_tables", mime_type="json", content_type="listfile")],
                           resources=ComputingResources(cores=1, ram=5.9, walltime=4.0))

she_estimate_shear = Executable(command=ERun_CTE + "SHE_CTE_EstimateShear",
                                inputs=[Input("data_images", content_type="listfile"),
                                        Input("stacked_image"),
                                        Input("psf_images_and_tables", content_type="listfile"),
                                        Input("segmentation_images", content_type="listfile"),
                                        Input("stacked_segmentation_image"),
                                        Input("detections_tables", content_type="listfile"),
                                        Input("object_ids"),
                                        Input("bfd_training_data"),
                                        Input("ksb_training_data"),
                                        Input("lensmc_training_data"),
                                        Input("momentsml_training_data"),
                                        Input("regauss_training_data"),
                                        Input("pipeline_config"),
                                        Input("mdb"),
                                        # Input("galaxy_population_priors_table"), # Disabled for now
                                        # Input("calibration_parameters_product"), # Disabled for now
                                        ],
                                outputs=[Output("shear_estimates_product", mime_type="xml"), ],
                                resources=ComputingResources(cores=1, ram=5.9, walltime=4.0))

she_shear_estimates_merge = Executable(command=ERun_CTE + "SHE_CTE_ShearEstimatesMerge",
                                       inputs=[Input("input_shear_estimates_listfile", content_type="listfile"), ],
                                       outputs=[Output("output_shear_estimates", mime_type='xml')],
                                       resources=ComputingResources(cores=8, ram=7.9, walltime=2.0))

she_cross_validate_shear = Executable(command=ERun_CTE + "SHE_CTE_CrossValidateShear",
                                      inputs=[Input("shear_estimates_product")],
                                      outputs=[Output("cross_validated_shear_estimates_product", mime_type="xml")],
                                      resources=ComputingResources(cores=1, ram=7.9, walltime=2.0))

she_match_to_tu = Executable(command=ERun_CTE + "SHE_CTE_MatchToTU",
                             inputs=[Input("shear_estimates_product"),
                                     Input("tu_galaxy_catalog"),
                                     Input("tu_star_catalog")],
                             outputs=[Output("matched_catalog", mime_type="xml")],
                             resources=ComputingResources(cores=1, ram=15.9, walltime=2.0))
