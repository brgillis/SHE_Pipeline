""" @file package_definition.py

    Created 29 Aug 2017

    Package definition for the OU-SHE pipeline.
"""

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

from euclidwf.framework.taskdefs import Executable, Input, Output, ComputingResources

ERun_CTE = "E-Run SHE_CTE 0.4 "

she_prepare_configs = Executable(command = ERun_CTE + "SHE_CTE_PrepareConfigs",
                                 inputs = [Input("simulation_config_template"), Input("calibration_plan_product")],
                                 outputs = [Output("simulation_configs_list", mime_type = "json", content_type = "listfile")])

she_simulate_images = Executable(command = ERun_CTE + "SHE_CTE_SimulateImages",
                                 inputs = [Input("simulation_config"),
                                         Input("galaxy_population_priors_table")],
                                 outputs = [Output("data_images", mime_type = "json", content_type = "listfile"),
                                          Output("psf_images_and_tables", mime_type = "json", content_type = "listfile"),
                                          Output("segmentation_images", mime_type = "json", content_type = "listfile"),
                                          Output("detections_tables", mime_type = "json", content_type = "listfile"),
                                          Output("details_table", mime_type = "xml")])

she_remap_mosaic = Executable(command = "E-Run SHE_MER 0.2 SHE_MER_RemapMosaic",
                              inputs = [Input("mer_tile_listfile", content_type= "listfile"),
                                        Input("vis_prod_filename")],
                              outputs = [Output("output_filename", mime_type = 'xml')])

she_fit_psf = Executable(command = ERun_CTE + "SHE_CTE_FitPSFs",
                         inputs = [Input("data_images", content_type = "listfile"),
                                 Input("detections_tables", content_type = "listfile"),
                                 Input("segmentation_images", content_type = "listfile"),
                                 # Input("aocs_time_series_products", content_type="listfile"), # Disabled for now
                                 # Input("psf_calibration_products", content_type="listfile"), # Disabled for now
                                 ],
                         outputs = [Output("psf_field_params", mime_type = "json", content_type = "listfile")])

she_model_psf = Executable(command = ERun_CTE + "SHE_CTE_ModelPSFs",
                           inputs = [Input("data_images", content_type = "listfile"),
                                     Input("detections_tables", content_type = "listfile"),
                                     Input("segmentation_images", content_type = "listfile"),
                                     Input("psf_field_params", content_type = "listfile")
                                     # Input("aocs_time_series_products", content_type="listfile"), # Disabled for now
                                     # Input("psf_calibration_products", content_type="listfile"), # Disabled for now
                                     ],
                           outputs = [Output("psf_images_and_tables", mime_type = "json", content_type = "listfile")])

she_estimate_shear = Executable(command = ERun_CTE + "SHE_CTE_EstimateShear",
                                 inputs = [Input("data_images", content_type = "listfile"),
                                         Input("stacked_image", content_type = "listfile"),
                                         Input("psf_images_and_tables", content_type = "listfile"),
                                         Input("segmentation_images", content_type = "listfile"),
                                         Input("stacked_segmentation_image", content_type = "listfile"),
                                         Input("detections_tables", content_type = "listfile"),
                                         # Input("bfd_training_data", content_type="listfile"), # Disabled for now
                                         # Input("ksb_training_data", content_type="listfile"), # Disabled for now
                                         # Input("lensmc_training_data", content_type="listfile"), # Disabled for now
                                         # Input("momentsml_training_data", content_type="listfile"), # Disabled for now
                                         # Input("regauss_training_data", content_type="listfile"), # Disabled for now
                                         Input("galaxy_population_priors_table"),
                                         # Input("calibration_parameters_product"), # Disabled for now
                                         ],
                                 outputs = [Output("shear_estimates_product", mime_type = "xml"), ])

she_cross_validate_shear = Executable(command = ERun_CTE + "SHE_CTE_CrossValidateShear",
                                      inputs = [Input("shear_estimates_product")],
                                      outputs = [Output("cross_validated_shear_estimates_product", mime_type = "xml")])

she_measure_statistics = Executable(command = ERun_CTE + "SHE_CTE_MeasureStatistics",
                                    inputs = [Input("details_table"),
                                            Input("shear_estimates_product")],
                                    outputs = [Output("estimation_statistics_product", mime_type = "xml"),
                                             Output("partial_validation_statistics_product", mime_type = "json")])

she_measure_bias = Executable(command = ERun_CTE + "SHE_CTE_MeasureBias",
                                 inputs = [Input("estimation_statistics_products", content_type = "listfile")],
                                 outputs = [Output("bias_measurements_product", mime_type = "xml")])

she_compile_statistics = Executable(command = ERun_CTE + "SHE_CTE_CompileStatistics",
                                     inputs = [Input("partial_validation_statistics_products", content_type = "listfile"),
                                             Input("bias_measurements_product")],
                                     outputs = [Output("validation_statistics_product", mime_type = "xml")])

