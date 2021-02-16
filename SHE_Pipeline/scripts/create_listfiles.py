#!/usr/bin/env python

""" @file create_listfiles.py

    Created 15 July 2019
    
    Script to search through the current directory for MER and VIS products, and create listfiles for running of
    Analysis and Reconciliation pipelines for all observation IDs and Tile IDs.
    
    Must be run with E-Run.
"""
from collections import namedtuple
from enum import Enum
import os

from SHE_PPT import products
from SHE_PPT.file_io import read_xml_product, write_listfile
from SHE_PPT.products.mer_final_catalog import dpdMerFinalCatalog
from SHE_PPT.products.mer_segmentation_map import dpdMerSegmentationMap
from SHE_PPT.products.she_exposure_segmentation_map import dpdSheExposureReprojectedSegmentationMap
from SHE_PPT.products.she_stack_segmentation_map import dpdSheStackReprojectedSegmentationMap
from SHE_PPT.products.vis_calibrated_frame import dpdVisCalibratedFrame
from SHE_PPT.products.vis_stacked_frame import dpdVisStackedFrame
from SHE_PPT.utility import get_nested_attr


__updated__ = "2021-02-16"

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


ROOT_DIR = "."


class ProdKeys(Enum):
    # Dict keys Enum
    MFC = "MER Final Catalogue"
    MSEG = "MER Segmentation Map"
    SESEG = "SHE Exposure Reprojected Segmentation Map"
    SSSEG = "SHE Stack Reprojected Segmentation Map"
    VCF = "VIS Calibrated Frame"
    VSF = "VIS Stacked Frame"


PRODUCT_KEYS = (ProdKeys.MFC, ProdKeys.MSEG, ProdKeys.SESEG, ProdKeys.SSSEG, ProdKeys.VCF, ProdKeys.VSF)

# Product types

PRODUCT_TYPES = {ProdKeys.MFC: dpdMerFinalCatalog,
                 ProdKeys.MSEG: dpdMerSegmentationMap,
                 ProdKeys.SESEG: dpdSheExposureReprojectedSegmentationMap,
                 ProdKeys.SSSEG: dpdSheStackReprojectedSegmentationMap,
                 ProdKeys.VCF: dpdVisCalibratedFrame,
                 ProdKeys.VSF: dpdVisStackedFrame,
                 }


# Desired filename info

FILENAME_HEADS = {ProdKeys.MFC: "mer_final_catalog_obs_",
                  ProdKeys.MSEG: "mer_segmentation_map_obs_",
                  ProdKeys.SESEG: "she_exposure_reprojected_segmentation_map_obs_",
                  ProdKeys.SSSEG: "she_exposure_reprojected_segmentation_map_obs_",
                  ProdKeys.VCF: "vis_calibrated_frame_",
                  ProdKeys.VSF: None,
                  }

FILENAME_TAILS = {ProdKeys.MFC: "_listfile.json",
                  ProdKeys.MSEG: "_listfile.json",
                  ProdKeys.SESEG: "_listfile.json",
                  ProdKeys.SSSEG: "_product.xml",
                  ProdKeys.VCF: "_listfile.json",
                  ProdKeys.VSF: None,
                  }


# A namedtuple type for all data of each product type
ProductTypeData = namedtuple("ProductTypeData", ["type", "full_list", "obs_id_dict", "tile_id_dict","filename_head", "filename_tail"])

# A namedtuple type for a filename and product
FileProduct = namedtuple("FileProduct", ["filename", "product"])

# Init dict of data for each product type
product_type_data_dict = {}
for key in PRODUCT_KEYS:
    product_type_data_dict[key] = ProductTypeData(PRODUCT_TYPES[key],
                                                  [],
                                                  {},
                                                  {},
                                                  FILENAME_HEADS[key],
                                                  FILENAME_TAILS[key])

# Get all existing products, read them in, and sort them depending on type

all_filenames = os.listdir(ROOT_DIR)

for filename in all_filenames:

    if not filename[-4] == ".xml":
        continue

    product = read_xml_product(filename, workdir=ROOT_DIR)

    # Find the type of the product and add it to the appropriate list
    for product_key in PRODUCT_KEYS:
        
        product_type_data = product_type_data_dict[product_key]
        
        if not isinstance(product, product_type_data.type):
            continue
        
        # Add this to the full list for the product
        product_type_data.full_list.append(FileProduct(filename, product))
        
# Get sets of all Observation and Tile IDs
observation_id_set = set()
tile_id_set = set()

# Add observation IDs from the stacked frames
for stacked_frame_fileprod in product_type_data_dict[ProdKeys.VSF].full_list:
    obs_id = stacked_frame_fileprod.product.Data.ObservationSequence.ObservationId
    observation_id_set.add(obs_id)
    
    # Also add this to the dict for fileprods of stacked frames for this ID
    product_type_data_dict[ProdKeys.VSF].obs_id_dict[obs_id] = [stacked_frame_fileprod]
    
    # And init the list for this obs_id in the obs_id_dict for other types
    for prod_key in PRODUCT_KEYS:
        if prod_key == ProdKeys.VSF:
            continue
        product_type_data_dict[prod_key].obs_id_dict[obs_id] = []
        
# Fill in the obs_id_dicts for other product types
for prod_key, attr, is_list in ((ProdKeys.MFC, "Data.ObservationIdList", True),
                                (ProdKeys.MSEG, "Data.ObservationIdList", True),
                                (ProdKeys.SESEG, "Data.ObservationId", False),
                                (ProdKeys.SSSEG, "Data.ObservationId", False),
                                (ProdKeys.VCF, "Data.ObservationSequence.ObservationId", True),
                                ):
    product_type_data = product_type_data_dict[prod_key]
    for fileprod in product_type_data.full_list:
        obs_id_or_list = get_nested_attr(fileprod.product,attr)
        if is_list:
            # List of IDs, so iterate over it and add to each list
            for obs_id in obs_id_or_list:
                product_type_data.obs_id_dict[obs_id].append(fileprod)
        else:
            # Just one ID, so add it directly
            product_type_data.obs_id_dict[obs_id_or_list].append(fileprod)
    
# Add tile IDs from the final catalogs
for final_catalog_fileprod in product_type_data_dict[ProdKeys.MFC]:
    tile_id = final_catalog_fileprod.product.Data.TileIndex
    tile_id_set.add(tile_id)
    
    # Also add this to the dict for fileprods of final catalogs for this ID
    product_type_data_dict[ProdKeys.MFC].tile_id_dict[tile_id] = [final_catalog_fileprod]
    
    # And init the list for this obs_id in the obs_id_dict for other types
    for prod_key in PRODUCT_KEYS:
        if prod_key == ProdKeys.MFC:
            continue
        product_type_data_dict[prod_key].tile_id_dict[obs_id] = []
        
# Fill in the tile_id_dicts for other product types
for prod_key, attr, is_tile in ((ProdKeys.MSEG, "Data.TileIndex", True),
                                (ProdKeys.SESEG, "Data.ObservationId", False),
                                (ProdKeys.SSSEG, "Data.ObservationId", False),
                                (ProdKeys.VCF, "Data.ObservationSequence.ObservationId", False),
                                (ProdKeys.VSF, "Data.ObservationId", False),
                                ):
    product_type_data = product_type_data_dict[prod_key]
    for fileprod in product_type_data.full_list:
        if is_tile:
            tile_id = get_nested_attr(fileprod.product,attr)
        else:
            # This is an observation product, so we'll have to find its corresponding tiles indirectly
            obs_id = get_nested_attr(fileprod.product,attr)
            for final_catalog_fileprod in product_type_data_dict[ProdKeys.MFC].full_list:
                if obs_id in final_catalog_fileprod.product.Data.ObservationIdList:
                    tile_id = final_catalog_fileprod.product.Data.TileIndex
        product_type_data.tile_id_dict[tile_id].append(fileprod)
                    
    
# Set up Analysis listfiles and ISFs for each observation ID
for observation_id in observation_id_set:
    