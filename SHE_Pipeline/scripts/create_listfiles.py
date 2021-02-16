#!/usr/bin/env python

""" @file create_listfiles.py

    Created 15 July 2019
    
    Script to search through the current directory for MER and VIS products, and create listfiles for running of
    Analysis and Reconciliation pipelines for all observation IDs and Tile IDs.
    
    Must be run with E-Run.
"""

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

from collections import namedtuple
from enum import Enum
import os

from SHE_PPT import products
from SHE_PPT.file_io import read_xml_product, write_listfile, find_aux_file
from SHE_PPT.logging import getLogger
from SHE_PPT.products.mer_final_catalog import dpdMerFinalCatalog
from SHE_PPT.products.mer_segmentation_map import dpdMerSegmentationMap
from SHE_PPT.products.she_exposure_segmentation_map import dpdSheExposureReprojectedSegmentationMap
from SHE_PPT.products.she_stack_segmentation_map import dpdSheStackReprojectedSegmentationMap
from SHE_PPT.products.vis_calibrated_frame import dpdVisCalibratedFrame
from SHE_PPT.products.vis_stacked_frame import dpdVisStackedFrame
from SHE_PPT.utility import get_nested_attr


logger = getLogger(__name__)

ROOT_DIR = "."

ANALYSIS_ISF_HEAD = "analysis_isf_"
ANALYSIS_ISF_TAIL = ".txt"

RECONCILIATION_ISF_HEAD = "reconciliation_isf_"
RECONCILIATION_ISF_TAIL = ".txt"

MDB_FILENAME = "sample_mdb-SC8.xml"


class ProdKeys(Enum):
    # Dict keys Enum
    MFC = "MER Final Catalogue"
    MSEG = "MER Segmentation Map"
    SESEG = "SHE Exposure Reprojected Segmentation Map"
    SSSEG = "SHE Stack Reprojected Segmentation Map"
    VCF = "VIS Calibrated Frame"
    VSF = "VIS Stacked Frame"


PRODUCT_KEYS = (ProdKeys.MFC, ProdKeys.MSEG, ProdKeys.SESEG, ProdKeys.SSSEG, ProdKeys.VCF, ProdKeys.VSF)

# ISF ports

ISF_PORTS = {ProdKeys.MFC: "mer_final_catalog_listfile",
             ProdKeys.MSEG: "mer_segmentation_map_listfile",
             ProdKeys.SESEG: "she_exposure_reprojected_segmentation_map_listfile",
             ProdKeys.SSSEG: "she_stack_reprojected_segmentation_map",
             ProdKeys.VCF: "vis_calibrated_frame_listfile",
             ProdKeys.VSF: "vis_stacked_frame",
             }

FIXED_ANALYSIS_ISF_FILENAMES = [f"mdb = {MDB_FILENAME}",
                                "phz_output_cat = None",
                                "ksb_training_data = test_ksb_training.xml"
                                "lensmc_training_data = test_lensmc_training.xml",
                                "momentsml_training_data = None",
                                "regauss_training_data = test_regauss_training.xml",
                                "spe_output_cat = None"]

RECONCILIATION_ISF_FILENAMES = ["she_validated_measurements_listfile=SheValidatedMeasurementsListfile-TILEID.json",
                                "she_lensmc_chains_listfile=SheLensMcChainsListfile-TILEID.json"]
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
ProductTypeData = namedtuple("ProductTypeData", ["type", "full_list",
                                                 "obs_id_dict", "tile_id_dict", "filename_head", "filename_tail"])

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

    if filename == MDB_FILENAME or filename[-4] != ".xml":
        continue

    product = read_xml_product(filename, workdir=ROOT_DIR)

    identified_product = False

    # Find the type of the product and add it to the appropriate list
    for product_key in PRODUCT_KEYS:

        product_type_data = product_type_data_dict[product_key]

        if not isinstance(product, product_type_data.type):
            continue

        identified_product = True

        # Add this to the full list for the product
        product_type_data.full_list.append(FileProduct(filename, product))

    if not identified_product:
        logger.warning(f"Cannot identify type of product {filename}")

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
        obs_id_or_list = get_nested_attr(fileprod.product, attr)
        if is_list:
            # List of IDs, so iterate over it and add to each list
            for obs_id in obs_id_or_list:
                product_type_data.obs_id_dict[obs_id].append(fileprod)
        else:
            # Just one ID, so add it directly
            product_type_data.obs_id_dict[obs_id_or_list].append(fileprod)

# Add tile IDs from the final catalogs
for final_catalog_fileprod in product_type_data_dict[ProdKeys.MFC].full_list:
    tile_id = final_catalog_fileprod.product.Data.TileIndex
    tile_id_set.add(tile_id)

    # Also add this to the dict for fileprods of final catalogs for this ID
    product_type_data_dict[ProdKeys.MFC].tile_id_dict[tile_id] = [final_catalog_fileprod]

    # And init the list for this obs_id in the obs_id_dict for other types
    for prod_key in PRODUCT_KEYS:
        if prod_key == ProdKeys.MFC:
            continue
        product_type_data_dict[prod_key].tile_id_dict[obs_id] = []

if len(observation_id_set) == 0:
    logger.error("No observation IDs found.")
    exit()

if len(tile_id_set) == 0:
    logger.error("No tile IDs found.")
    exit()

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
            tile_id = get_nested_attr(fileprod.product, attr)
        else:
            # This is an observation product, so we'll have to find its corresponding tiles indirectly
            obs_id = get_nested_attr(fileprod.product, attr)
            for final_catalog_fileprod in product_type_data_dict[ProdKeys.MFC].full_list:
                if obs_id in final_catalog_fileprod.product.Data.ObservationIdList:
                    tile_id = final_catalog_fileprod.product.Data.TileIndex
        product_type_data.tile_id_dict[tile_id].append(fileprod)


# Set up Analysis listfiles and ISFs for each observation ID
for obs_id in observation_id_set:

    filename_dict = {}

    # Set up and write the listfiles
    for prod_key, sort_by in ((ProdKeys.MFC, "Data.TileIndex"),
                              (ProdKeys.MSEG, "Data.TileIndex"),
                              (ProdKeys.SESEG, "Data.PointingId"),
                              (ProdKeys.SSSEG, "Data.PointingId"),
                              (ProdKeys.VCF, "Data.PointingId"),
                              ):

        product_type_data = product_type_data_dict[prod_key]
        filename = product_type_data.filename_head + str(obs_id) + product_type_data.filename_tail
        filename_dict[prod_key] = filename

        obs_fileprod_list = product_type_data.obs_id_dict[obs_id]
        obs_fileprod_list.sort(key=lambda p: get_nested_attr(p, sort_by))

        obs_filename_list = [obs_fileprod.filename for obs_fileprod in obs_fileprod_list]

        write_listfile(os.path.join(ROOT_DIR, filename), obs_filename_list)

    # Set the filename for the VIS Calibrated Frame product
    filename_dict[ProdKeys.VSF] = product_type_data_dict[ProdKeys.VSF].obs_id_dict[obs_id][0].filename

    # Write the ISF for this observation
    isf_filename = ANALYSIS_ISF_HEAD + str(obs_id) + ANALYSIS_ISF_TAIL
    with open(isf_filename, "w") as fo:
        # Write these listfile filenames to the ISF
        for prod_key in PRODUCT_KEYS:
            fo.write(f"{ISF_PORTS[prod_key]}={filename_dict[prod_key]}")
        # Write the fixed product filenames to the ISF
        for l in FIXED_ANALYSIS_ISF_FILENAMES:
            fo.write(l)

# Set up Reconciliation listfiles and ISFs for each Tile ID
for tile_id in tile_id_set:

    filename_dict = {}

    # Set up and write the listfiles
    prod_key = ProdKeys.MFC
    sort_by = "Data.TileIndex"

    product_type_data = product_type_data_dict[prod_key]
    filename = product_type_data.filename_head + str(tile_id) + product_type_data.filename_tail
    filename_dict[prod_key] = filename

    obs_fileprod_list = product_type_data.tile_id_dict[tile_id]
    obs_fileprod_list.sort(key=lambda p: get_nested_attr(p, sort_by))

    obs_filename_list = [obs_fileprod.filename for obs_fileprod in obs_fileprod_list]

    write_listfile(os.path.join(ROOT_DIR, filename), obs_filename_list)

    # Write the ISF for this tile
    isf_filename = RECONCILIATION_ISF_HEAD + str(tile_id) + RECONCILIATION_ISF_TAIL
    with open(isf_filename, "w") as fo:
        # Write the final catalog listfile filename to the ISF
        fo.write(f"{ISF_PORTS[prod_key]}={filename_dict[prod_key]}")
        # Write the fixed product filenames to the ISF
        for l in RECONCILIATION_ISF_FILENAMES:
            fo.write(l.replace("TILEID", str(tile_id)))
