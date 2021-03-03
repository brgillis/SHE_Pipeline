#!/usr/bin/env python

""" @file create_listfiles.py

    Created 15 July 2019
    
    Script to search through the current directory for MER and VIS products, and create listfiles for running of
    Analysis and Reconciliation pipelines for all observation IDs and Tile IDs.
    
    Must be run with E-Run.
"""

__updated__ = "2021-03-03"

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
from SHE_PPT.products.she_lensmc_chains import dpdSheLensMcChains
from SHE_PPT.products.she_stack_segmentation_map import dpdSheStackReprojectedSegmentationMap
from SHE_PPT.products.she_validated_measurements import dpdSheValidatedMeasurements
from SHE_PPT.products.tu_gal_cat import dpdGalaxyCatalogProduct
from SHE_PPT.products.tu_star_cat import dpdStarsCatalogProduct
from SHE_PPT.products.vis_calibrated_frame import dpdVisCalibratedFrame
from SHE_PPT.products.vis_stacked_frame import dpdVisStackedFrame
from SHE_PPT.utility import get_nested_attr


logger = getLogger(__name__)

ROOT_DIR = "."

ANALYSIS_ISF_HEAD = "analysis_"
ANALYSIS_ISF_TAIL = "_isf.txt"

AFTER_REMAP_TAG = "after_remap_"
TU_MATCH_TAG = "with_tu_match_"

RECONCILIATION_ISF_HEAD = "reconciliation_"
RECONCILIATION_ISF_TAIL = "_isf.txt"

MDB_FILENAME = "sample_mdb-SC8.xml"


class ProdKeys(Enum):
    # Dict keys Enum
    MFC = "MER Final Catalogue"
    MSEG = "MER Segmentation Map"
    SESEG = "SHE Exposure Reprojected Segmentation Map"
    SSSEG = "SHE Stack Reprojected Segmentation Map"
    SVM = "SHE Validated Measurements"
    SLMC = "SHE LensMC Chains"
    VCF = "VIS Calibrated Frame"
    VSF = "VIS Stacked Frame"
    TUG = "TU Galaxy Catalog"
    TUS = "TU Star Catalog"


ANALYSIS_PRODUCT_KEYS = (ProdKeys.MFC, ProdKeys.MSEG, ProdKeys.SESEG, ProdKeys.SSSEG,
                         ProdKeys.VCF, ProdKeys.VSF, ProdKeys.TUG, ProdKeys.TUS)
RECONCILIATION_PRODUCT_KEYS = (ProdKeys.MFC, ProdKeys.SVM, ProdKeys.SLMC,)

# Which pipelines a given product is used for
# 1 = only for that variant, -1 = only not that variant, 0 = both variants
ONLY_FOR_AFTER_REMAP = {ProdKeys.MFC: 0,
                        ProdKeys.MSEG: -1,
                        ProdKeys.SESEG: 1,
                        ProdKeys.SSSEG: 1,
                        ProdKeys.VCF: 0,
                        ProdKeys.VSF: 0,
                        ProdKeys.TUG: 0,
                        ProdKeys.TUS: 0,
                        }
ONLY_FOR_TU_MATCH = {ProdKeys.MFC: 0,
                     ProdKeys.MSEG: 0,
                     ProdKeys.SESEG: 0,
                     ProdKeys.SSSEG: 0,
                     ProdKeys.VCF: 0,
                     ProdKeys.VSF: 0,
                     ProdKeys.TUG: 1,
                     ProdKeys.TUS: 1,
                     }

# ISF ports and common filenames

ANALYSIS_ISF_PORTS = {ProdKeys.MFC: "mer_final_catalog_listfile",
                      ProdKeys.MSEG: "mer_segmentation_map_listfile",
                      ProdKeys.SESEG: "she_exposure_reprojected_segmentation_map_listfile",
                      ProdKeys.SSSEG: "she_stack_reprojected_segmentation_map",
                      ProdKeys.VCF: "vis_calibrated_frame_listfile",
                      ProdKeys.VSF: "vis_stacked_frame",
                      ProdKeys.TUG: "tu_galaxy_catalog_list",
                      ProdKeys.TUS: "tu_star_catalog_list",
                      }

FIXED_ANALYSIS_ISF_FILENAMES = [f"mdb = {MDB_FILENAME}",
                                "phz_output_cat = None",
                                "ksb_training_data = test_ksb_training.xml",
                                "lensmc_training_data = test_lensmc_training.xml",
                                "momentsml_training_data = None",
                                "regauss_training_data = test_regauss_training.xml",
                                "spe_output_cat = None"]

RECONCILIATION_ISF_PORTS = {ProdKeys.MFC: "mer_final_catalog",
                            ProdKeys.SVM: "she_validated_measurements_listfile",
                            ProdKeys.SLMC: "she_lensmc_chains_listfile",
                            }

RECONCILIATION_ISF_FILENAMES = ["she_validated_measurements_listfile=SheValidatedMeasurementsListfile-TILEID.json",
                                "she_lensmc_chains_listfile=SheLensMcChainsListfile-TILEID.json"]
# Product types

PRODUCT_TYPES = {ProdKeys.MFC: dpdMerFinalCatalog,
                 ProdKeys.MSEG: dpdMerSegmentationMap,
                 ProdKeys.SESEG: dpdSheExposureReprojectedSegmentationMap,
                 ProdKeys.SSSEG: dpdSheStackReprojectedSegmentationMap,
                 ProdKeys.SVM: dpdSheValidatedMeasurements,
                 ProdKeys.SLMC: dpdSheLensMcChains,
                 ProdKeys.VCF: dpdVisCalibratedFrame,
                 ProdKeys.VSF: dpdVisStackedFrame,
                 ProdKeys.TUG: dpdGalaxyCatalogProduct,
                 ProdKeys.TUS: dpdStarsCatalogProduct,
                 }


# Desired filename info

FILENAME_HEADS = {ProdKeys.MFC: "mer_final_catalog_obs_",
                  ProdKeys.MSEG: "mer_segmentation_map_obs_",
                  ProdKeys.SESEG: "she_exposure_reprojected_segmentation_map_obs_",
                  ProdKeys.SSSEG: "she_stack_reprojected_segmentation_map_obs_",
                  ProdKeys.SVM: "she_validated_measurements_listfile_tile_",
                  ProdKeys.SLMC: "she_lensmc_chains_listfile_tile_",
                  ProdKeys.VCF: "vis_calibrated_frame_obs_",
                  ProdKeys.VSF: None,
                  ProdKeys.TUG: "galcats",
                  ProdKeys.TUS: "starcats",
                  }

FILENAME_TAILS = {ProdKeys.MFC: "_listfile.json",
                  ProdKeys.MSEG: "_listfile.json",
                  ProdKeys.SESEG: "_listfile.json",
                  ProdKeys.SSSEG: "_product.xml",
                  ProdKeys.SVM: "_listfile.json",
                  ProdKeys.SLMC: "_listfile.json",
                  ProdKeys.VCF: "_listfile.json",
                  ProdKeys.VSF: None,
                  ProdKeys.TUG: ".json",
                  ProdKeys.TUS: ".json",
                  }


# A namedtuple type for all data of each product type
ProductTypeData = namedtuple("ProductTypeData", ["type",
                                                 "full_list",
                                                 "obs_id_dict",
                                                 "tile_id_dict",
                                                 "filename_head",
                                                 "filename_tail"])

# A namedtuple type for a filename and product
FileProduct = namedtuple("FileProduct", ["filename", "product"])

# Init dict of data for each product type
product_type_data_dict = {}
for key in ANALYSIS_PRODUCT_KEYS + RECONCILIATION_PRODUCT_KEYS:
    product_type_data_dict[key] = ProductTypeData(PRODUCT_TYPES[key],
                                                  [],
                                                  {},
                                                  {},
                                                  FILENAME_HEADS[key],
                                                  FILENAME_TAILS[key])

# Get all existing products, read them in, and sort them depending on type

all_filenames = os.listdir(ROOT_DIR)

for filename in all_filenames:

    if filename == MDB_FILENAME or filename[-4:] != ".xml":
        continue

    try:
        product = read_xml_product(filename, workdir=ROOT_DIR)
    except:
        raise ValueError("Can't interpret file " + filename)

    identified_product = False

    # Find the type of the product and add it to the appropriate list
    for product_key in ANALYSIS_PRODUCT_KEYS + RECONCILIATION_PRODUCT_KEYS:

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

# Fill in the obs_id_dicts for all product types
for prod_key, attr, is_list in ((ProdKeys.SESEG, "Data.ObservationId", False),
                                (ProdKeys.SSSEG, "Data.ObservationId", False),
                                (ProdKeys.SVM, "Data.ObservationId", False),
                                (ProdKeys.SLMC, "Data.ObservationId", False),
                                (ProdKeys.VSF, "Data.ObservationId", False),
                                (ProdKeys.VCF, "Data.ObservationSequence.ObservationId", False),
                                (ProdKeys.MFC, "Data.ObservationIdList", True),
                                (ProdKeys.MSEG, "Data.ObservationIdList", True),
                                ):
    product_type_data = product_type_data_dict[prod_key]
    for fileprod in product_type_data.full_list:
        obs_id_or_list = get_nested_attr(fileprod.product, attr)
        if is_list:
            # List of IDs, so iterate over it and add to each list
            for obs_id in obs_id_or_list:
                if obs_id in product_type_data.obs_id_dict:
                    product_type_data.obs_id_dict[obs_id].append(fileprod)
        else:
            # Just one ID, so add it directly
            if obs_id_or_list in product_type_data.obs_id_dict:
                product_type_data.obs_id_dict[obs_id_or_list].append(fileprod)
            else:
                product_type_data.obs_id_dict[obs_id_or_list] = [fileprod]
                observation_id_set.add(obs_id_or_list)

# Fill in the tile_id_dicts for all product types
for prod_key, attr, is_tile in ((ProdKeys.MFC, "Data.TileIndex", True),
                                (ProdKeys.MSEG, "Data.TileIndex", True),
                                (ProdKeys.SESEG, "Data.ObservationId", False),
                                (ProdKeys.SSSEG, "Data.ObservationId", False),
                                (ProdKeys.SVM, "Data.ObservationId", False),
                                (ProdKeys.SLMC, "Data.ObservationId", False),
                                (ProdKeys.VCF, "Data.ObservationSequence.ObservationId", False),
                                (ProdKeys.VSF, "Data.ObservationId", False),
                                ):
    product_type_data = product_type_data_dict[prod_key]
    for fileprod in product_type_data.full_list:
        if is_tile:
            tile_ids = [get_nested_attr(fileprod.product, attr)]
        else:
            # This is an observation product, so we'll have to find its corresponding tiles indirectly
            obs_id = get_nested_attr(fileprod.product, attr)
            tile_ids = []
            for final_catalog_fileprod in product_type_data_dict[ProdKeys.MFC].full_list:
                if obs_id in final_catalog_fileprod.product.Data.ObservationIdList:
                    tile_ids.append(final_catalog_fileprod.product.Data.TileIndex)
        for tile_id in tile_ids:
            if tile_id in product_type_data.tile_id_dict:
                product_type_data.tile_id_dict[tile_id].append(fileprod)
            else:
                product_type_data.tile_id_dict[tile_id] = [fileprod]
                tile_id_set.add(tile_id)


if len(observation_id_set) == 0:
    logger.error("No observation IDs found.")
    exit()

if len(tile_id_set) == 0:
    logger.error("No tile IDs found.")
    exit()

analysis_filename_dict = {}
reconciliation_filename_dict = {}

# Write Analysis listfiles for the galaxy and star catalogues
for prod_key in (ProdKeys.TUG, ProdKeys.TUS):

    product_type_data = product_type_data_dict[prod_key]
    filename = product_type_data.filename_head + product_type_data.filename_tail
    analysis_filename_dict[prod_key] = filename

    fileprod_list = product_type_data.full_list

    filename_list = [fileprod.filename for fileprod in fileprod_list]

    write_listfile(os.path.join(ROOT_DIR, filename), filename_list)

# Set up Analysis listfiles and ISFs for each observation ID
for obs_id in observation_id_set:

    analysis_valid = True

    # Set up and write the listfiles
    for prod_key, sort_by in ((ProdKeys.MFC, "Data.TileIndex"),
                              (ProdKeys.MSEG, "Data.TileIndex"),
                              (ProdKeys.SESEG, "Data.PointingId"),
                              (ProdKeys.VCF, "Data.ObservationSequence.PointingId"),
                              ):

        product_type_data = product_type_data_dict[prod_key]

        if not obs_id in product_type_data.obs_id_dict:
            # This product isn't present, so skip it and mark as invalid for the analysis pipeline
            analysis_valid = False
            break

        filename = product_type_data.filename_head + str(obs_id) + product_type_data.filename_tail
        analysis_filename_dict[prod_key] = filename

        obs_fileprod_list = product_type_data.obs_id_dict[obs_id]
        obs_fileprod_list.sort(key=lambda fp: get_nested_attr(fp.product, sort_by))

        obs_filename_list = [obs_fileprod.filename for obs_fileprod in obs_fileprod_list]

        write_listfile(os.path.join(ROOT_DIR, filename), obs_filename_list)

    if not analysis_valid:
        continue

    # Set the filename for the VIS Calibrated Frame product and SHE Stacked Segmentation Map product
    analysis_filename_dict[ProdKeys.VSF] = product_type_data_dict[ProdKeys.VSF].obs_id_dict[obs_id][0].filename
    analysis_filename_dict[ProdKeys.SSSEG] = product_type_data_dict[ProdKeys.SSSEG].filename_head + \
        str(obs_id) + product_type_data_dict[ProdKeys.SSSEG].filename_tail

    # Write the ISF for this observation for each variant
    for after_remap in (False, True):
        for with_tu_match in (False, True):

            # Get the filename for this specific variant
            isf_filename = ANALYSIS_ISF_HEAD
            if after_remap:
                isf_filename += AFTER_REMAP_TAG
            if with_tu_match:
                isf_filename += TU_MATCH_TAG
            isf_filename += str(obs_id) + ANALYSIS_ISF_TAIL

            # Write the ISF
            with open(isf_filename, "w") as fo:
                # Write these listfile filenames to the ISF
                for prod_key in ANALYSIS_PRODUCT_KEYS:
                    # Determine whether to write this key or not from the variant
                    met_criteria = True
                    for criteria, variant in ((ONLY_FOR_AFTER_REMAP[prod_key], after_remap),
                                              (ONLY_FOR_TU_MATCH[prod_key], with_tu_match)):
                        if (criteria == 1 and not variant) or (criteria == -1 and variant):
                            met_criteria = False
                    if met_criteria:
                        fo.write(f"{ANALYSIS_ISF_PORTS[prod_key]} = {analysis_filename_dict[prod_key]}\n")
                # Write the fixed product filenames to the ISF
                for l in FIXED_ANALYSIS_ISF_FILENAMES:
                    fo.write(l + "\n")


# Set up Reconciliation listfiles and ISFs for each Tile ID
for tile_id in tile_id_set:

    tile_valid = True

    if not tile_id in product_type_data_dict[ProdKeys.MFC].tile_id_dict:
        continue

    reconciliation_filename_dict[ProdKeys.MFC] = product_type_data_dict[ProdKeys.MFC].tile_id_dict[tile_id][0].filename

    # Set up and write the listfiles
    for prod_key, sort_by in ((ProdKeys.SVM, "Data.ObservationId"),
                              (ProdKeys.SLMC, "Data.ObservationId"),
                              ):

        product_type_data = product_type_data_dict[prod_key]

        if not tile_id in product_type_data.tile_id_dict:
            tile_valid = False
            break

        filename = product_type_data.filename_head + str(tile_id) + product_type_data.filename_tail
        reconciliation_filename_dict[prod_key] = filename

        obs_fileprod_list = product_type_data.tile_id_dict[tile_id]
        obs_fileprod_list.sort(key=lambda fp: get_nested_attr(fp.product, sort_by))

        obs_filename_list = [obs_fileprod.filename for obs_fileprod in obs_fileprod_list]

        write_listfile(os.path.join(ROOT_DIR, filename), obs_filename_list)

    if not tile_valid:
        continue

    # Write the ISF for this tile
    isf_filename = RECONCILIATION_ISF_HEAD + str(tile_id) + RECONCILIATION_ISF_TAIL
    with open(isf_filename, "w") as fo:
        # Write these listfile filenames to the ISF
        for prod_key in RECONCILIATION_PRODUCT_KEYS:
            fo.write(f"{RECONCILIATION_ISF_PORTS[prod_key]} = {reconciliation_filename_dict[prod_key]}\n")
