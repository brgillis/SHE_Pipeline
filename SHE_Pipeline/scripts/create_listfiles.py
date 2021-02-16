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
from SHE_PPT.file_io import read_xml_product, write_listfile
from SHE_PPT.products.mer_final_catalog import dpdMerFinalCatalog
from SHE_PPT.products.mer_segmentation_map import dpdMerSegmentationMap
from SHE_PPT.products.she_exposure_segmentation_map import dpdSheExposureReprojectedSegmentationMap
from SHE_PPT.products.she_stack_segmentation_map import dpdSheStackReprojectedSegmentationMap
from SHE_PPT.products.vis_calibrated_frame import dpdVisCalibratedFrame
from SHE_PPT.products.vis_stacked_frame import dpdVisStackedFrame


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
ProductTypeData = namedtuple("ProductTypeData", ["full_list", "sorted_list", "filename_head", "filename_tail"])

# A namedtuple type for a filename and product
FileProduct = namedtuple("FileProduct", ["filename", "product"])

# Init dict of data for each product type
product_type_data = {}
for key in PRODUCT_KEYS:
    product_type_data = ProductTypeData([],
                                        [],
                                        FILENAME_HEADS[key],
                                        FILENAME_TAILS[key])

# Get all existing products, read them in, and sort them depending on type

all_filenames = os.listdir(ROOT_DIR)

for filename in all_filenames:

    if not filename[-4] == ".xml":
        continue

    product = read_xml_product(filename, workdir=ROOT_DIR)
