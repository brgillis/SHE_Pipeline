""" @file magic_values.py

    Created 14 may 2019

    Magic values for the package definitions
"""

__updated__ = "2019-08-21"

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

import SHE_CTE
import SHE_GST
import SHE_MER_RemapMosaic
import SHE_PSFToolkit
import SHE_Pipeline

ERun_CTE = "E-Run SHE_CTE " + SHE_CTE.__version__ + " "
ERun_MER = "E-Run SHE_MER " + SHE_MER_RemapMosaic.__version__ + " "
ERun_GST = "E-Run SHE_GST " + SHE_GST.__version__ + " "
ERun_PSF = "E-Run SHE_PSFToolkit " + SHE_PSFToolkit.__version__ + " "
ERun_Pipeline = "E-Run SHE_Pipeline " + SHE_Pipeline.__version__ + " "
