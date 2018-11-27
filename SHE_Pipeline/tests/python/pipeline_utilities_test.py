""" @file sample_test.py

    Created 16 August 2018
    
    Sample unit test
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
# the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
# Boston, MA 02110-1301 US

import logging
import multiprocessing
import os

import pytest

import SHE_Pipeline.pipeline_utilities as pu


class TestPipelineUtilities():
    """ Unit tests for functions in run_bias_parallel
    """
    
    # Test main functions 
    
    def simpleFunction(self, threadNo, raiseException=False):
        """ 
        
        """
        if raiseException:
            raise Exception("Test Exception") 
        lines=['%s\n' % threadNo]
        open('output2134_%s.dat' % (threadNo),'w').writelines(lines)
        return

    def test_run_threads1(self):
        """ Test run_threads on simple set of multiple processes
        """
        
        filename_list=[]
        prod_threads=[]
        for ii in range(2):
            filename='output2134_%s.dat' % ii
            if os.path.exists(filename):
                os.remove(filename)
            filename_list.append(filename)
            prod_threads.append(multiprocessing.Process(
                target=self.simpleFunction,
                args=(ii,)))
        if prod_threads:
            try:
                pu.run_threads(prod_threads)
            except Exception as e:
                # But still seems to do it...
                if '<ERROR>' in e:
                    assert False
        for ii,filename in enumerate(filename_list):
            assert os.path.exists(filename)
            lines=open(filename).readlines()
            assert str(lines[0]).startswith(str(ii))
        
        pass
    
    def test_run_threads2(self):
        """ Test run_threads on simple set processes,
        where exceptions are raised.
        """
        
        filename_list=[]
        prod_threads=[]
        for ii in range(2):
            filename='output2134_%s.dat' % ii
            if os.path.exists(filename):
                os.remove(filename)
            raiseException = ii==1
            filename_list.append(filename)
            prod_threads.append(multiprocessing.Process(
                target=self.simpleFunction,
                args=(ii,raiseException)))
        if prod_threads:
            try:
                pu.run_threads(prod_threads)
            except Exception as e:
                if '<ERROR>' in e:
                    assert True
                else:
                    assert False
        #for ii,fileName in enumerate(fileNameList):
        #    assert os.path.exists(fileName)
        #    lines=open(fileName).readlines()
        #    assert str(lines[0]).startswith(str(ii))
        
        pass



    def test_run_threads3(self):
        """ Test run with more threads than allowed.
        """
        
        num_threads = multiprocessing.cpu_count()+3
        
        filename_list=[]
        prod_threads=[]
        for ii in range(num_threads):
            filename='output2134_%s.dat' % ii
            if os.path.exists(filename):
                os.remove(filename)
            raiseException = ii==1
            filename_list.append(filename)
            prod_threads.append(multiprocessing.Process(
                target=self.simpleFunction,
                args=(ii,raiseException)))
        if prod_threads:
            try:
                pu.run_threads(prod_threads)
            except Exception as e:
                # But still seems to do it...
                if '<ERROR>' in e:
                    assert True
         
        pass


