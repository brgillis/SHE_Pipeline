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
import pytest

import os
import multiprocessing
import SHE_Pipeline.pipeline_utilities as pu

class TestPipelineUtilities():
    """ Unit tests for functions in run_bias_parallel
    """
    
    # Test main functions 

    def test_external_process_run1(self):
        """ A simple command without any standard output or error
        
        
        """

        # Test external process, with no logging..., no stderr
        fileName='temp_njc12345.dat'
        if os.path.exists(fileName):
            os.remove(fileName)
        cmd="echo '33' > %s" % fileName
        
        pu.external_process_run(cmd)
        if not os.path.exists(fileName):
            assert False
        lines=open('temp.dat').readlines()
        assert len(lines)==1 
        assert lines[0].startswith('33')
        pass


    def test_external_process_run2(self):
        """ A command with some standard output, but no standard error
        
        
        """

        # Test external process, with no logging..., no stderr
        cmd="E-Run SHE_Pipeline 0.3 python3 SHE_Pipeline/tests/python/external_process_creator.py --standout stdout"
        
        stdOut,_stdErr=pu.external_process_run(cmd)
        assert 'Testing stdout' in str(stdOut[0])
        pass

    def test_external_process_run3(self):
        """ A command with some standard error, but not raised on error
        
        
        """

        # Test external process, with no logging..., no stderr
        cmd="E-Run SHE_Pipeline 0.3 python3 SHE_Pipeline/tests/python/external_process_creator.py --standout stderr"
        
        _stdOut,stdErr=pu.external_process_run(cmd,raiseOnError=False)
        assert 'Testing stderr' in str(stdErr[-1])
        pass
    
    
    def test_external_process_run4(self):
        """ A command with some standard error, 
        raised on error
        
        """

        # Test external process, with no logging..., no stderr
        cmd="E-Run SHE_Pipeline 0.3 python3 SHE_Pipeline/tests/python/external_process_creator.py --standout stderr"
        
        try:
            _stdOut,_stdErr=pu.external_process_run(cmd,raiseOnError=True)
        except:
            assert True
            return
        assert False
        pass
    
    def test_external_process_run5(self):
        """ A command with some standard error, 
        raised on error
        
        """

        # Test external process, with no logging..., no stderr
        cmd="E-Run SHE_Pipeline 0.3 python3 SHE_Pipeline/tests/python/external_process_creator.py --standout both"
        
        try:
            stdOut,stdErr=pu.external_process_run(cmd,raiseOnError=False)
        except:
            assert False
        assert 'Testing stderr' in str(stdErr[-1])
        assert 'Testing stdout' in str(stdOut[0])
        
        pass

    # Additional tests for other features???
    
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
        
        fileNameList=[]
        prodThreads=[]
        for ii in range(2):
            fileName='output2134_%s.dat' % ii
            if os.path.exists(fileName):
                os.remove(fileName)
            fileNameList.append(fileName)
            prodThreads.append(multiprocessing.Process(
                target=self.simpleFunction,
                args=(ii,)))
        if prodThreads:
            try:
                pu.runThreads(prodThreads)
            except Exception as e:
                # But still seems to do it...
                if '<ERROR>' in e:
                    assert False
        for ii,fileName in enumerate(fileNameList):
            assert os.path.exists(fileName)
            lines=open(fileName).readlines()
            assert str(lines[0]).startswith(str(ii))
        
        pass
    
    def test_run_threads2(self):
        """ Test run_threads on simple set processes,
        where exceptions are raised.
        """
        
        fileNameList=[]
        prodThreads=[]
        for ii in range(2):
            fileName='output2134_%s.dat' % ii
            if os.path.exists(fileName):
                os.remove(fileName)
            raiseException = ii==1
            fileNameList.append(fileName)
            prodThreads.append(multiprocessing.Process(
                target=self.simpleFunction,
                args=(ii,raiseException)))
        if prodThreads:
            try:
                pu.runThreads(prodThreads)
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
        
        fileNameList=[]
        prodThreads=[]
        for ii in range(num_threads):
            fileName='output2134_%s.dat' % ii
            if os.path.exists(fileName):
                os.remove(fileName)
            raiseException = ii==1
            fileNameList.append(fileName)
            prodThreads.append(multiprocessing.Process(
                target=self.simpleFunction,
                args=(ii,raiseException)))
        if prodThreads:
            try:
                pu.runThreads(prodThreads)
            except Exception as e:
                # But still seems to do it...
                if '<ERROR>' in e:
                    assert True
         
        pass


