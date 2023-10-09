..
    This file is part of GUFI, which is part of MarFS, which is released
    under the BSD license.


    Copyright (c) 2017, Los Alamos National Security (LANS), LLC
    All rights reserved.

    Redistribution and use in source and binary forms, with or without modification,
    are permitted provided that the following conditions are met:

    1. Redistributions of source code must retain the above copyright notice, this
    list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright notice,
    this list of conditions and the following disclaimer in the documentation and/or
    other materials provided with the distribution.

    3. Neither the name of the copyright holder nor the names of its contributors
    may be used to endorse or promote products derived from this software without
    specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
    ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
    WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
    IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
    INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
    LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
    OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
    ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


    From Los Alamos National Security, LLC:
    LA-CC-15-039

    Copyright (c) 2017, Los Alamos National Security, LLC All rights reserved.
    Copyright 2017. Los Alamos National Security, LLC. This software was produced
    under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
    Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
    the U.S. Department of Energy. The U.S. Government has rights to use,
    reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
    ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
    ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
    modified to produce derivative works, such modified software should be
    clearly marked, so as not to confuse it with the version available from
    LANL.

    THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
    THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
    ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
    CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
    EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
    OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
    INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
    CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
    IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
    OF SUCH DAMAGE.

``gufi_stats``
==============

``gufi_stats`` is used to analyze a tree and retrieve statistics from an
index. ``gufi_stat`` does not have an analogous standard utility.

Flags
-----

``gufi_stats`` is called with at minimum one positional argument
specifying the statistic desired. Each statistic has a default version,
Most also have a more in-depth version that is specified by their
category: recursive, cumulative, or other.

  ``--help``
    Displays help menu                        

  ``--version``, ``-v``            
    Display program’s version number and exits                                     

  ``--recursive``, ``-r``          
    Run command recursively                   

  ``--cumulative``, ``-c``         
    Return cumulative values                  

  ``--order <order>``          
    Sort output (if applicable)               

  ``--delim <c>``              
    Delimeter separating output columns       

  ``--num-results <n>``        
    First n results                           

  ``--uid <u>``, ``--user <u>``    
    Restrict to user                          

  ``--in-memory-name <name>``  
    Name of intermediate database             
    
  ``--aggregate-name <name>``  
    Name of final database                    

  ``--skip-file <filename>``   
    Name of file containing directory basenames to skip                         

Recursive
~~~~~~~~~

| These statistics will run a computation on a single directory.
  Specifying ``-r`` or ``--recursive`` will recursively descend the
  starting directory and return each subdirectory’s statistic.

  ``depth``       
    Get the depths of the provided directory relative to the root directory                                     

  ``filesize``    
    Get the size the files in the immediate directory      

  ``filecount``   
    Get the number of files in the immediate directory     

  ``linkcount``   
    Get the number of links in the immediate directory     

  ``dircount``    
    Get the number of directories in the immediate directory                                              

  ``leaf-dirs``   
    Get the leaf directories immediately under the current directory                                              

  ``leaf-depth``  
    Get the depth of the leaf directories immediately under the current directory                            

  ``leaf-files``  
    Get number of files in the leaf directories immediately under the current directory                

  ``leaf-links``  
    Get number of links in the leaf directories immediately under the current directory                

Cumulative
~~~~~~~~~~

| These statistics will run a computation on an entire subtree. By
  default, the results will be grouped together by UID. Specifying
  ``-c`` or ``--cumulative`` will combine all of the results into a
  single sum.

  ``total-filesize``      
    Get the total size taken up by the entire directory                                      

  ``total-filecount``     
    Get the total number of files under the directory                                      

  ``total-linkcount``     
    Get the total number of links under the directory                                      

  ``total-dircount``      
    Get the total number of directories under the directory                                      

  ``total-leaf-files``    
    Get total number of files in the leaf directories under this directory               

  ``total-leaf-links``    
    Get total number of links in the leaf directories under this directory               

  ``files-per-level``
    Get counts of how many files are in each level of the tree                                    

  ``links-per-level``
    Get counts of how many links are in each level of the tree                                    

  ``dirs-per-level``
    Get counts of how many dirs are in each level of the tree                                    

  ``average-leaf-files``  
    Get average number of leaf files under the provided directory                             

  ``average-leaf-links``  
    Get average number of leaf links under the provided directory                             

Other
~~~~~

| Statistics under the “other” category do not have extra flags
  associated with them.

  ``median-leaf-files``
    Get median number of leaf files under the provided directory

  ``duplicate-names``
    Find files with matching names and sizes
