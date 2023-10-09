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

``gufi_ls``
===========

``gufi_ls`` is the equivalent of the POSIX command ls applied to a GUFI
tree. As with ``gufi_find``, there are a multitude of options available
listed below

Flags
-----

  ``--help`` 
    Displays help menu                      

  ``-v``, ``--version`` 
    Show program’s version number and exit  

  ``-a``, ``--all``
    Do not ignore entries ending with ``.``    

  ``-A``, ``--almost-all``
    Do not list implied ``.`` and ``..``            

  ``--block-size <block_size>``
    With -l, scale sizes by block_size when printing them                           

  ``-B``, ``--ignore-backups``
    Do not list implied entries ending with 

  ``-G``, ``--no-group``
    In a long listing, don’t print group names                                   

  ``-i``, ``--inode``
    Print the index number of each file     

  ``-l``
    Use a long listing format              

  ``-r``, ``--reverse``         
    Reverse order while sorting             

  ``-R``, ``--recursive``
    List sub-directories recursively        

  ``-s``, ``--size``
    Print the allocated size of each file in blocks                               

  ``-S``                        
    Sort by file size, largest first        

  ``--time-style <TIME_STYLE>``
    Time/date format with ``-l``                

  ``-t``
    Sort by modification time, newest first 

  ``-U``
    Do not sort; list entries in directory order                                   

| Additionally, several GUFI extensions have been added:

  ``--delim <c``               
    delimiter separating output columns       

  ``--in-memory-name <name>``  
    Name of in-memory database when ``-R`` is used                                      

  ``--aggregate-name <name>``  
    Name of final database when ``-R`` is used    

  ``--nlink-width <chars>``    
    Width of nlink column                     

  ``--size-width <chars>``     
    Width of size column                      

  ``--user-width <chars>``     
    Width of user column                      

  ``--group-width <chars>``    
    Width of group column                     

  ``--skip-file <filename>``   
    Name of file containing directory basenames to skip                         
