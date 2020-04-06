/*
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
*/



#include <algorithm>
#include <cstring>
#include <fstream>
#include <iostream>
#include <libgen.h>
#include <string>

bool verify_stanza(std::istream & stream, const char delim = '\x1e', const char dir = 'd') {
    std::string line;
    if (!std::getline(stream, line)) {
        return false;
    }

    // empt lines are not errors
    if (!line.size()) {
        return true;
    }

    const std::string::size_type first_delim = line.find(delim);
    if (first_delim == std::string::npos) {
        std::cerr << "Stanza missing first delimiter: " << line << std::endl;
        return false;
    }

    // expect to start at a directory
    if (line[first_delim + 1] != dir) {
        std::cerr << "Expected a directory: " << line << std::endl;
        return false;
    }

    // get the path of the parent, removing the trailing slash
    const std::string parent = line.substr(0, first_delim - (line[first_delim - 1] == '/'));

    // followed by a series of non-directories
    while (true) {
        const std::istream::streampos pos = stream.tellg();

        if (!std::getline(stream, line)) {
            break;
        }

        // need at least 23 columns
        const int columns = std::count(line.begin(), line.end(), delim);
        if (columns < 23) {
            std::cerr << "Not enough columns" << std::endl;
            return false;
        }

        // find the first delimiter to get the type
        const std::string::size_type first_delim = line.find(delim);
        if (first_delim == std::string::npos) {
            std::cerr << "Entry missing delimiter: " << line << std::endl;
            return false;
        }

        // stop when a directory is encountered (new stanza)
        if (line[first_delim + 1] == dir) {
            // go back to the beginning of this line
            stream.seekg(pos);
            break;
        }

        // check if the current path is a direct child of the parent path
        std::string::size_type last_slash = line.find_last_of('/', first_delim);

        // entries in root directory don't have any slashes
        if (last_slash == std::string::npos) {
            last_slash = 0;
        }

        if (last_slash != parent.size()) {
            std::cerr << "Bad child: " << line << std::endl;
            return false;
        }
    }

    return true;
}

bool verify_trace(std::istream & stream, const char delim = '\x1e', const char dir = 'd') {
    if (!stream) {
        std::cerr << "Bad stream" << std::endl;
        return false;
    }

    while (verify_stanza(stream, delim, dir));

    // if the loop broke early, then the
    // stream was not completely read, so
    // a good stream is an error
    return !stream;
}

int main(int argc, char * argv[]) {
    if (argc < 2) {
        std::cerr << "Syntax: " << argv[0] << " delim [trace ...]" << std::endl;
        return 0;
    }

    const char delim = argv[1][0];

    for(int i = 2; i < argc; i++) {
        std::ifstream trace(argv[i]);
        if (!trace) {
            std::cerr << "Could not open " << argv[i] << std::endl;
            continue;
        }

        std::cout << argv[i] << ": " << (verify_trace(trace, delim)?"Pass":"Fail")  << std::endl;
    }

    return 0;
}
