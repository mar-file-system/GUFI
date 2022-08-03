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
#include <map>
#include <string>

// directory -> <inode, parent inode>
typedef std::map <std::string, std::pair<std::string, std::string> > Tree;

static bool verify_stanza(std::istream & stream, Tree & tree, const char delim = '\x1e', const char dir = 'd') {
    std::string line;
    if (!std::getline(stream, line)) {
        return false;
    }

    // empty lines are not errors
    if (!line.size()) {
        return true;
    }

    // there should be at least one delimiter
    const std::string::size_type first_delim = line.find(delim);
    if (first_delim == std::string::npos) {
        std::cerr << "Error: Stanza missing first delimiter: " << line << std::endl;
        return false;
    }

    // need at least 23 columns
    const int columns = std::count(line.begin(), line.end(), delim);
    if (columns < 23) {
        std::cerr << "Error: Not enough columns: " << line << std::endl;
        return false;
    }

    // expect to start at a directory
    if (line[first_delim + 1] != dir) {
        std::cerr << "Error: Expected a directory: " << line << std::endl;
        return false;
    }

    std::string::size_type len = first_delim;
    // remove trailing slashes
    while (len && (line[len] == '/')) {
        len--;
    }

    const std::string parent = line.substr(0, len);

    // find the inode (1 column after type)
    const std::string::size_type second_delim = first_delim + 2;
    const std::string::size_type third_delim = line.find(delim, second_delim + 1);
    const std::string inode = line.substr(second_delim + 1, third_delim - second_delim - 1);

    // find the pinode
    const std::string::size_type last_delim = line.find_last_of(delim);
    if (last_delim != (line.size() - 1)) {
        std::cerr << "Warning: Trailing characters present" << std::endl;
    }

    const std::string::size_type pinode_delim = line.find_last_of(delim, last_delim - 1);
    if (pinode_delim <= (first_delim + 2)) {
        std::cerr << "Error: Could not find parent pinode: " << line << std::endl;
        return false;
    }

    const std::string pinode = line.substr(pinode_delim + 1, last_delim - pinode_delim - 1);
    if (!std::all_of(pinode.begin(), pinode.end(), ::isdigit)) { // don't use std::isdigit since it takes an int, but *it is a char
        std::cerr << "Error: Bad parent pinode: " << line << std::endl;
        return false;
    }

    if (tree.find(parent) != tree.end()) {
        std::cerr << "Error: Path reappears in trace: " << parent << std::endl;
        return false;
    }

    tree[parent] = std::make_pair(inode, pinode);

    // followed by a series of non-directories
    while (true) {
        const std::streampos pos = stream.tellg();

        if (!std::getline(stream, line)) {
            break;
        }

        // empty lines are not errors
        if (!line.size()) {
            continue;
        }

        // there should be at least one delimiter
        const std::string::size_type first_delim = line.find(delim);
        if (first_delim == std::string::npos) {
            std::cerr << "Error: Entry missing delimiter: " << line << std::endl;
            return false;
        }

        // need at least 23 columns
        const int columns = std::count(line.begin(), line.end(), delim);
        if (columns < 23) {
            std::cerr << "Error: Not enough columns: " << line << std::endl;
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

        if ((last_slash != parent.size()) ||
            (line.substr(0, last_slash) != parent)) {
            std::cerr << "Error: Bad child: " << line << std::endl;
            return false;
        }

        const std::string::size_type last_delim = line.find_last_of(delim);
        if (last_delim != (line.size() - 1)) {
            std::cerr << "Warning: Trailing characters present" << std::endl;
        }

        const std::string::size_type pinode_delim = line.find_last_of(delim, last_delim - 1);
        if (pinode_delim <= (first_delim + 2)) {
            std::cerr << "Error: Could not find child pinode: " << line << std::endl;
            return false;
        }

        // make sure the child pinode is 0
        const std::string pinode = line.substr(pinode_delim + 1, last_delim - pinode_delim - 1);
        if (pinode != "0") {
            std::cerr << "Error: Bad child pinode: " << line << std::endl;
            return false;
        }
    }

    return true;
}

static std::string dirname(const std::string & path) {
    std::string::size_type len = path.size();

    // remove trailing slashes
    while (len && (path[len - 1] == '/')) {
        len--;
    }

    // find parent (remove basename)
    while (len && (path[len - 1] != '/')) {
        len--;
    }

    // remove trailing slashes
    while (len && (path[len - 1] == '/')) {
        len--;
    }

    return path.substr(0, len);
}

static std::size_t complete_tree(const Tree & tree) {
    std::size_t bad = 0;
    for(std::pair <const std::string,
                   std::pair<std::string,
                             std::string> > const & dir : tree) {
        const std::string parent_path = dirname(dir.first);

        // root
        if ((parent_path == "") || (parent_path == dir.first)) {
            continue;
        }

        Tree::const_iterator parent = tree.find(parent_path);
        if (parent == tree.end()) {
            std::cerr << "Parent path of \"" << dir.first << "\" missing." << std::endl;
            bad++;
            continue;
        }

        if (parent->second.first != dir.second.second) {
            std::cerr << "Parent inode does not match directory's pinode: \"" << dir.first << "\"" << std::endl;
            bad++;
            continue;
        }
    }

    return bad;
}

static bool verify_trace(std::istream & stream, const char delim = '\x1e', const char dir = 'd') {
    if (!stream) {
        std::cerr << "Bad stream" << std::endl;
        return false;
    }

    Tree tree;

    while (verify_stanza(stream, tree, delim, dir));

    const std::size_t bad = complete_tree(tree);
    if (bad) {
        std::cerr << bad << " directories either missing or mismatched" << std::endl;
    }

    // if the loop broke early, then the
    // stream was not completely read, so
    // a good stream is an error
    return (!stream) && (bad == 0);
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

        std::cout << argv[i] << ": " << std::flush;
        std::cout << (verify_trace(trace, delim)?"Pass":"Fail") << std::endl;
    }

    return 0;
}
