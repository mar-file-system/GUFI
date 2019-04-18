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

    const std::string::size_type first_delim = line.find(delim);
    if (first_delim == std::string::npos) {
        std::cerr << "Missing delimiter: " << line << std::endl;
        return false;
    }

    // expect to start at a directory
    if (line[first_delim + 1] != dir) {
        std::cerr << "Expected a directory: " << line << std::endl;
        return false;
    }

    const std::string parent = line.substr(0, first_delim - (line[first_delim - 1] == '/'));

    // followed by a series of non-directories
    while (true) {
        const std::istream::streampos pos = stream.tellg();

        if (!std::getline(stream, line)) {
            break;
        }

        const std::string::size_type first_delim = line.find(delim);
        if (first_delim == std::string::npos) {
            std::cerr << "Missing delimiter: " << line << std::endl;
            return false;
        }

        // stop when a directory is encountered (new stanza)
        if (line[first_delim + 1] == dir) {
            // go back to the beginning of this line
            stream.seekg(pos);
            break;
        }

        // check if the current path is a direct child of the parent path
        char * buf = new char[first_delim + 1]();
        memcpy(buf, line.c_str(), first_delim);
        char * bufdir = dirname(buf);
        const std::size_t len = strlen(bufdir);

        if ((parent.size() != len)         ||
            parent.compare(0, len, bufdir)) {
            delete [] buf;
            std::cerr << "Bad child: " << line << std::endl;
            return false;
        }

        delete [] buf;
    }

    return true;
}

bool verify_trace(std::istream & stream, const char delim = '\x1e', const char dir = 'd') {
    if (!stream) {
        return false;
    }

    while (verify_stanza(stream, delim, dir));
    return !stream;
}

int main(int argc, char * argv[]) {
    if (argc < 1) {
        std::cerr << "Syntax: " << argv[0] << " [trace ...]" << std::endl;
        return 0;
    }

    for(int i = 1; i < argc; i++) {
        std::ifstream trace(argv[i]);
        if (!trace) {
            std::cerr << "Could not open " << argv[i] << std::endl;
            continue;
        }

        std::cout << argv[i] << " " << (verify_trace(trace)?"Pass":"Fail")  << std::endl;
    }

    return 0;
}
