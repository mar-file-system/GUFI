cmake_minimum_required(VERSION 3.0.0)

# copy test scripts into the test directory within the build directory
# list these explicitly to prevent random garbage from getting in
foreach(SCRIPT
    bfwiflat2gufitest
    dfw2gufitest
    generatetree
    gitest.py
    gufitest.py
    robinhoodin
    runbffuse
    runbfmi
    runbfq
    runbfqforfuse
    runbfti
    runbfwi
    runbfwreaddirplus2db
    runbfwreaddirplus2db.orig
    rundfw
    rungenuidgidsummaryavoidentriesscan
    rungroupfilespacehog
    rungroupfilespacehogusesummary
    runlistschemadb
    runlisttablesdb
    runoldbigfiles
    runquerydb
    runquerydbn
    runtests
    runuidgidsummary
    runuidgidummary
    runuserfilespacehog
    runuserfilespacehogusesummary
    verifyknowntree)
  # copy the scropt into the build directory for easy access
  configure_file(${SCRIPT} ${CMAKE_CURRENT_BINARY_DIR}/${SCRIPT} COPYONLY)
endforeach()

# recurse down into the googletest subdirectory
add_subdirectory(googletest)
