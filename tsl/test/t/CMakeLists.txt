set(PROVE_TEST_FILES
    001_simple_multinode.pl
    002_multinode_standby.pl
)

foreach(P_FILE ${PROVE_TEST_FILES})
    configure_file(${P_FILE} ${CMAKE_CURRENT_BINARY_DIR}/${P_FILE} COPYONLY)
endforeach(P_FILE)
