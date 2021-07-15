set(PERL_FILES AccessNode.pm DataNode.pm TimescaleNode.pm)

foreach(P_FILE ${PERL_FILES})
  configure_file(${P_FILE} ${CMAKE_CURRENT_BINARY_DIR}/${P_FILE} COPYONLY)
endforeach(P_FILE)

# Check if PostgreSQL was compiled with --enable-tap-tests
if(EXISTS "${PG_PKGLIBDIR}/pgxs/src/test/perl")
  install(FILES ${PERL_FILES} DESTINATION "${PG_PKGLIBDIR}/pgxs/src/test/perl")
endif()
