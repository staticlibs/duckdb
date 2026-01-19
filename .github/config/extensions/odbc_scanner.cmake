if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(odbc_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/odbc-scanner
            GIT_TAG b22616480a0c43e704e9ac8572adf7ced305be2d
            )
endif()