find_package(nlohmann_json 3.10.2 REQUIRED)

set(SOURCE_FILES
    node_config.h
    node_config.cc
    stats.h
    zipf_generator.h
    zipf_generator.cc
)
add_library(util ${SOURCE_FILES})

target_link_libraries(util PRIVATE nlohmann_json::nlohmann_json)
