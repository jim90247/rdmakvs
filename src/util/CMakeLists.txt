find_package(nlohmann_json 3.10.2 REQUIRED)

add_library(util node_config.h node_config.cc)

target_link_libraries(util PRIVATE nlohmann_json::nlohmann_json)
