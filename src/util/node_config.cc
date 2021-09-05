#include "util/node_config.h"

#include <fstream>
#include <map>
#include <string>
#include <vector>

std::map<std::string, NodeConfig> ParseNodeConfig(std::string path) {
    std::ifstream ifs(path);
    json j;
    ifs >> j;
    ifs.close();

    std::map<std::string, NodeConfig> m;

    for (auto& node : j.items()) {
        std::string nid = node.key();
        NodeConfig nconf = node.value().get<NodeConfig>();
        m[nid] = nconf;
    }

    return m;
}
