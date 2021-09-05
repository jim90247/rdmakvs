#include <map>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

using nlohmann::json;

enum NodeType { NT_SERVER, NT_CLIENT, NT_UNKNOWN };

NLOHMANN_JSON_SERIALIZE_ENUM(NodeType,
                             {{NT_UNKNOWN, nullptr}, {NT_SERVER, "server"}, {NT_CLIENT, "client"}})

struct NodeConfig {
    std::string endpoint;  // zmq endpoint
    NodeType type;         // server or client
    int threads;           // threads of this instance
};

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(NodeConfig, endpoint, type, threads)

std::map<std::string, NodeConfig> ParseNodeConfig(std::string path);
