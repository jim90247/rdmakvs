#include <gflags/gflags.h>
#include <glog/logging.h>

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    return 0;
}
