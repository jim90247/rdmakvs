#include <random>

class ZipfGenerator {
   public:
    ZipfGenerator(int n, double alpha, int seed = 42);
    ZipfGenerator(const ZipfGenerator &) = delete;
    ZipfGenerator &operator=(const ZipfGenerator &) = delete;
    virtual ~ZipfGenerator();

    int GetNumber();

   private:
    std::mt19937_64 gen;
    std::uniform_real_distribution<double> distrib;
    int n_;
    double *sum_probs_;  // pre-calculated sum of probabilities
};
