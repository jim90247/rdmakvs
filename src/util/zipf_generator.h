#include <random>

class ZipfGenerator {
   public:
    ZipfGenerator(int n, double alpha, int seed = 42);
    virtual ~ZipfGenerator();

    int GetNumber();

   private:
    // disallow these methods
    ZipfGenerator(const ZipfGenerator &);
    ZipfGenerator &operator=(const ZipfGenerator &);

    std::mt19937_64 gen;
    std::uniform_real_distribution<double> distrib;
    int n_;
    double *sum_probs_;  // pre-calculated sum of probabilities
};
