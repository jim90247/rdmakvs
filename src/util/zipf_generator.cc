#include "util/zipf_generator.h"

#include <cmath>

ZipfGenerator::ZipfGenerator(int n, double alpha, int seed) : gen(seed), distrib(0, 1), n_(n) {
    double c = 0.0;  // normalization constant
    for (int i = 1; i <= n; i++) {
        c += 1.0 / pow(static_cast<double>(i), alpha);
    }
    c = 1.0 / c;

    sum_probs_ = new double[n + 1];
    sum_probs_[0] = 0;
    for (int i = 1; i <= n; i++) {
        sum_probs_[i] = sum_probs_[i - 1] + c / pow(static_cast<double>(i), alpha);
    }
}

ZipfGenerator::~ZipfGenerator() { delete[] sum_probs_; }

int ZipfGenerator::GetNumber() {
    double z = distrib(gen);

    int l = 0, r = n_;
    while (r - l > 1) {
        int m = (l + r) / 2;
        if (sum_probs_[m] > z) {
            r = m;
        } else {
            l = m;
        }
    }

    return l;
}