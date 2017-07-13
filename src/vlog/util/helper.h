#ifndef ML_UTILS_H
#define ML_UTILS_H

#include <iostream>
#include <vector>

class Utils {

    public:
        static double array_sum(double arr[], int len);

        static double *array_pow(double arr[], int len, int power);

        static double *array_multiplication(double arr1[], double arr2[], int len);

        static double *array_diff(double arr1[], double arr2[], int len);

//        template <typename T>
        static std::string stringify(const std::vector<double>& v);

};

#endif //ML_UTILS_H
