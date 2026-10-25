#include <exception>
#include <iostream>

#include "budget_compare.hpp"

int main(int argc, char** argv) {
    namespace budget = chunkdb::budget_compare;
    try {
        return budget::Main(budget::CommandLineArguments(argc, argv), budget::SystemHooks(), std::cout, std::cerr);
    } catch (const std::exception& e) {
        std::cerr << "chunkdb_budget_compare: " << e.what() << "\n";
        return 1;
    }
}
