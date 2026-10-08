#pragma once

#include <string_view>

#include "chunkdb/table_catalog.hpp"

namespace chunkdb {

// Sets the table option named `key`, as TABLEINFO prints it, from `value`.
// Throws std::invalid_argument for an unknown option, a geometry setting
// (fixed when a table is created) or a value the option does not take.
void ApplyTableOption(TableOptionsUpdate* update, std::string_view key, std::string_view value);

}  // namespace chunkdb
