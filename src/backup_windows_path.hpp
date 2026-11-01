#pragma once

#include <string>
#include <string_view>

namespace chunkdb {

// Namespace prefixes do not change containment; API names retain long-path access.
inline std::wstring BackupWindowsComparisonName(std::wstring_view name) {
    if (name.starts_with(L"\\\\?\\UNC\\")) return L"\\\\" + std::wstring(name.substr(8U));
    if (name.starts_with(L"\\\\?\\")) return std::wstring(name.substr(4U));
    return std::wstring(name);
}

inline std::wstring BackupWindowsApiName(std::wstring_view name, bool explicit_namespace) {
    auto normalized = BackupWindowsComparisonName(name);
    if (!explicit_namespace && normalized.size() < 260U) return normalized;
    if (normalized.starts_with(L"\\\\")) return L"\\\\?\\UNC\\" + normalized.substr(2U);
    return L"\\\\?\\" + normalized;
}

}  // namespace chunkdb
