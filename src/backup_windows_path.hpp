#pragma once

#include <string>
#include <string_view>
#include <stdexcept>
#include <vector>

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

// Parse native roots directly: older Windows standard libraries do not
// recognize UNC root names or extended namespace prefixes consistently.
struct BackupWindowsPathParts {
    std::wstring root;
    std::vector<std::wstring> components;

    std::wstring Name() const {
        auto result = root;
        for (const auto& component : components) {
            if (result.back() != L'\\') result += L'\\';
            result += component;
        }
        return result;
    }
};

inline BackupWindowsPathParts SplitBackupWindowsPath(std::wstring_view name) {
    auto native = BackupWindowsComparisonName(name);
    for (auto& ch : native) if (ch == L'/') ch = L'\\';
    BackupWindowsPathParts result;
    std::size_t at = 0U;
    if (native.size() >= 3U && native[1] == L':' && native[2] == L'\\' &&
        ((native[0] >= L'A' && native[0] <= L'Z') || (native[0] >= L'a' && native[0] <= L'z'))) {
        result.root = native.substr(0U, 3U);
        at = 3U;
    } else if (native.starts_with(L"\\\\")) {
        const auto server_end = native.find(L'\\', 2U);
        if (server_end == std::wstring::npos || server_end == 2U)
            throw std::invalid_argument("backup UNC path has no share");
        const auto share_end = native.find(L'\\', server_end + 1U);
        const auto end = share_end == std::wstring::npos ? native.size() : share_end;
        if (end == server_end + 1U) throw std::invalid_argument("backup UNC path has no share");
        result.root = native.substr(0U, end) + L'\\';
        at = end;
    } else {
        throw std::invalid_argument("backup path has no absolute Windows root");
    }
    while (at < native.size()) {
        if (native[at] == L'\\') { ++at; continue; }
        const auto end = native.find(L'\\', at);
        const auto component = native.substr(at, end == std::wstring::npos ? end : end - at);
        if (component == L"..") {
            if (!result.components.empty()) result.components.pop_back();
        } else if (component != L".") {
            result.components.push_back(component);
        }
        if (end == std::wstring::npos) break;
        at = end + 1U;
    }
    return result;
}

inline std::wstring BackupWindowsParentApiName(std::wstring_view absolute) {
    auto parts = SplitBackupWindowsPath(absolute);
    if (!parts.components.empty()) parts.components.pop_back();
    return BackupWindowsApiName(parts.Name(), absolute.starts_with(L"\\\\?\\"));
}

}  // namespace chunkdb
