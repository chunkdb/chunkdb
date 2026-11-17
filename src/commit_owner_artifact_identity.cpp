#include "commit_owner_artifact_identity.hpp"

#ifdef _WIN32
#include <algorithm>
#include <iterator>
#include <system_error>
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

namespace chunkdb {
CommitOwnerNativeIdentity ReadCommitOwnerNativeIdentity(void* native_handle) {
    FILE_ID_INFO information{};
    if (!GetFileInformationByHandleEx(static_cast<HANDLE>(native_handle), FileIdInfo,
            &information, sizeof(information))) {
        throw std::system_error(static_cast<int>(GetLastError()), std::system_category(),
            "read complete owner native identity");
    }
    CommitOwnerNativeIdentity identity; identity.volume = information.VolumeSerialNumber;
    std::copy(std::begin(information.FileId.Identifier), std::end(information.FileId.Identifier), identity.file.begin());
    return identity;
}
}  // namespace chunkdb
#endif
