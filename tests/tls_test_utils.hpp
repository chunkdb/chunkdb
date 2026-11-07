#pragma once

#ifdef CHUNKDB_WITH_OPENSSL

#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

namespace chunkdb::test {

// A self-signed certificate and its private key, both PEM.
struct TestTlsMaterial {
    std::string cert_pem;
    std::string key_pem;
};

namespace tls_detail {

template <typename T, void (*Free)(T*)>
struct Deleter {
    void operator()(T* object) const noexcept { Free(object); }
};
template <typename T, void (*Free)(T*)>
using Owned = std::unique_ptr<T, Deleter<T, Free>>;

inline void FreeGeneralNames(GENERAL_NAMES* names) { GENERAL_NAMES_free(names); }

// Throws with `what` and every queued OpenSSL error, which leaves the queue empty.
[[noreturn]] inline void ThrowOpenSslError(const std::string& what) {
    std::string message = what;
    for (unsigned long code = ERR_get_error(); code != 0; code = ERR_get_error()) {
        char text[256];
        ERR_error_string_n(code, text, sizeof(text));
        message += ": ";
        message += text;
    }
    throw std::runtime_error(message);
}

inline void Check(bool ok, const char* what) {
    if (!ok) ThrowOpenSslError(what);
}

// RSA, so that ECDHE-RSA cipher suites are available over TLS 1.2.
inline Owned<EVP_PKEY, EVP_PKEY_free> NewRsaKey() {
    const Owned<EVP_PKEY_CTX, EVP_PKEY_CTX_free> context(EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr));
    Check(context != nullptr, "EVP_PKEY_CTX_new_id failed");
    Check(EVP_PKEY_keygen_init(context.get()) == 1, "EVP_PKEY_keygen_init failed");
    Check(EVP_PKEY_CTX_set_rsa_keygen_bits(context.get(), 2048) == 1, "EVP_PKEY_CTX_set_rsa_keygen_bits failed");
    EVP_PKEY* key = nullptr;
    Check(EVP_PKEY_keygen(context.get(), &key) == 1, "EVP_PKEY_keygen failed");
    return Owned<EVP_PKEY, EVP_PKEY_free>(key);
}

inline std::string MemoryBioText(BIO* bio) {
    char* data = nullptr;
    const long size = BIO_get_mem_data(bio, &data);
    Check(size > 0 && data != nullptr, "BIO_get_mem_data failed");
    return std::string(data, static_cast<std::size_t>(size));
}

inline TestTlsMaterial GenerateTestTlsMaterial() {
    const auto key = NewRsaKey();
    const Owned<X509, X509_free> certificate(X509_new());
    Check(certificate != nullptr, "X509_new failed");
    Check(X509_set_version(certificate.get(), 2) == 1, "X509_set_version failed");
    Check(ASN1_INTEGER_set(X509_get_serialNumber(certificate.get()), 1) == 1, "ASN1_INTEGER_set failed");
    // Valid from a day ago to a day ahead, so clock differences do not matter.
    constexpr long kDaySeconds = 24L * 60L * 60L;
    Check(X509_gmtime_adj(X509_getm_notBefore(certificate.get()), -kDaySeconds) != nullptr,
          "X509_gmtime_adj failed");
    Check(X509_gmtime_adj(X509_getm_notAfter(certificate.get()), kDaySeconds) != nullptr, "X509_gmtime_adj failed");
    Check(X509_set_pubkey(certificate.get(), key.get()) == 1, "X509_set_pubkey failed");
    // The name is built apart and copied in: OpenSSL 4 returns a certificate's
    // names as const.
    const Owned<X509_NAME, X509_NAME_free> name(X509_NAME_new());
    Check(name != nullptr, "X509_NAME_new failed");
    Check(X509_NAME_add_entry_by_txt(name.get(), "CN", MBSTRING_ASC,
                                     reinterpret_cast<const unsigned char*>("127.0.0.1"), -1, -1, 0) == 1,
          "X509_NAME_add_entry_by_txt failed");
    Check(X509_set_subject_name(certificate.get(), name.get()) == 1, "X509_set_subject_name failed");
    Check(X509_set_issuer_name(certificate.get(), name.get()) == 1, "X509_set_issuer_name failed");
    {
        const Owned<GENERAL_NAMES, FreeGeneralNames> alt_names(sk_GENERAL_NAME_new_null());
        Check(alt_names != nullptr, "sk_GENERAL_NAME_new_null failed");
        Owned<GENERAL_NAME, GENERAL_NAME_free> entry(GENERAL_NAME_new());
        Check(entry != nullptr, "GENERAL_NAME_new failed");
        ASN1_OCTET_STRING* address = a2i_IPADDRESS("127.0.0.1");
        Check(address != nullptr, "a2i_IPADDRESS failed");
        GENERAL_NAME_set0_value(entry.get(), GEN_IPADD, address);
        Check(sk_GENERAL_NAME_push(alt_names.get(), entry.get()) > 0, "sk_GENERAL_NAME_push failed");
        (void)entry.release(); // Owned by alt_names now.
        Check(X509_add1_ext_i2d(certificate.get(), NID_subject_alt_name, alt_names.get(), 0, X509V3_ADD_DEFAULT) == 1,
              "X509_add1_ext_i2d failed");
    }
    Check(X509_sign(certificate.get(), key.get(), EVP_sha256()) > 0, "X509_sign failed");

    const Owned<BIO, BIO_free_all> cert_out(BIO_new(BIO_s_mem()));
    const Owned<BIO, BIO_free_all> key_out(BIO_new(BIO_s_mem()));
    Check(cert_out != nullptr && key_out != nullptr, "BIO_new failed");
    Check(PEM_write_bio_X509(cert_out.get(), certificate.get()) == 1, "PEM_write_bio_X509 failed");
    Check(PEM_write_bio_PrivateKey(key_out.get(), key.get(), nullptr, nullptr, 0, nullptr, nullptr) == 1,
          "PEM_write_bio_PrivateKey failed");
    return {.cert_pem = MemoryBioText(cert_out.get()), .key_pem = MemoryBioText(key_out.get())};
}

inline void WriteFile(const std::filesystem::path& path, const std::string& contents) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("failed to open file for writing: " + path.string());
    out.write(contents.data(), static_cast<std::streamsize>(contents.size()));
    if (!out) throw std::runtime_error("failed to write file: " + path.string());
}

}  // namespace tls_detail

// A self-signed RSA 2048 certificate for 127.0.0.1 (common name and IP
// subject alternative name), valid from a day ago to a day ahead, signed with
// SHA-256, and its unencrypted PKCS#8 key. Generated once per process.
inline const TestTlsMaterial& TestTls() {
    static const TestTlsMaterial material = tls_detail::GenerateTestTlsMaterial();
    return material;
}

// Writes TestTls() to `cert_path` and `key_path`.
inline void WriteTestTlsFiles(const std::filesystem::path& cert_path, const std::filesystem::path& key_path) {
    tls_detail::WriteFile(cert_path, TestTls().cert_pem);
    tls_detail::WriteFile(key_path, TestTls().key_pem);
}

}  // namespace chunkdb::test

#endif
