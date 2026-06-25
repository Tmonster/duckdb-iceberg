#pragma once

#include "duckdb/main/secret/secret.hpp"

namespace duckdb {
class ExtensionLoader;
class ClientContext;
class BaseSecret;

//! Secret provider for Iceberg vended credentials.
//!
//! Storage secrets created from vended credentials (type "s3"/"gcs"/"r2") use this provider so that
//! when the underlying short-lived credentials expire, DuckDB's secret-refresh machinery
//! (httpfs' CreateS3SecretFunctions::TryRefreshS3Secret) re-dispatches back into this provider:
//! GenerateRefreshSecretInfo() reconstructs a CreateSecretInput whose provider is the secret's
//! provider, then calls SecretManager::CreateSecret(), which routes here.
//!
//! On refresh we re-hit the GetTableInformation endpoint and re-vend fresh credentials via
//! IcebergTableInformation::GetVendedCredentials(), mirroring the AWS STS credential-chain refresh
//! pattern (where 'refresh_info' carries the parameters needed to re-fetch credentials).
class IcebergTableSecretProvider {
public:
	//! Provider name registered under the s3/gcs/r2 secret types for refreshable vended credentials.
	static constexpr const char *PROVIDER = "iceberg";

	//! Register the create-secret functions (one per storage type) that implement refreshable
	//! vended credentials.
	static void Register(ExtensionLoader &loader);

	//! Build the 'refresh_info' struct embedded into a vended secret. It carries the qualified table
	//! identity needed to re-vend credentials when the secret is refreshed.
	static Value MakeRefreshInfo(const string &catalog_name, const string &schema_name, const string &table_name);

	//! CreateSecretFunction entrypoint. Handles both the initial creation of a vended secret (when the
	//! input already carries freshly vended credentials + a 'refresh_info' struct) and a refresh (when
	//! the input only carries the table identity unpacked from 'refresh_info').
	static unique_ptr<BaseSecret> CreateSecret(ClientContext &context, CreateSecretInput &input);
};

} // namespace duckdb
