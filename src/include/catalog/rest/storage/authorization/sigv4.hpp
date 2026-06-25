#pragma once

#include "catalog/rest/storage/iceberg_authorization.hpp"
#include "catalog/rest/storage/aws.hpp"

#include <mutex>

namespace duckdb {

class SIGV4Authorization : public IcebergAuthorization {
public:
	static constexpr const IcebergAuthorizationType TYPE = IcebergAuthorizationType::SIGV4;

	//! When a refresh fails (or no signal is available momentarily), wait this long before retrying so
	//! we don't re-attempt an STS call on every catalog request.
	static constexpr int64_t CREDENTIAL_REFRESH_BACKOFF_SECONDS = 30;

public:
	SIGV4Authorization(AttachedDatabase &db);
	SIGV4Authorization(AttachedDatabase &db, const string &secret);

public:
	static unique_ptr<IcebergAuthorization> FromAttachOptions(AttachedDatabase &db, IcebergAttachOptions &input);
	unique_ptr<HTTPResponse> Request(RequestType request_type, ClientContext &context,
	                                 const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
	                                 const string &data = "") override;

private:
	AWSInput CreateAWSInput(ClientContext &context, const IRCEndpointBuilder &endpoint_builder);

	//! Proactively re-vend the catalog credentials secret if it is considered stale. Called before
	//! every SigV4-signed catalog request. Serialized via credential_mutex.
	void RefreshCatalogCredentialsIfNeeded(ClientContext &context) override;
	//! Compute the next epoch-seconds deadline at which credentials are considered stale. Uses the
	//! 'sigv4_credential_refresh_seconds' attach option if set; otherwise the secret's 'expires_at'
	//! (refreshing once 90% of the lifetime has elapsed); otherwise never (no signal available).
	//! Caller must hold credential_mutex.
	int64_t ComputeNextRefreshDeadlineUnlocked(ClientContext &context, int64_t now_seconds);
	//! Re-vend the catalog credentials secret via its own provider (e.g. AWS credential_chain / STS),
	//! reconstructing the create-secret call from the secret's stored 'refresh_info'. Returns false if
	//! the secret does not exist or is not refreshable. Caller must hold credential_mutex.
	bool RefreshStorageSecretUnlocked(ClientContext &context);

public:
	string secret;
	string region;
	//! Optional: override the AWS service name used for SigV4 signing, useful for self-hosted REST catalog services
	string sigv4_service;
	//! Optional: override the AWS region used for SigV4 signing, useful for non-AWS endpoints
	string sigv4_region;
	//! Optional: proactively refresh the catalog credentials every N seconds. 0 = unset (fall back to
	//! the secret's 'expires_at', or no proactive refresh if neither is available).
	int64_t credential_refresh_seconds = 0;

private:
	//! Mutable refresh state (protected by credential_mutex). Epoch-seconds deadline after which the
	//! catalog credentials are considered stale; 0 means "not yet initialized".
	int64_t credentials_refresh_at = 0;
	//! Serializes the check+refresh so at most one thread re-vends the secret at a time.
	std::mutex credential_mutex;
};

} // namespace duckdb
