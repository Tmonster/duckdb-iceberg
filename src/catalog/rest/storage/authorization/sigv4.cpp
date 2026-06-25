#include "catalog/rest/storage/authorization/sigv4.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/main/setting_info.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/types/value.hpp"

#include "catalog/rest/api/api_utils.hpp"
#include "catalog/rest/api/url_utils.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/storage/authorization/sigv4_utils.hpp"
#include "iceberg_logging.hpp"

#include <chrono>
#include <limits>

namespace duckdb {

namespace {

//! Detect the scheme from a host string, defaulting to HTTPS
Aws::Http::Scheme DetectScheme(const string &host) {
	auto lower = StringUtil::Lower(host);
	if (StringUtil::StartsWith(lower, "http://")) {
		return Aws::Http::Scheme::HTTP;
	}
	return Aws::Http::Scheme::HTTPS;
}

} // namespace

SIGV4Authorization::SIGV4Authorization(AttachedDatabase &db)
    : IcebergAuthorization(db, IcebergAuthorizationType::SIGV4) {
}

SIGV4Authorization::SIGV4Authorization(AttachedDatabase &db, const string &secret)
    : IcebergAuthorization(db, IcebergAuthorizationType::SIGV4), secret(secret) {
}

unique_ptr<IcebergAuthorization> SIGV4Authorization::FromAttachOptions(AttachedDatabase &db,
                                                                       IcebergAttachOptions &input) {
	auto result = make_uniq<SIGV4Authorization>(db);

	unordered_map<string, Value> remaining_options;
	for (auto &entry : input.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			if (!result->secret.empty()) {
				throw InvalidInputException("Duplicate 'secret' option detected!");
			}
			result->secret = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "sigv4_service") {
			result->sigv4_service = entry.second.ToString();
		} else if (lower_name == "sigv4_region") {
			result->sigv4_region = entry.second.ToString();
		} else if (lower_name == "sigv4_credential_refresh_seconds") {
			result->credential_refresh_seconds = entry.second.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
			if (result->credential_refresh_seconds < 0) {
				throw InvalidInputException("'sigv4_credential_refresh_seconds' must be a non-negative integer");
			}
		} else if (lower_name == "extra_http_headers") {
			// Parse extra_http_headers if provided directly in attach options
			IcebergAuthorization::ParseExtraHttpHeaders(entry.second, result->extra_http_headers);
		} else {
			remaining_options.emplace(std::move(entry));
		}
	}
	input.options = std::move(remaining_options);
	return std::move(result);
}

AWSInput SIGV4Authorization::CreateAWSInput(ClientContext &context, const IRCEndpointBuilder &endpoint_builder) {
	AWSInput aws_input(db);
	aws_input.cert_path = APIUtils::GetCURLCertPath();

	// Set the user Agent
	auto &config = DBConfig::GetConfig(context);
	aws_input.user_agent = config.UserAgent();
	Value val;
	auto lookup_result = context.TryGetCurrentSetting("http_timeout", val);
	if (lookup_result.GetScope() != SettingScope::INVALID) {
		aws_input.use_httpfs_timeout = true;
		// http timeout is in seconds, multiply by 1000 to get ms
		aws_input.request_timeout_in_ms = val.GetValue<idx_t>() * 1000;
	}

	auto host = endpoint_builder.GetHost();
	aws_input.scheme = DetectScheme(host);
	auto stripped_host = StripScheme(host);

	// AWS service and region: use explicit overrides if provided, otherwise parse from host
	if (!sigv4_service.empty()) {
		aws_input.service = sigv4_service;
	} else {
		aws_input.service = GetAwsService(stripped_host);
	}
	if (!sigv4_region.empty()) {
		aws_input.region = sigv4_region;
	} else {
		aws_input.region = GetAwsRegion(stripped_host);
	}

	// Host decomposition
	auto decomposed_host = DecomposeHost(stripped_host);
	aws_input.authority = decomposed_host.authority;

	for (auto &component : decomposed_host.path_components) {
		aws_input.path_segments.push_back(component);
	}
	for (auto &component : endpoint_builder.path_components) {
		aws_input.path_segments.push_back(component.raw);
	}
	for (auto &param : endpoint_builder.GetParams()) {
		aws_input.query_string_parameters.emplace_back(param.first, param.second.raw);
	}

	// AWS credentials
	auto secret_entry = IcebergCatalog::GetStorageSecret(context, secret);
	auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
	aws_input.key_id = kv_secret.secret_map["key_id"].GetValue<string>();
	aws_input.secret = kv_secret.secret_map["secret"].GetValue<string>();
	aws_input.session_token =
	    kv_secret.secret_map["session_token"].IsNull() ? "" : kv_secret.secret_map["session_token"].GetValue<string>();

	return aws_input;
}

bool SIGV4Authorization::RefreshStorageSecretUnlocked(ClientContext &context) {
	auto secret_entry = IcebergCatalog::GetStorageSecret(context, secret);
	if (!secret_entry || !secret_entry->secret) {
		return false;
	}
	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

	Value refresh_info;
	if (!kv_secret.TryGetValue("refresh_info", refresh_info) || refresh_info.IsNull()) {
		// The secret carries static credentials (no 'refresh_info'); there is nothing to re-vend.
		return false;
	}

	// Reconstruct the create-secret call from the secret + its 'refresh_info', mirroring httpfs'
	// GenerateRefreshSecretInfo. Dispatch is by provider, so this re-runs whatever created the secret
	// (e.g. the AWS credential_chain / STS provider) to obtain fresh credentials.
	CreateSecretInput input;
	input.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	input.persist_type = SecretPersistType::TEMPORARY;
	input.type = kv_secret.GetType();
	input.provider = kv_secret.GetProvider();
	input.name = kv_secret.GetName();
	input.scope = kv_secret.GetScope();
	input.storage_type = secret_entry->storage_mode;

	auto child_count = StructType::GetChildCount(refresh_info.type());
	auto &children = StructValue::GetChildren(refresh_info);
	for (idx_t i = 0; i < child_count; i++) {
		input.options[StructType::GetChildName(refresh_info.type(), i)] = children[i];
	}

	try {
		auto &secret_manager = SecretManager::Get(context);
		secret_manager.CreateSecret(context, input);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		DUCKDB_LOG_WARNING(context, "Failed to refresh SigV4 catalog credentials secret '%s': %s", secret,
		                   error.RawMessage());
		return false;
	}
	return true;
}

int64_t SIGV4Authorization::ComputeNextRefreshDeadlineUnlocked(ClientContext &context, int64_t now_seconds) {
	// An explicit attach option takes precedence: refresh on a fixed interval.
	if (credential_refresh_seconds > 0) {
		return now_seconds + credential_refresh_seconds;
	}

	// Otherwise, if the credentials carry an expiry, refresh once 90% of the lifetime has elapsed.
	auto secret_entry = IcebergCatalog::GetStorageSecret(context, secret);
	if (secret_entry && secret_entry->secret) {
		auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
		Value expires_at_val;
		if (kv_secret.TryGetValue("expires_at", expires_at_val) && !expires_at_val.IsNull()) {
			Value casted;
			string error;
			// 'expires_at' is epoch milliseconds (e.g. from s3.session-token-expires-at-ms).
			if (expires_at_val.DefaultTryCastAs(LogicalType::BIGINT, casted, &error)) {
				int64_t expires_at_seconds = casted.GetValue<int64_t>() / 1000;
				int64_t remaining = expires_at_seconds - now_seconds;
				if (remaining <= 0) {
					return now_seconds; // already expired -> refresh now
				}
				return now_seconds + static_cast<int64_t>(0.9 * static_cast<double>(remaining));
			}
		}
	}

	// No refresh signal available (no attach option and no expiry on the secret): never proactively refresh.
	return std::numeric_limits<int64_t>::max();
}

void SIGV4Authorization::RefreshCatalogCredentialsIfNeeded(ClientContext &context) {
	std::lock_guard<std::mutex> lock(credential_mutex);

	auto now = std::chrono::system_clock::now();
	int64_t now_seconds = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();

	if (credentials_refresh_at == 0) {
		// First request: establish the refresh deadline from the current credentials, without
		// refreshing (they are assumed valid at attach time).
		credentials_refresh_at = ComputeNextRefreshDeadlineUnlocked(context, now_seconds);
	}

	if (now_seconds < credentials_refresh_at) {
		return; // credentials are still considered fresh
	}

	// Credentials are stale: re-vend the secret, then recompute the deadline from the new state.
	bool refreshed = RefreshStorageSecretUnlocked(context);
	now = std::chrono::system_clock::now();
	now_seconds = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
	if (refreshed) {
		DUCKDB_LOG_INFO(context, "Refreshed SigV4 catalog credentials for secret '%s'", secret);
		credentials_refresh_at = ComputeNextRefreshDeadlineUnlocked(context, now_seconds);
	} else {
		// Refresh unavailable or failed: back off so we don't retry on every request.
		credentials_refresh_at = now_seconds + CREDENTIAL_REFRESH_BACKOFF_SECONDS;
	}
}

unique_ptr<HTTPResponse> SIGV4Authorization::Request(RequestType request_type, ClientContext &context,
                                                     const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
                                                     const string &data) {
	// Proactively refresh the catalog credentials before signing if they are considered stale.
	RefreshCatalogCredentialsIfNeeded(context);

	// Note: For SIGV4, custom headers should be added BEFORE signing so they're included in the signature
	// Merge extra HTTP headers first
	for (auto &entry : extra_http_headers) {
		headers.Insert(entry.first, entry.second);
	}

	auto aws_input = CreateAWSInput(context, endpoint_builder);
	return aws_input.Request(request_type, context, headers, data);
}

} // namespace duckdb
