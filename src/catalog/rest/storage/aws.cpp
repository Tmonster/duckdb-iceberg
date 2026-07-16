#include "catalog/rest/storage/aws.hpp"

#include "duckdb/common/http_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/main/client_data.hpp"
#include "mbedtls_wrapper.hpp"
#include "iceberg_logging.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"

#include <algorithm>

namespace duckdb {

namespace {

typedef unsigned char hash_str[64];
typedef unsigned char hash_bytes[32];

void sha256(const char *in, size_t in_len, hash_bytes &out) {
	duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(in, in_len, (char *)out);
}

void hmac256(const std::string &message, const char *secret, size_t secret_len, hash_bytes &out) {
	duckdb_mbedtls::MbedTlsWrapper::Hmac256(secret, secret_len, message.data(), message.size(), (char *)out);
}

void hmac256(std::string message, hash_bytes secret, hash_bytes &out) {
	hmac256(message, (char *)secret, sizeof(hash_bytes), out);
}

void hex256(hash_bytes &in, hash_str &out) {
	const char *hex = "0123456789abcdef";
	unsigned char *pin = in;
	unsigned char *pout = out;
	for (; pin < in + sizeof(in); pout += 2, pin++) {
		pout[0] = hex[(*pin >> 4) & 0xF];
		pout[1] = hex[*pin & 0xF];
	}
}

//! HTTP method name as it appears in the SigV4 canonical request
string GetRequestMethodName(RequestType request_type) {
	switch (request_type) {
	case RequestType::GET_REQUEST:
		return "GET";
	case RequestType::PUT_REQUEST:
		return "PUT";
	case RequestType::HEAD_REQUEST:
		return "HEAD";
	case RequestType::DELETE_REQUEST:
		return "DELETE";
	case RequestType::POST_REQUEST:
		return "POST";
	case RequestType::OPTIONS_REQUEST:
		return "OPTIONS";
	default:
		throw NotImplementedException("Unexpected HTTP Method requested");
	}
}

} // namespace

static string GetPayloadHash(const char *buffer, idx_t buffer_len) {
	if (buffer_len > 0) {
		hash_bytes payload_hash_bytes;
		hash_str payload_hash_str;
		sha256(buffer, buffer_len, payload_hash_bytes);
		hex256(payload_hash_bytes, payload_hash_str);
		return string((char *)payload_hash_str, sizeof(payload_hash_str));
	} else {
		return "";
	}
}

string AWSInput::GetURLEncodedPath() const {
	string url_encoded_path;
	for (auto &segment : path_segments) {
		url_encoded_path += "/" + StringUtil::URLEncode(segment);
	}
	if (url_encoded_path.empty()) {
		url_encoded_path = "/";
	}
	return url_encoded_path;
}

string AWSInput::GetQueryString() const {
	//! Sort parameters by key, as required by the SigV4 canonical request
	auto sorted_params = query_string_parameters;
	std::sort(sorted_params.begin(), sorted_params.end());
	string query_string;
	for (auto &param : sorted_params) {
		if (!query_string.empty()) {
			query_string += "&";
		}
		query_string += StringUtil::URLEncode(param.first) + "=" + StringUtil::URLEncode(param.second);
	}
	return query_string;
}

unique_ptr<HTTPResponse> AWSInput::ExecuteRequest(ClientContext &context, RequestType request_type,
                                                  HTTPHeaders &headers, const string &body) {
	auto &db = DatabaseInstance::GetDatabase(context);

	HTTPHeaders res(db);

	const string host = authority;
	res["host"] = host;
	// If access key is not set, we don't set the headers at all to allow accessing public files through s3 urls

	string payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"; // Empty payload hash

	if (!body.empty()) {
		payload_hash = GetPayloadHash(body.c_str(), body.size());
	}

	// key_id, secret, session_token
	// we can pass date/time but this is mostly useful in testing. normally we just get the current datetime
	// here.
	auto timestamp = Timestamp::GetCurrentTimestamp();
	string date_now = StrfTimeFormat::Format(timestamp, "%Y%m%d");
	string datetime_now = StrfTimeFormat::Format(timestamp, "%Y%m%dT%H%M%SZ");

	res["x-amz-date"] = datetime_now;
	res["x-amz-content-sha256"] = payload_hash;
	if (session_token.length() > 0) {
		res["x-amz-security-token"] = session_token;
	}
	string content_type;
	if (headers.HasHeader("Content-Type")) {
		content_type = headers.GetHeaderValue("Content-Type");
	}
	if (!content_type.empty()) {
		res["Content-Type"] = content_type;
	}
	string signed_headers = "";
	hash_bytes canonical_request_hash;
	hash_str canonical_request_hash_str;
	if (content_type.length() > 0) {
		signed_headers += "content-type;";
		res["Content-Type"] = content_type;
	}
	signed_headers += "host;x-amz-content-sha256;x-amz-date";
	if (session_token.length() > 0) {
		signed_headers += ";x-amz-security-token";
	}

	string url_encoded_path = GetURLEncodedPath();
	string query_string = GetQueryString();

	string canonical_path = url_encoded_path;
	{
		// it's unclear to be why we need to transform %2F into %252F, see
		// https://en.wikipedia.org/wiki/Percent-encoding#Percent_character
		canonical_path = StringUtil::Replace(canonical_path, "%2F", "%252F");
	}

	auto canonical_request = GetRequestMethodName(request_type) + "\n" + canonical_path + "\n" + query_string;

	if (content_type.length() > 0) {
		canonical_request += "\ncontent-type:" + content_type;
	}
	canonical_request += "\nhost:" + host + "\nx-amz-content-sha256:" + payload_hash + "\nx-amz-date:" + datetime_now;
	if (session_token.length() > 0) {
		canonical_request += "\nx-amz-security-token:" + session_token;
	}
	canonical_request += "\n\n" + signed_headers + "\n" + payload_hash;
	sha256(canonical_request.c_str(), canonical_request.length(), canonical_request_hash);

	hex256(canonical_request_hash, canonical_request_hash_str);
	auto string_to_sign = "AWS4-HMAC-SHA256\n" + datetime_now + "\n" + date_now + "/" + region + "/" + service +
	                      "/aws4_request\n" + string((char *)canonical_request_hash_str, sizeof(hash_str));

	// TODO: DUCKDB_LOGS (canonical_request + string_to_sing)

	// compute signature
	hash_bytes k_date, k_region, k_service, signing_key, signature;
	hash_str signature_str;
	auto sign_key = "AWS4" + secret;
	hmac256(date_now, sign_key.c_str(), sign_key.length(), k_date);
	hmac256(region, k_date, k_region);
	hmac256(service, k_region, k_service);
	hmac256("aws4_request", k_service, signing_key);
	hmac256(string_to_sign, signing_key, signature);
	hex256(signature, signature_str);

	res["Authorization"] = "AWS4-HMAC-SHA256 Credential=" + key_id + "/" + date_now + "/" + region + "/" + service +
	                       "/aws4_request, SignedHeaders=" + signed_headers +
	                       ", Signature=" + string((char *)signature_str, sizeof(hash_str));

	auto &http_util = HTTPUtil::Get(db);
	unique_ptr<HTTPParams> params;

	string request_url = scheme + "://" + host + url_encoded_path;
	if (!query_string.empty()) {
		request_url += "?" + query_string;
	}

	params = http_util.InitializeParameters(context, request_url);

	auto &client = IcebergAuthorizationContextState::GetHTTPClient(attached_db, context);
	if (client) {
		client->Initialize(*params);
	}

	switch (request_type) {
	case RequestType::HEAD_REQUEST: {
		HeadRequestInfo head_request(request_url, res, *params);
		return http_util.Request(head_request, client);
	}
	case RequestType::DELETE_REQUEST: {
		DeleteRequestInfo delete_request(request_url, res, *params);
		return http_util.Request(delete_request, client);
	}
	case RequestType::GET_REQUEST: {
		GetRequestInfo get_request(request_url, res, *params, nullptr, nullptr);
		return http_util.Request(get_request, client);
	}
	case RequestType::POST_REQUEST: {
		PostRequestInfo post_request(request_url, res, *params, reinterpret_cast<const_data_ptr_t>(body.c_str()),
		                             body.size());
		auto x = http_util.Request(post_request, client);
		if (x) {
			x->body = post_request.buffer_out;
		}
		return x;
	}
	default:
		throw NotImplementedException("Unexpected HTTP Method requested");
	}
}

unique_ptr<HTTPResponse> AWSInput::Request(RequestType request_type, ClientContext &context, HTTPHeaders &headers,
                                           const string &data) {
	switch (request_type) {
	case RequestType::GET_REQUEST:
	case RequestType::DELETE_REQUEST:
	case RequestType::HEAD_REQUEST:
		return ExecuteRequest(context, request_type, headers);
	case RequestType::POST_REQUEST:
		return ExecuteRequest(context, request_type, headers, data);
	default:
		throw NotImplementedException("Cannot make request of type %s", EnumUtil::ToString(request_type));
	}
}

} // namespace duckdb
