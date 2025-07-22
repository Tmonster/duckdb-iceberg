//===----------------------------------------------------------------------===//
//                         DuckDB
//
// url_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

struct PathComponent {
	PathComponent(string component) : component(component), url_encoded(false) {};
	PathComponent(string component, bool url_encoded) : component(component), url_encoded(url_encoded) {
	}

	string component;
	bool url_encoded;

	string GetEncodedComponent() const {
		if (url_encoded) {
			return component;
		}
		return StringUtil::URLEncode(component);
	}
};

class IRCEndpointBuilder {
public:
	void AddPathComponent(const string &component);
	void AddPathComponent(PathComponent &component);

	void SetHost(const string &host);
	string GetHost() const;

	void SetParam(const string &key, const string &value);
	string GetParam(const string &key) const;
	const unordered_map<string, string> GetParams() const;

	string GetURL() const;

	//! path components when querying. Like namespaces/tables etc.
	vector<PathComponent> path_components;

private:
	string host;
	unordered_map<string, string> params;
};

} // namespace duckdb
