#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"

#include "iceberg_options.hpp"

#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"

#include "avro_scan.hpp"

namespace duckdb {

// Manifest Reader

class BaseManifestReader {
public:
	BaseManifestReader(idx_t iceberg_version) : iceberg_version(iceberg_version) {
		if (iceberg_version > 2) {
			throw InvalidConfigurationException("Iceberg version > 2 is not supported yet");
		}
	}
	virtual ~BaseManifestReader() {
	}

public:
	void Initialize(unique_ptr<AvroScan> scan_p);
	bool Finished() const;
	virtual void CreateVectorMapping(idx_t i, MultiFileColumnDefinition &column) = 0;
	virtual bool ValidateVectorMapping() = 0;

protected:
	idx_t ScanInternal(idx_t remaining);

protected:
	DataChunk chunk;
	unordered_map<int32_t, ColumnIndex> vector_mapping;
	const idx_t iceberg_version;
	unique_ptr<AvroScan> scan;
	idx_t offset = 0;
	bool finished = true;
};

namespace manifest_list {

static constexpr const int32_t MANIFEST_PATH = 500;
static constexpr const int32_t MANIFEST_LENGTH = 501;
static constexpr const int32_t PARTITION_SPEC_ID = 502;
static constexpr const int32_t CONTENT = 517;
static constexpr const int32_t SEQUENCE_NUMBER = 515;
static constexpr const int32_t MIN_SEQUENCE_NUMBER = 516;
static constexpr const int32_t ADDED_SNAPSHOT_ID = 503;
static constexpr const int32_t ADDED_FILES_COUNT = 504;
static constexpr const int32_t EXISTING_FILES_COUNT = 505;
static constexpr const int32_t DELETED_FILES_COUNT = 506;
static constexpr const int32_t ADDED_ROWS_COUNT = 512;
static constexpr const int32_t EXISTING_ROWS_COUNT = 513;
static constexpr const int32_t DELETED_ROWS_COUNT = 514;
static constexpr const int32_t PARTITIONS = 507;
static constexpr const int32_t PARTITIONS_ELEMENT = 508;
static constexpr const int32_t FIELD_SUMMARY_CONTAINS_NULL = 509;
static constexpr const int32_t FIELD_SUMMARY_CONTAINS_NAN = 518;
static constexpr const int32_t FIELD_SUMMARY_LOWER_BOUND = 510;
static constexpr const int32_t FIELD_SUMMARY_UPPER_BOUND = 511;
static constexpr const int32_t KEY_METADATA = 519;
static constexpr const int32_t FIRST_ROW_ID = 520;

//! Produces IcebergManifests read, from the 'manifest_list'
class ManifestListReader : public BaseManifestReader {
public:
	ManifestListReader(idx_t iceberg_version);
	~ManifestListReader() override {
	}

public:
	idx_t Read(idx_t count, vector<IcebergManifest> &result);
	void CreateVectorMapping(idx_t i, MultiFileColumnDefinition &column) override;
	bool ValidateVectorMapping() override;

private:
	idx_t ReadChunk(idx_t offset, idx_t count, vector<IcebergManifest> &result);
};

} // namespace manifest_list

namespace manifest_file {

static constexpr const int32_t STATUS = 0;
static constexpr const int32_t SNAPSHOT_ID = 1;
static constexpr const int32_t SEQUENCE_NUMBER = 3;
static constexpr const int32_t FILE_SEQUENCE_NUMBER = 4;
static constexpr const int32_t DATA_FILE = 2;
static constexpr const int32_t CONTENT = 134;
static constexpr const int32_t FILE_PATH = 100;
static constexpr const int32_t FILE_FORMAT = 101;
static constexpr const int32_t PARTITION = 102;
static constexpr const int32_t RECORD_COUNT = 103;
static constexpr const int32_t FILE_SIZE_IN_BYTES = 104;
// static constexpr const int32_t BLOCK_SIZE_IN_BYTES = 105; // (deprecated)
// static constexpr const int32_t FILE_ORDINAL = 106; // (deprecated)
// static constexpr const int32_t SORT_COLUMNS = 107; // (deprecated)
// static constexpr const int32_t SORT_COLUMNS_ELEMENT = 112; // (deprecated)
static constexpr const int32_t COLUMN_SIZES = 108;
static constexpr const int32_t COLUMN_SIZES_KEY = 117;
static constexpr const int32_t COLUMN_SIZES_VALUE = 118;
static constexpr const int32_t VALUE_COUNTS = 109;
static constexpr const int32_t VALUE_COUNTS_KEY = 119;
static constexpr const int32_t VALUE_COUNTS_VALUE = 120;
static constexpr const int32_t NULL_VALUE_COUNTS = 110;
static constexpr const int32_t NULL_VALUE_COUNTS_KEY = 121;
static constexpr const int32_t NULL_VALUE_COUNTS_VALUE = 122;
static constexpr const int32_t NAN_VALUE_COUNTS = 137;
static constexpr const int32_t NAN_VALUE_COUNTS_KEY = 138;
static constexpr const int32_t NAN_VALUE_COUNTS_VALUE = 139;
// static constexpr const int32_t DISTINCT_COUNTS = 111; // (deprecated)
static constexpr const int32_t LOWER_BOUNDS = 125;
static constexpr const int32_t LOWER_BOUNDS_KEY = 126;
static constexpr const int32_t LOWER_BOUNDS_VALUE = 127;
static constexpr const int32_t UPPER_BOUNDS = 128;
static constexpr const int32_t UPPER_BOUNDS_KEY = 129;
static constexpr const int32_t UPPER_BOUNDS_VALUE = 130;
// static constexpr const int32_t KEY_METADATA = 131; // (optional)
static constexpr const int32_t SPLIT_OFFSETS = 132;
static constexpr const int32_t SPLIT_OFFSETS_ELEMENT = 133;
static constexpr const int32_t EQUALITY_IDS = 135;
static constexpr const int32_t EQUALITY_IDS_ELEMENT = 136;
static constexpr const int32_t SORT_ORDER_ID = 140;
static constexpr const int32_t FIRST_ROW_ID = 142;
static constexpr const int32_t REFERENCED_DATA_FILE = 143;
static constexpr const int32_t CONTENT_OFFSET = 144;
static constexpr const int32_t CONTENT_SIZE_IN_BYTES = 145;

//! Produces IcebergManifestEntries read, from the 'manifest_file'
class ManifestFileReader : public BaseManifestReader {
public:
	ManifestFileReader(idx_t iceberg_version, bool skip_deleted = true);
	~ManifestFileReader() override {
	}

public:
	idx_t Read(idx_t count, vector<IcebergManifestEntry> &result);
	void CreateVectorMapping(idx_t i, MultiFileColumnDefinition &column) override;
	bool ValidateVectorMapping() override;

public:
	void SetSequenceNumber(sequence_number_t sequence_number);
	void SetPartitionSpecID(int32_t partition_spec_id);

private:
	idx_t ReadChunk(idx_t offset, idx_t count, vector<IcebergManifestEntry> &result);

public:
	//! The sequence number to inherit when the condition to do so is met
	sequence_number_t sequence_number;
	//! The inherited partition spec id (from the 'manifest_file')
	int32_t partition_spec_id;
	//! Whether the deleted entries should be skipped outright
	bool skip_deleted = false;
};

} // namespace manifest_file

} // namespace duckdb
