package config

const (
	DEFAULT_DATATABLE_DIMENSION_DATA               = "dt%d_dimension_data"                  // dt<datatable_id>_dimension_data
	DEFAULT_DATATABLE_DIMENSION_DATA_LOCALE        = "dt%d_dimension_data_locale_%d"        // dt<datatable_id>_dimension_data_locale_<locale_id>
	DEFAULT_DATATABLE_DIMENSION_DATA_SEARCH_LOCALE = "dt%d_dimension_data_search_locale_%d" // dt<datatable_id>_dimension_data_search_locale_<locale_id>
	DEFAULT_DATATABLE_BASE_MEASURES                = "dt%d_base_measures_%s"                // dt<datatable_id>_base_measures_<data type>
	DEFAULT_DATATABLE_BASE_MEASURES_BRANCHES       = "dt%d_base_measures_%s_branches"       // dt<datatable_id>_base_measures_<data type>_branches

	DEFAULT_CACHELAYER_DIMENSION_DATA               = "cache_layer_%d_dimension_data"                  // cache_layer_<cache_layer_id>_dimension_data
	DEFAULT_CACHELAYER_DIMENSION_DATA_LOCALE        = "cache_layer_%d_dimension_data_locale_%d"        // cache_layer_<cache_layer_id>_dimension_data_locale_<locale_id>
	DEFAULT_CACHELAYER_DIMENSION_DATA_SEARCH_LOCALE = "cache_layer_%d_dimension_data_search_locale_%d" // cache_layer_<cache_layer_id>_dimension_data_search_locale_<locale_id>
	DEFAULT_CACHELAYER_BASE_MEASURES                = "cache_layer_%d_measures_%s"                     // cache_layer_<cache_layer_id>_measures_<data type>
	DEFAULT_CACHELAYER_BASE_MEASURES_BRANCHES       = "cache_layer_%d_measures_%s_branches"            // cache_layer_<cache_layer_id>_measures_<data type>_branches

	MEASURE_AGGREGATION_TYPE_COUNT     = "COUNT"
	MEASURE_AGGREGATION_TYPE_SUM       = "SUM"
	MEASURE_AGGREGATION_TYPE_MIN       = "MIN"
	MEASURE_AGGREGATION_TYPE_MAX       = "MAX"
	MEASURE_AGGREGATION_TYPE_BASE_ONLY = "BASE_ONLY"
	MEASURE_AGGREGATION_TYPE_FORMULA   = "FORMULA"
	MEASURE_AGGREGATION_TYPE_EXTENSION = "EXTENSION"

	DEFAULT_TIME_FORMAT      = "2006-01-02 15:04:05.999999"
	CASTYYPE_TIME_FORMAT     = "15:04:05.999999"
	CASTYYPE_DATETIME_FORMAT = "2006-01-02 15:04:05.999999"
	CASTYYPE_DATE_FORMAT     = "2006-01-02"

	// Data Query
	DEFAULT_LOCALE_ID               = 3
	DEFAULT_DATA_QUERY_FIRST        = 10
	DEFAULT_DATA_QUERY_LIMIT        = 3000
	DEFAULT_DATA_QUERY_SEARCH_FIRST = 10
	DATA_QUERY_CACHE_KEY            = "query/results/%s/"
	DEFAULT_DATA_QUERY_CACHE_SHARD  = 100

	// Cache expiration
	DEFAULT_BASE_CACHE_EXPIRATION    = 1200 // seconds = 20 min
	DEFAULT_MINRAND_CACHE_EXPIRATION = 0    // seconds
	DEFAULT_MAXRAND_CACHE_EXPIRATION = 120  // seconds

	// Data Update
	DATA_UPDATE_BULK_SIZE = 4

	// TODO: CACHE LAYER HARDCODED
	HARDCODED_CACHE_LAYER_ID = 1
	HARDCODED_DATATABLE_ID   = 5

	// Ingests
	INGEST_DELETE_TIMESTAMP     = 6
	INGEST_UPDATE_BULK_SIZE     = 8
	VERSION_KEEP_ALIVE_INTERVAL = 5 // mins
)
