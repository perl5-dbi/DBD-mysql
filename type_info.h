#include "dbdimp.h"

typedef struct sql_type_info_s {
	const char *type_name;
	int data_type;
	int column_size;
	const char *literal_prefix;
	const char *literal_suffix;
	const char *create_params;
	int nullable;
	int case_sensitive;
	int searchable;
	int unsigned_attribute;
	int fixed_prec_scale;
	int auto_unique_value;
	const char *local_type_name;
	int minimum_scale;
	int maximum_scale;
	int num_prec_radix;
	int sql_datatype;
	int sql_datetime_sub;
	int interval_precision;
	int native_type;
	int is_num;
        char*  (*quote)();
        void   (*dequote)();

} sql_type_info_t;

AV *build_type_info_all(void);
const sql_type_info_t *native2sql(const int t);
const sql_type_info_t *sql_type_data(const int tp);

