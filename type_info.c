#include "type_info.h"

const sql_type_info_t SQL_GET_TYPE_INFO_values[] = {
	{"varchar", SQL_VARCHAR, 255, "'", "'", "max length",
	 1, 0, 3, 0, 0, 0, "variable length string", 0, 0, 0,
	 SQL_VARCHAR, 0, 0, FIELD_TYPE_VAR_STRING, 0
	 /* 0 */
	 },
	{"decimal", SQL_DECIMAL, 15, NULL, NULL, "precision,scale",
	 1, 0, 3, 0, 0, 0, "double", 0, 6, 2,
	 SQL_DECIMAL, 0, 0, FIELD_TYPE_DECIMAL, 1
	 /* 1 */
	 },
	{"tinyint", SQL_TINYINT, 3, NULL, NULL, NULL,
	 1, 0, 3, 0, 0, 0, "Tiny integer", 0, 0, 10,
	 SQL_TINYINT, 0, 0, FIELD_TYPE_TINY, 1
	 /* 2 */
	 },
	{"smallint", SQL_SMALLINT, 5, NULL, NULL, NULL,
	 1, 0, 3, 0, 0, 0, "Short integer", 0, 0, 10,
	 SQL_SMALLINT, 0, 0, FIELD_TYPE_SHORT, 1
	 /* 3 */
	 },
	{"integer", SQL_INTEGER, 10, NULL, NULL, NULL,
	 1, 0, 3, 0, 0, 0, "integer", 0, 0, 10,
	 SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1
	 /* 4 */
	 },
	{"float", SQL_REAL, 7, NULL, NULL, NULL,
	 1, 0, 0, 0, 0, 0, "float", 0, 2, 10,
	 SQL_FLOAT, 0, 0, FIELD_TYPE_FLOAT, 1
	 /* 5 */
	 },
	{"double", SQL_FLOAT, 15, NULL, NULL, NULL,
	 1, 0, 3, 0, 0, 0, "double", 0, 4, 2,
	 SQL_FLOAT, 0, 0, FIELD_TYPE_DOUBLE, 1
	 /* 6 */
	 },
	{"double", SQL_DOUBLE, 15, NULL, NULL, NULL,
	 1, 0, 3, 0, 0, 0, "double", 0, 4, 10,
	 SQL_DOUBLE, 0, 0, FIELD_TYPE_DOUBLE, 1
	 /* 6 */
	 },
	/* FIELD_TYPE_NULL ?  */
	{"timestamp", SQL_TIMESTAMP, 14, "'", "'", NULL,
	 0, 0, 3, 0, 0, 0, "timestamp", 0, 0, 0,
	 SQL_TIMESTAMP, 0, 0, FIELD_TYPE_TIMESTAMP, 0
	 /* 7 */
	 },
	{"bigint", SQL_BIGINT, 19, NULL, NULL, NULL,
	 1, 0, 3, 0, 0, 0, "Longlong integer", 0, 0, 10,
	 SQL_BIGINT, 0, 0, FIELD_TYPE_LONGLONG, 1
	 /* 8 */
	 },
	{"middleint", SQL_INTEGER, 8, NULL, NULL, NULL,
	 1, 0, 3, 0, 0, 0, "Medium integer", 0, 0, 10,
	 SQL_INTEGER, 0, 0, FIELD_TYPE_INT24, 1
	 /* 9 */
	 },
	{"date", SQL_DATE, 10, "'", "'", NULL,
	 1, 0, 3, 0, 0, 0, "date", 0, 0, 0,
	 SQL_DATE, 0, 0, FIELD_TYPE_DATE, 0
	 /* 10 */
	 },
	{"time", SQL_TIME, 6, "'", "'", NULL,
	 1, 0, 3, 0, 0, 0, "time", 0, 0, 0,
	 SQL_TIME, 0, 0, FIELD_TYPE_TIME, 0
	 /* 11 */
	 },
	{"datetime", SQL_TIMESTAMP, 21, "'", "'", NULL,
	 1, 0, 3, 0, 0, 0, "datetime", 0, 0, 0,
	 SQL_TIMESTAMP, 0, 0, FIELD_TYPE_DATETIME, 0
	 /* 12 */
	 },
	{"year", SQL_SMALLINT, 4, NULL, NULL, NULL,
	 1, 0, 3, 0, 0, 0, "year", 0, 0, 10,
	 SQL_SMALLINT, 0, 0, FIELD_TYPE_YEAR, 0
	 /* 13 */
	 },
	{"date", SQL_DATE, 10, "'", "'", NULL,
	 1, 0, 3, 0, 0, 0, "date", 0, 0, 0,
	 SQL_DATE, 0, 0, FIELD_TYPE_NEWDATE, 0
	 /* 14 */
	 },
	{"enum", SQL_VARCHAR, 255, "'", "'", NULL,
	 1, 0, 1, 0, 0, 0, "enum(value1,value2,value3...)", 0, 0, 0,
	 0, 0, 0, FIELD_TYPE_ENUM, 0
	 /* 15 */
	 },
	{"set", SQL_VARCHAR, 255, "'", "'", NULL,
	 1, 0, 1, 0, 0, 0, "set(value1,value2,value3...)", 0, 0, 0,
	 0, 0, 0, FIELD_TYPE_SET, 0
	 /* 16 */
	 },
	{"blob", SQL_LONGVARBINARY, 65535, "'", "'", NULL,
	 1, 0, 3, 0, 0, 0, "binary large object (0-65535)", 0, 0, 0,
	 SQL_LONGVARBINARY, 0, 0, FIELD_TYPE_BLOB, 0
	 /* 17 */
	 },
	{"tinyblob", SQL_VARBINARY, 255, "'", "'", NULL,
	 1, 0, 3, 0, 0, 0, "binary large object (0-255) ", 0, 0, 0,
	 SQL_VARBINARY, 0, 0, FIELD_TYPE_TINY_BLOB, 0
	 /* 18 */
	 },
	{"mediumblob", SQL_LONGVARBINARY, 16777215, "'", "'", NULL,
	 1, 0, 3, 0, 0, 0, "binary large object", 0, 0, 0,
	 SQL_LONGVARBINARY, 0, 0, FIELD_TYPE_MEDIUM_BLOB, 0
	 /* 19 */
	 },
	{"longblob", SQL_LONGVARBINARY, 2147483647, "'", "'", NULL,
	 1, 0, 3, 0, 0, 0, "binary large object, use mediumblob instead",
	 0, 0, 0,
	 SQL_LONGVARBINARY, 0, 0, FIELD_TYPE_LONG_BLOB, 0
	 /* 20 */
	 },
	{"char", SQL_CHAR, 255, "'", "'", "max length",
	 1, 0, 3, 0, 0, 0, "string", 0, 0, 0,
	 SQL_CHAR, 0, 0, FIELD_TYPE_STRING, 0
	 /* 21 */
	 },

	{"decimal", SQL_NUMERIC, 15, NULL, NULL, "precision,scale",
	 1, 0, 3, 0, 0, 0, "double", 0, 6, 2,
	 SQL_NUMERIC, 0, 0, FIELD_TYPE_DECIMAL, 1},
	/*
	   { "tinyint", SQL_BIT, 3, NULL, NULL, NULL,
	   1, 0, 1, 0, 0, 0, "Tiny integer", 0, 0, 10,
	   FIELD_TYPE_TINY, 1
	   },
	 */
	{"tinyint unsigned", SQL_TINYINT, 3, NULL, NULL, NULL,
	 1, 0, 3, 1, 0, 0, "Tiny integer unsigned", 0, 0, 10,
	 SQL_TINYINT, 0, 0, FIELD_TYPE_TINY, 1},
	{"smallint unsigned", SQL_SMALLINT, 5, NULL, NULL, NULL,
	 1, 0, 3, 1, 0, 0, "Short integer unsigned", 0, 0, 10,
	 SQL_SMALLINT, 0, 0, FIELD_TYPE_SHORT, 1},
	{"middleint unsigned", SQL_INTEGER, 8, NULL, NULL, NULL,
	 1, 0, 3, 1, 0, 0, "Medium integer unsigned", 0, 0, 10,
	 SQL_INTEGER, 0, 0, FIELD_TYPE_INT24, 1},
	{"int unsigned", SQL_INTEGER, 10, NULL, NULL, NULL,
	 1, 0, 3, 1, 0, 0, "integer unsigned", 0, 0, 10,
	 SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1},
	{"int", SQL_INTEGER, 10, NULL, NULL, NULL,
	 1, 0, 3, 0, 0, 0, "integer", 0, 0, 10,
	 SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1},
	{"integer unsigned", SQL_INTEGER, 10, NULL, NULL, NULL,
	 1, 0, 3, 1, 0, 0, "integer", 0, 0, 10,
	 SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1},
	{"bigint unsigned", SQL_BIGINT, 20, NULL, NULL, NULL,
	 1, 0, 3, 1, 0, 0, "Longlong integer unsigned", 0, 0, 10,
	 SQL_BIGINT, 0, 0, FIELD_TYPE_LONGLONG, 1},
	{"text", SQL_LONGVARCHAR, 65535, "'", "'", NULL,
	 1, 0, 3, 0, 0, 0, "large text object (0-65535)", 0, 0, 0,
	 SQL_LONGVARCHAR, 0, 0, FIELD_TYPE_BLOB, 0},
	{"mediumtext", SQL_LONGVARCHAR, 16777215, "'", "'", NULL,
	 1, 0, 3, 0, 0, 0, "large text object", 0, 0, 0,
	 SQL_LONGVARCHAR, 0, 0, FIELD_TYPE_MEDIUM_BLOB, 0}

	/* BEGIN MORE STUFF */
	,


	{"mediumint unsigned auto_increment", SQL_INTEGER, 8, NULL, NULL,
	 NULL,
	 0, 0, 3, 1, 0, 1, "Medium integer unsigned auto_increment", 0, 0,
	 10,
	 SQL_INTEGER, 0, 0, FIELD_TYPE_INT24, 1,
	 },

	{"tinyint unsigned auto_increment", SQL_TINYINT, 3, NULL, NULL,
	 NULL,
	 0, 0, 3, 1, 0, 1, "tinyint unsigned auto_increment", 0, 0, 10,
	 SQL_TINYINT, 0, 0, FIELD_TYPE_TINY, 1},

	{"smallint auto_increment", SQL_SMALLINT, 5, NULL, NULL, NULL,
	 0, 0, 3, 0, 0, 1, "smallint auto_increment", 0, 0, 10,
	 SQL_SMALLINT, 0, 0, FIELD_TYPE_SHORT, 1},

	{"int unsigned auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
	 0, 0, 3, 1, 0, 1, "integer unsigned auto_increment", 0, 0, 10,
	 SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1},

	{"mediumint", SQL_INTEGER, 7, NULL, NULL, NULL,
	 1, 0, 3, 0, 0, 0, "Medium integer", 0, 0, 10,
	 SQL_INTEGER, 0, 0, FIELD_TYPE_INT24, 1},

	{"bit", SQL_BIT, 1, NULL, NULL, NULL,
	 1, 0, 3, 0, 0, 0, "char(1)", 0, 0, 0,
	 SQL_BIT, 0, 0, FIELD_TYPE_TINY, 0},

	{"numeric", SQL_NUMERIC, 19, NULL, NULL, "precision,scale",
	 1, 0, 3, 0, 0, 0, "numeric", 0, 19, 10,
	 SQL_NUMERIC, 0, 0, FIELD_TYPE_DECIMAL, 1,
	 },

	{"integer unsigned auto_increment", SQL_INTEGER, 10, NULL, NULL,
	 NULL,
	 0, 0, 3, 1, 0, 1, "integer unsigned auto_increment", 0, 0, 10,
	 SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1,
	 },

	{"mediumint unsigned", SQL_INTEGER, 8, NULL, NULL, NULL,
	 1, 0, 3, 1, 0, 0, "Medium integer unsigned", 0, 0, 10,
	 SQL_INTEGER, 0, 0, FIELD_TYPE_INT24, 1},

	{"smallint unsigned auto_increment", SQL_SMALLINT, 5, NULL, NULL,
	 NULL,
	 0, 0, 3, 1, 0, 1, "smallint unsigned auto_increment", 0, 0, 10,
	 SQL_SMALLINT, 0, 0, FIELD_TYPE_SHORT, 1},

	{"int auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
	 0, 0, 3, 0, 0, 1, "integer auto_increment", 0, 0, 10,
	 SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1},

	{"long varbinary", SQL_LONGVARBINARY, 16777215, "0x", NULL, NULL,
	 1, 0, 3, 0, 0, 0, "mediumblob", 0, 0, 0,
	 SQL_LONGVARBINARY, 0, 0, FIELD_TYPE_LONG_BLOB, 0},

	{"double auto_increment", SQL_FLOAT, 15, NULL, NULL, NULL,
	 0, 0, 3, 0, 0, 1, "double auto_increment", 0, 4, 2,
	 SQL_FLOAT, 0, 0, FIELD_TYPE_DOUBLE, 1},

	{"double auto_increment", SQL_DOUBLE, 15, NULL, NULL, NULL,
	 0, 0, 3, 0, 0, 1, "double auto_increment", 0, 4, 10,
	 SQL_DOUBLE, 0, 0, FIELD_TYPE_DOUBLE, 1},

	{"integer auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
	 0, 0, 3, 0, 0, 1, "integer auto_increment", 0, 0, 10,
	 SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1,
	 },

	{"bigint auto_increment", SQL_BIGINT, 19, NULL, NULL, NULL,
	 0, 0, 3, 0, 0, 1, "bigint auto_increment", 0, 0, 10,
	 SQL_BIGINT, 0, 0, FIELD_TYPE_LONGLONG, 1},

	{"bit auto_increment", SQL_BIT, 1, NULL, NULL, NULL,
	 0, 0, 3, 0, 0, 1, "char(1) auto_increment", 0, 0, 0,
	 SQL_BIT, 0, 0, FIELD_TYPE_TINY, 1},

	{"mediumint auto_increment", SQL_INTEGER, 7, NULL, NULL, NULL,
	 0, 0, 3, 0, 0, 1, "Medium integer auto_increment", 0, 0, 10,
	 SQL_INTEGER, 0, 0, FIELD_TYPE_INT24, 1},

	{"float auto_increment", SQL_REAL, 7, NULL, NULL, NULL,
	 0, 0, 0, 0, 0, 1, "float auto_increment", 0, 2, 10,
	 SQL_FLOAT, 0, 0, FIELD_TYPE_FLOAT, 1},

	{"long varchar", SQL_LONGVARCHAR, 16777215, "'", "'", NULL,
	 1, 0, 3, 0, 0, 0, "mediumtext", 0, 0, 0,
	 SQL_LONGVARCHAR, 0, 0, FIELD_TYPE_MEDIUM_BLOB, 1},

	{"tinyint auto_increment", SQL_TINYINT, 3, NULL, NULL, NULL,
	 0, 0, 3, 0, 0, 1, "tinyint auto_increment", 0, 0, 10,
	 SQL_TINYINT, 0, 0, FIELD_TYPE_TINY, 1},

	{"bigint unsigned auto_increment", SQL_BIGINT, 20, NULL, NULL,
	 NULL,
	 0, 0, 3, 1, 0, 1, "bigint unsigned auto_increment", 0, 0, 10,
	 SQL_BIGINT, 0, 0, FIELD_TYPE_LONGLONG, 1},

/* END MORE STUFF */
};

/*  The order of the following is important: The first column of a given
 *  data_type is choosen to represent all columns of the same type. */

const sql_type_info_t* native2sql(int t)
{
	switch (t) {

	case FIELD_TYPE_VAR_STRING:	return &SQL_GET_TYPE_INFO_values[0];
	case FIELD_TYPE_DECIMAL:	return &SQL_GET_TYPE_INFO_values[1];
	case FIELD_TYPE_TINY:		return &SQL_GET_TYPE_INFO_values[2];
	case FIELD_TYPE_SHORT:		return &SQL_GET_TYPE_INFO_values[3];
	case FIELD_TYPE_LONG:		return &SQL_GET_TYPE_INFO_values[4];
	case FIELD_TYPE_FLOAT:		return &SQL_GET_TYPE_INFO_values[5];

		/* 6  */
	case FIELD_TYPE_DOUBLE:		return &SQL_GET_TYPE_INFO_values[7];
	case FIELD_TYPE_TIMESTAMP:	return &SQL_GET_TYPE_INFO_values[8];
	case FIELD_TYPE_LONGLONG:	return &SQL_GET_TYPE_INFO_values[9];
	case FIELD_TYPE_INT24:		return &SQL_GET_TYPE_INFO_values[10];
	case FIELD_TYPE_DATE:		return &SQL_GET_TYPE_INFO_values[11];
	case FIELD_TYPE_TIME:		return &SQL_GET_TYPE_INFO_values[12];
	case FIELD_TYPE_DATETIME:	return &SQL_GET_TYPE_INFO_values[13];
	case FIELD_TYPE_YEAR:		return &SQL_GET_TYPE_INFO_values[14];
	case FIELD_TYPE_NEWDATE:	return &SQL_GET_TYPE_INFO_values[15];
	case FIELD_TYPE_ENUM:		return &SQL_GET_TYPE_INFO_values[16];
	case FIELD_TYPE_SET:		return &SQL_GET_TYPE_INFO_values[17];
	case FIELD_TYPE_BLOB:		return &SQL_GET_TYPE_INFO_values[18];
	case FIELD_TYPE_TINY_BLOB:	return &SQL_GET_TYPE_INFO_values[19];
	case FIELD_TYPE_MEDIUM_BLOB:	return &SQL_GET_TYPE_INFO_values[20];
	case FIELD_TYPE_LONG_BLOB:	return &SQL_GET_TYPE_INFO_values[21];
	case FIELD_TYPE_STRING:		return &SQL_GET_TYPE_INFO_values[22];
	default:			return &SQL_GET_TYPE_INFO_values[0];

	}
}

#define SQL_GET_TYPE_INFO_num \
        (sizeof(SQL_GET_TYPE_INFO_values)/sizeof(sql_type_info_t))


/*XXX: This is just a temp implementation as it was in dbd_db quote
  This will be replaced with a switch statement */

const sql_type_info_t *sql_type_data(const int tp)
{
	int i;
	for (i = 0; i < SQL_GET_TYPE_INFO_num; i++) {
		const sql_type_info_t *t = &SQL_GET_TYPE_INFO_values[i];
		if (t->data_type == tp)
			return  t;
		
	}
	return NULL;
}
#define PV_PUSH(c)                              \
    if (c) {                                    \
	sv = newSVpv((char*) (c), 0);           \
	SvREADONLY_on(sv);                      \
    } else {                                    \
        sv = &sv_undef;                         \
    }                                           \
    av_push(row, sv);

#define IV_PUSH(i) sv = newSViv((i)); SvREADONLY_on(sv); av_push(row, sv);


AV* build_type_info_all (void)
{
	AV *av = newAV();
	AV *row;
	HV *hv;
	SV *sv;
	int i;
	const char *cols[] = {
		"TYPE_NAME",
		"DATA_TYPE",
		"COLUMN_SIZE",
		"LITERAL_PREFIX",
		"LITERAL_SUFFIX",
		"CREATE_PARAMS",
		"NULLABLE",
		"CASE_SENSITIVE",
		"SEARCHABLE",
		"UNSIGNED_ATTRIBUTE",
		"FIXED_PREC_SCALE",
		"AUTO_UNIQUE_VALUE",
		"LOCAL_TYPE_NAME",
		"MINIMUM_SCALE",
		"MAXIMUM_SCALE",
		"NUM_PREC_RADIX",
		"SQL_DATATYPE",
		"SQL_DATETIME_SUB",
		"INTERVAL_PRECISION",
		"mysql_native_type",
		"mysql_is_num"
	};

	hv = newHV();
	av_push(av, newRV_noinc((SV *) hv));
	for (i = 0; i < (sizeof(cols) / sizeof(const char *)); i++) {
		if (!hv_store
		    (hv, (char *) cols[i], strlen(cols[i]), newSViv(i),
		     0)) {
			SvREFCNT_dec((SV *) av);
			return Nullav;
		}
	}
	for (i = 0; i < SQL_GET_TYPE_INFO_num; i++) {
		const sql_type_info_t *t = &SQL_GET_TYPE_INFO_values[i];

		row = newAV();
		av_push(av, newRV_noinc((SV *) row));
		PV_PUSH(t->type_name);
		IV_PUSH(t->data_type);
		IV_PUSH(t->column_size);
		PV_PUSH(t->literal_prefix);
		PV_PUSH(t->literal_suffix);
		PV_PUSH(t->create_params);
		IV_PUSH(t->nullable);
		IV_PUSH(t->case_sensitive);
		IV_PUSH(t->searchable);
		IV_PUSH(t->unsigned_attribute);
		IV_PUSH(t->fixed_prec_scale);
		IV_PUSH(t->auto_unique_value);
		PV_PUSH(t->local_type_name);
		IV_PUSH(t->minimum_scale);
		IV_PUSH(t->maximum_scale);
		if (t->num_prec_radix) {
			IV_PUSH(t->num_prec_radix);
		} else {
			av_push(row, &sv_undef);
		}
		IV_PUSH(t->sql_datatype);	/* SQL_DATATYPE */
		IV_PUSH(t->sql_datetime_sub);	/* SQL_DATETIME_SUB */
		IV_PUSH(t->interval_precision);	/* INTERVAL_PERCISION */
		IV_PUSH(t->native_type);
		IV_PUSH(t->is_num);
	}
	return av;
}
