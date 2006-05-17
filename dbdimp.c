/*
 *  DBD::mysql - DBI driver for the mysql database
 *
 *  Copyright (c) 2005       Patrick Galbraith
 *  Copyright (c) 2003       Rudolf Lippan
 *  Copyright (c) 1997-2003  Jochen Wiedmann
 *
 *  You may distribute this under the terms of either the GNU General Public
 *  License or the Artistic License, as specified in the Perl README file.
 *
 *  $Id$
 */


#ifdef WIN32
#include "windows.h"
#include "winsock.h"
#endif

#include "dbdimp.h"

#if defined(WIN32)  &&  defined(WORD)
    /*  Don't exactly know who's responsible for defining WORD ... :-(  */
#undef WORD
typedef short WORD;
#endif



DBISTATE_DECLARE;

typedef struct sql_type_info_s
{
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
} sql_type_info_t;


/*

  This function manually counts the number of placeholders in an SQL statement,
  used for emulated prepare statements < 4.1.3

*/
static int
count_params(char *statement)
{
  char* ptr = statement;
  int num_params = 0;
  char c;

  while ( (c = *ptr++) )
  {
    switch (c) {
    case '`':
    case '"':
    case '\'':
      /* Skip string */
      {
        char end_token = c;
        while ((c = *ptr)  &&  c != end_token)
        {
          if (c == '\\')
            if (! *ptr)
              continue;

          ++ptr;
        }
        if (c)
          ++ptr;
        break;
      }

    case '?':
      ++num_params;
      break;

    default:
      break;
    }
  }
  return num_params;
}

/*
  allocate memory in statement handle per number of placeholders
*/
static imp_sth_ph_t *alloc_param(int num_params)
{
  imp_sth_ph_t *params;

  if (num_params)
    Newz(908, params, num_params, imp_sth_ph_t);
  else
    params= NULL;

  return params;
}


#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
/* 

  allocate memory in MYSQL_BIND bind structure per
  number of placeholders
*/
static MYSQL_BIND *alloc_bind(int num_params)
{
  MYSQL_BIND *bind;

  if (num_params)
    Newz(908, bind, num_params, MYSQL_BIND);
  else
    bind= NULL;

  return bind;
}

/*
  allocate memory in fbind imp_sth_phb_t structure per
  number of placeholders
*/
static imp_sth_phb_t *alloc_fbind(int num_params)
{
  imp_sth_phb_t *fbind;

  if (num_params)
    Newz(908, fbind, num_params, imp_sth_phb_t);
  else
    fbind= NULL;

  return fbind;
}

/*
  alloc memory for imp_sth_fbh_t fbuffer per number of fields
*/
static imp_sth_fbh_t *alloc_fbuffer(int num_fields)
{
  imp_sth_fbh_t *fbh;

  if (num_fields)
    Newz(908, fbh, num_fields, imp_sth_fbh_t);
  else
    fbh= NULL;

  return fbh;
}

/*
  free MYSQL_BIND bind struct
*/
static void FreeBind(MYSQL_BIND* bind)
{
  if (bind)
    Safefree(bind);
  else
  {
    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP, "\t\tFREE ERROR BIND!\n");
    fprintf(stderr,"FREE ERROR BIND!");
  }
}

/*
   free imp_sth_phb_t fbind structure
*/
static void FreeFBind(imp_sth_phb_t *fbind)
{
  if (fbind)
    Safefree(fbind);
  else
  {
    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP, "\t\tFREE ERROR FBIND!\n");
    fprintf(stderr,"FREE ERROR FBIND!");
  }
}

/* 
  free imp_sth_fbh_t fbh structure
*/
static void FreeFBuffer(imp_sth_fbh_t * fbh)
{
  if (fbh)
    Safefree(fbh);
  else
    fprintf(stderr,"FREE ERROR FBUFFER!");
}

#endif

/*
  free statement param structure per num_params
*/
static void
FreeParam(imp_sth_ph_t *params, int num_params)
{
  if (params)
  {
    int i;
    for (i= 0;  i < num_params;  i++)
    {
      imp_sth_ph_t *ph= params+i;
      if (ph->value)
      {
        (void) SvREFCNT_dec(ph->value);
        ph->value= NULL;
      }
    }
    Safefree(params);
  }
}

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
/* 
  Convert a MySQL type to a type that perl can handle

  NOTE: In the future we may want to return a struct with a lot of
  information for each type
*/

static enum enum_field_types mysql_to_perl_type(enum enum_field_types type)
{
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "\t-> mysql_to_perl_type\n");

  switch (type) {
  case MYSQL_TYPE_DOUBLE:
  case MYSQL_TYPE_FLOAT:
    return MYSQL_TYPE_DOUBLE;

  case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_TINY:
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_YEAR:
#if MYSQL_VERSION_ID > NEW_DATATYPE_VERSION
  case MYSQL_TYPE_BIT:
#endif
    return MYSQL_TYPE_LONG;

  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_LONGLONG:			/* No longlong in perl */
  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_TIME:
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_NEWDATE:
  case MYSQL_TYPE_VAR_STRING:
#if MYSQL_VERSION_ID > GEO_DATATYPE_VERSION
  case MYSQL_TYPE_GEOMETRY:
#endif
#if MYSQL_VERSION_ID > NEW_DATATYPE_VERSION
  case MYSQL_TYPE_VARCHAR:
#endif
  case MYSQL_TYPE_STRING:
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_TIMESTAMP:
  /* case MYSQL_TYPE_UNKNOWN: */
    return MYSQL_TYPE_STRING;

  default:
    return MYSQL_TYPE_STRING;    /* MySQL can handle all types as strings */
  }
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "\t\tcol_type => %d\n", type);
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "\t<- mysql_to_perl_type\n");
}
#endif

#if defined(DBD_MYSQL_EMBEDDED)
/* 
  count embedded options
*/
int count_embedded_options(char *st)
{
  int rc;
  char c;
  char *ptr;

  ptr= st;
  rc= 0;

  if (st)
  {
    while ((c= *ptr++))
    {
      if (c == ',')
        rc++;
    }
    rc++;
  }

  return rc;
}

/*
  Free embbedded options
*/
int free_embedded_options(char ** options_list, int options_count)
{
  int i;

  for (i= 0; i < options_count; i++)
  {
    if (options_list[i])
      free(options_list[i]);
  }
  free(options_list);

  return 1;
}

/*
 Print out embbedded option settings

*/
int print_embedded_options(char ** options_list, int options_count)
{
  int i;

  for (i=0; i<options_count; i++)
  {
    if (options_list[i])
        PerlIO_printf(DBILOGFP,
                      "Embedded server, parameter[%d]=%s\n",
                      i, options_list[i]);
  }
  return 1;
}

/*

*/
char **fill_out_embedded_options(char *options,
                                 int options_type,
                                 int slen, int cnt)
{
  int  ind, len;
  char c;
  char *ptr;
  char **options_list= NULL;

  if (!(options_list= (char **) calloc(cnt, sizeof(char *))))
  {
    PerlIO_printf(DBILOGFP,
                  "Initialize embedded server. Out of memory \n");
    return NULL;
  }

  ptr= options;
  ind= 0;

  if (options_type == 0)
  {
    /* server_groups list NULL terminated */
    options_list[cnt]= (char *) NULL;
  }

  if (options_type == 1)
  {
    /* first item in server_options list is ignored. fill it with \0 */
    if (!(options_list[0]= calloc(1,sizeof(char))))
    {
      if (dbis->debug >= 2)
        PerlIO_printf(DBILOGFP,
                      "Initialize embedded server. Out of memory \n");
      return NULL;
    }
    ind++;
  }

  while ((c= *ptr++))
  {
    slen--;
    if (c == ',' || !slen)
    {
      len= ptr - options;
      if (c == ',')
        len--;
      if (!(options_list[ind]=calloc(len+1,sizeof(char))))
      {
        if (dbis->debug >= 2)
          PerlIO_printf(DBILOGFP,
                        "Initialize embedded server. Out of memory\n");
        return NULL;
      }
      strncpy(options_list[ind], options, len);
      ind++;
      options= ptr;
    }
  }
  return options_list;
}
#endif

/* 
  constructs an SQL statement previously prepared with
  actual values replacing placeholders
*/
static char *parse_params(
                          MYSQL *sock,
                          char *statement,
                          STRLEN *slen_ptr,
                          imp_sth_ph_t* params,
                          int num_params,
                          bool bind_type_guessing)
{

  bool seen_neg, seen_dec;
  char *salloc, *statement_ptr;
  char *statement_ptr_end, testchar, *ptr, *valbuf;
  int alen, i, j;
  int slen= *slen_ptr;
  int limit_flag= 0;
  STRLEN vallen;
  imp_sth_ph_t *ph;

  /* I want to add mysql DBUG_ENTER (DBUG_<> macros) */
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
                  "---> parse_params with statement %s num params %d\n",
                  statement, num_params);

  if (num_params == 0)
  {
    return NULL;
  }

  while (isspace(*statement))
  {
    ++statement;
    --slen;
  }
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
                  "     parse_params slen %d\n", slen);

  /* Calculate the number of bytes being allocated for the statement */
  alen= slen;

  for (i= 0, ph= params; i < num_params; i++, ph++)
  {
    if (!ph->value  ||  !SvOK(ph->value))
      alen+= 3;  /* Erase '?', insert 'NULL' */
    else
    {
      valbuf= SvPV(ph->value, vallen);
      alen+= 2+vallen+1;
      /* this will most likely not happen since line 214 */
      /* of mysql.xs hardcodes all types to SQL_VARCHAR */
      if (!ph->type)
      {
        if ( bind_type_guessing > 1 )
        {
          valbuf= SvPV(ph->value, vallen);
          ph->type= SQL_INTEGER;

          /* patch from Dragonchild */
          seen_neg= 0;
          seen_dec= 0;
          for (j= 0; j < (int)vallen; ++j)
          {
            testchar= *(valbuf+j);
            if ('-' == testchar)
            {
              if (seen_neg)
              {
                ph->type= SQL_VARCHAR;
                break;
              }
              else if (j)
              {
                ph->type= SQL_VARCHAR;
                break;
              }
              seen_neg= 1;
            }
            else if ('.' == testchar)
            {
              if (seen_dec)
              {
                ph->type= SQL_VARCHAR;
                break;
              }
              seen_dec= 1;
            }
            else if (!isdigit(testchar))
            {
              ph->type= SQL_VARCHAR;
              break;
            }
          }
        }
        else if (bind_type_guessing)
          ph->type= SvNIOK(ph->value) ? SQL_INTEGER : SQL_VARCHAR;
        else
          ph->type= SQL_VARCHAR;
      }
    }
  }

  /* Allocate memory, why *2, well, because we have ptr and statement_ptr */
  New(908, salloc, alen*2, char);
  ptr= salloc;

  i= 0;
 /* Now create the statement string; compare count_params above */
  statement_ptr_end= (statement_ptr= statement)+ slen;

  while (statement_ptr < statement_ptr_end)
  {
    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP,
                    "     parse_params statement_ptr %08lx = %s \
                    statement_ptr_end %08lx\n",
                    statement_ptr, statement_ptr, statement_ptr_end);
    /* LIMIT should be the last part of the query, in most cases */
    if (! limit_flag)
    {
      /*
        it would be good to be able to handle any number of cases and orders
      */
      if ((*statement_ptr == 'l' || *statement_ptr == 'L') &&
          (!strncmp(statement_ptr+1, "imit ?", 6) ||
           !strncmp(statement_ptr+1, "IMIT ?", 6)))
      {
        limit_flag = 1;
      }
    }
    switch (*statement_ptr)
    {
      case '`':
      case '\'':
      case '"':
      /* Skip string */
      {
        char endToken = *statement_ptr++;
        *ptr++ = endToken;
        while (statement_ptr != statement_ptr_end &&
               *statement_ptr != endToken)
        {
          if (*statement_ptr == '\\')
          {
            *ptr++ = *statement_ptr++;
            if (statement_ptr == statement_ptr_end)
	      break;
	  }
          *ptr++= *statement_ptr++;
	}
	if (statement_ptr != statement_ptr_end)
          *ptr++= *statement_ptr++;
      }
      break;

      case '?':
        /* Insert parameter */
        statement_ptr++;
        if (i >= num_params)
        {
          break;
        }

        ph = params+ (i++);
        if (!ph->value  ||  !SvOK(ph->value))
        {
          *ptr++ = 'N';
          *ptr++ = 'U';
          *ptr++ = 'L';
          *ptr++ = 'L';
        }
        else
        {
          int is_num = FALSE;
          int c;

          valbuf= SvPV(ph->value, vallen);
          if (valbuf)
          {
            switch (ph->type)
            {
              case SQL_NUMERIC:
              case SQL_DECIMAL:
              case SQL_INTEGER:
              case SQL_SMALLINT:
              case SQL_FLOAT:
              case SQL_REAL:
              case SQL_DOUBLE:
              case SQL_BIGINT:
              case SQL_TINYINT:
                is_num = TRUE;
                break;
            }

            /* we're at the end of the query, so any placeholders if */
            /* after a LIMIT clause will be numbers and should not be quoted */
            if (limit_flag == 1)
              is_num = TRUE;

            if (!is_num)
            {
              *ptr++ = '\'';
              ptr += mysql_real_escape_string(sock, ptr, valbuf, vallen);
              *ptr++ = '\'';
            }
            else
            {
              while (vallen--)
              {
		c = *valbuf++;
		if ((c < '0' || c > '9') && c != ' ')
		  break;
		*ptr++= c;
              }
            }
          }
        }
        break;

	/* in case this is a nested LIMIT */
      case ')':
        limit_flag = 0;
	*ptr++ = *statement_ptr++;
        break;

      default:
        *ptr++ = *statement_ptr++;
        break;

    }
  }

  *slen_ptr = ptr - salloc;
  *ptr++ = '\0';
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "<--- parse_params\n");

  return(salloc);
}

int bind_param(imp_sth_ph_t *ph, SV *value, IV sql_type)
{
  if (ph->value)
    (void) SvREFCNT_dec(ph->value);

  ph->value = newSVsv(value);

  if (sql_type)
    ph->type = sql_type;

  return TRUE;
}

static const sql_type_info_t SQL_GET_TYPE_INFO_values[]= {
  { "varchar",    SQL_VARCHAR,                    255, "'",  "'",  "max length",
    1, 0, 3, 0, 0, 0, "variable length string",
    0, 0, 0,
    SQL_VARCHAR, 0, 0,
    FIELD_TYPE_VAR_STRING,  0,
    /* 0 */
  },
  { "decimal",   SQL_DECIMAL,                      15, NULL, NULL, "precision,scale",
    1, 0, 3, 0, 0, 0, "double",
    0, 6, 2,
    SQL_DECIMAL, 0, 0,
    FIELD_TYPE_DECIMAL,     1
    /* 1 */
  },
  { "tinyint",   SQL_TINYINT,                       3, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Tiny integer",
    0, 0, 10,
    SQL_TINYINT, 0, 0,
    FIELD_TYPE_TINY,        1
    /* 2 */
  },
  { "smallint",  SQL_SMALLINT,                      5, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Short integer",
    0, 0, 10,
    SQL_SMALLINT, 0, 0,
    FIELD_TYPE_SHORT,       1
    /* 3 */
  },
  { "integer",   SQL_INTEGER,                      10, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    FIELD_TYPE_LONG,        1
    /* 4 */
  },
  { "float",     SQL_REAL,                          7,  NULL, NULL, NULL,
    1, 0, 0, 0, 0, 0, "float",
    0, 2, 10,
    SQL_FLOAT, 0, 0,
    FIELD_TYPE_FLOAT,       1
    /* 5 */
  },
  { "double",    SQL_FLOAT,                       15,  NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "double",
    0, 4, 2,
    SQL_FLOAT, 0, 0,
    FIELD_TYPE_DOUBLE,      1
    /* 6 */
  },
  { "double",    SQL_DOUBLE,                       15,  NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "double",
    0, 4, 10,
    SQL_DOUBLE, 0, 0,
    FIELD_TYPE_DOUBLE,      1
    /* 6 */
  },
  /*
    FIELD_TYPE_NULL ?
  */
  { "timestamp", SQL_TIMESTAMP,                    14, "'", "'", NULL,
    0, 0, 3, 0, 0, 0, "timestamp",
    0, 0, 0,
    SQL_TIMESTAMP, 0, 0,
    FIELD_TYPE_TIMESTAMP,   0
    /* 7 */
  },
  { "bigint",    SQL_BIGINT,                       19, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Longlong integer",
    0, 0, 10,
    SQL_BIGINT, 0, 0,
    FIELD_TYPE_LONGLONG,    1
    /* 8 */
  },
  { "middleint", SQL_INTEGER,                       8, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Medium integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    FIELD_TYPE_INT24,       1
    /* 9 */
  },
  { "date",      SQL_DATE,                         10, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "date",
    0, 0, 0,
    SQL_DATE, 0, 0,
    FIELD_TYPE_DATE,        0
    /* 10 */
  },
  { "time",      SQL_TIME,                          6, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "time",
    0, 0, 0,
    SQL_TIME, 0, 0,
    FIELD_TYPE_TIME,        0
    /* 11 */
  },
  { "datetime",  SQL_TIMESTAMP,                    21, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "datetime",
    0, 0, 0,
    SQL_TIMESTAMP, 0, 0,
    FIELD_TYPE_DATETIME,    0
    /* 12 */
  },
  { "year",      SQL_SMALLINT,                      4, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "year",
    0, 0, 10,
    SQL_SMALLINT, 0, 0,
    FIELD_TYPE_YEAR,        0
    /* 13 */
  },
  { "date",      SQL_DATE,                         10, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "date",
    0, 0, 0,
    SQL_DATE, 0, 0,
    FIELD_TYPE_NEWDATE,     0
    /* 14 */
  },
  { "enum",      SQL_VARCHAR,                     255, "'",  "'",  NULL,
    1, 0, 1, 0, 0, 0, "enum(value1,value2,value3...)",
    0, 0, 0,
    0, 0, 0,
    FIELD_TYPE_ENUM,        0
    /* 15 */
  },
  { "set",       SQL_VARCHAR,                     255, "'",  "'",  NULL,
    1, 0, 1, 0, 0, 0, "set(value1,value2,value3...)",
    0, 0, 0,
    0, 0, 0,
    FIELD_TYPE_SET,         0
    /* 16 */
  },
  { "blob",       SQL_LONGVARBINARY,              65535, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object (0-65535)",
    0, 0, 0,
    SQL_LONGVARBINARY, 0, 0,
    FIELD_TYPE_BLOB,        0
    /* 17 */
  },
  { "tinyblob",  SQL_VARBINARY,                 255, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object (0-255) ",
    0, 0, 0,
    SQL_VARBINARY, 0, 0,
    FIELD_TYPE_TINY_BLOB,   0
    /* 18 */
  },
  { "mediumblob", SQL_LONGVARBINARY,           16777215, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object",
    0, 0, 0,
    SQL_LONGVARBINARY, 0, 0,
    FIELD_TYPE_MEDIUM_BLOB, 0
    /* 19 */
  },
  { "longblob",   SQL_LONGVARBINARY,         2147483647, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object, use mediumblob instead",
    0, 0, 0,
    SQL_LONGVARBINARY, 0, 0,
    FIELD_TYPE_LONG_BLOB,   0
    /* 20 */
  },
  { "char",       SQL_CHAR,                       255, "'",  "'",  "max length",
    1, 0, 3, 0, 0, 0, "string",
    0, 0, 0,
    SQL_CHAR, 0, 0,
    FIELD_TYPE_STRING,      0
    /* 21 */
  },

  { "decimal",            SQL_NUMERIC,            15,  NULL, NULL, "precision,scale",
    1, 0, 3, 0, 0, 0, "double",
    0, 6, 2,
    SQL_NUMERIC, 0, 0,
    FIELD_TYPE_DECIMAL,     1
  },
  /*
  { "tinyint",            SQL_BIT,                  3, NULL, NULL, NULL,
    1, 0, 1, 0, 0, 0, "Tiny integer",
    0, 0, 10, FIELD_TYPE_TINY,        1
  },
  */
  { "tinyint unsigned",   SQL_TINYINT,              3, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Tiny integer unsigned",
    0, 0, 10,
    SQL_TINYINT, 0, 0,
    FIELD_TYPE_TINY,        1
  },
  { "smallint unsigned",  SQL_SMALLINT,             5, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Short integer unsigned",
    0, 0, 10,
    SQL_SMALLINT, 0, 0,
    FIELD_TYPE_SHORT,       1
  },
  { "middleint unsigned", SQL_INTEGER,              8, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Medium integer unsigned",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    FIELD_TYPE_INT24,       1
  },
  { "int unsigned",       SQL_INTEGER,             10, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "integer unsigned",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    FIELD_TYPE_LONG,        1
  },
  { "int",                SQL_INTEGER,             10, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    FIELD_TYPE_LONG,        1
  },
  { "integer unsigned",   SQL_INTEGER,             10, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    FIELD_TYPE_LONG,        1
  },
  { "bigint unsigned",    SQL_BIGINT,              20, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Longlong integer unsigned",
    0, 0, 10,
    SQL_BIGINT, 0, 0,
    FIELD_TYPE_LONGLONG,    1
  },
  { "text",               SQL_LONGVARCHAR,      65535, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "large text object (0-65535)",
    0, 0, 0,
    SQL_LONGVARCHAR, 0, 0,
    FIELD_TYPE_BLOB,        0
  },
  { "mediumtext",         SQL_LONGVARCHAR,   16777215, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "large text object",
    0, 0, 0,
    SQL_LONGVARCHAR, 0, 0,
    FIELD_TYPE_MEDIUM_BLOB, 0
  }


 /* BEGIN MORE STUFF */
,


  { "mediumint unsigned auto_increment", SQL_INTEGER, 8, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "Medium integer unsigned auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, FIELD_TYPE_INT24, 1,
  },

  { "tinyint unsigned auto_increment", SQL_TINYINT, 3, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "tinyint unsigned auto_increment", 0, 0, 10,
    SQL_TINYINT, 0, 0, FIELD_TYPE_TINY, 1
  },

  { "smallint auto_increment", SQL_SMALLINT, 5, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "smallint auto_increment", 0, 0, 10,
    SQL_SMALLINT, 0, 0, FIELD_TYPE_SHORT, 1
  },

  { "int unsigned auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "integer unsigned auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1
  },

  { "mediumint", SQL_INTEGER, 7, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Medium integer", 0, 0, 10,
    SQL_INTEGER, 0, 0, FIELD_TYPE_INT24, 1
  },

  { "bit", SQL_BIT, 1, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "char(1)", 0, 0, 0,
    SQL_BIT, 0, 0, FIELD_TYPE_TINY, 0
  },

  { "numeric", SQL_NUMERIC, 19, NULL, NULL, "precision,scale",
    1, 0, 3, 0, 0, 0, "numeric", 0, 19, 10,
    SQL_NUMERIC, 0, 0, FIELD_TYPE_DECIMAL, 1,
  },

  { "integer unsigned auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "integer unsigned auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1,
  },

  { "mediumint unsigned", SQL_INTEGER, 8, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Medium integer unsigned", 0, 0, 10,
    SQL_INTEGER, 0, 0, FIELD_TYPE_INT24, 1
  },

  { "smallint unsigned auto_increment", SQL_SMALLINT, 5, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "smallint unsigned auto_increment", 0, 0, 10,
    SQL_SMALLINT, 0, 0, FIELD_TYPE_SHORT, 1
  },

  { "int auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "integer auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1
  },

  { "long varbinary", SQL_LONGVARBINARY, 16777215, "0x", NULL, NULL,
    1, 0, 3, 0, 0, 0, "mediumblob", 0, 0, 0,
    SQL_LONGVARBINARY, 0, 0, FIELD_TYPE_LONG_BLOB, 0
  },

  { "double auto_increment", SQL_FLOAT, 15, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "double auto_increment", 0, 4, 2,
    SQL_FLOAT, 0, 0, FIELD_TYPE_DOUBLE, 1
  },

  { "double auto_increment", SQL_DOUBLE, 15, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "double auto_increment", 0, 4, 10,
    SQL_DOUBLE, 0, 0, FIELD_TYPE_DOUBLE, 1
  },

  { "integer auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "integer auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1,
  },

  { "bigint auto_increment", SQL_BIGINT, 19, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "bigint auto_increment", 0, 0, 10,
    SQL_BIGINT, 0, 0, FIELD_TYPE_LONGLONG, 1
  },

  { "bit auto_increment", SQL_BIT, 1, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "char(1) auto_increment", 0, 0, 0,
    SQL_BIT, 0, 0, FIELD_TYPE_TINY, 1
  },

  { "mediumint auto_increment", SQL_INTEGER, 7, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "Medium integer auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, FIELD_TYPE_INT24, 1
  },

  { "float auto_increment", SQL_REAL, 7, NULL, NULL, NULL,
    0, 0, 0, 0, 0, 1, "float auto_increment", 0, 2, 10,
    SQL_FLOAT, 0, 0, FIELD_TYPE_FLOAT, 1
  },

  { "long varchar", SQL_LONGVARCHAR, 16777215, "'", "'", NULL,
    1, 0, 3, 0, 0, 0, "mediumtext", 0, 0, 0,
    SQL_LONGVARCHAR, 0, 0, FIELD_TYPE_MEDIUM_BLOB, 1
  },

  { "tinyint auto_increment", SQL_TINYINT, 3, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "tinyint auto_increment", 0, 0, 10,
    SQL_TINYINT, 0, 0, FIELD_TYPE_TINY, 1
  },

  { "bigint unsigned auto_increment", SQL_BIGINT, 20, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "bigint unsigned auto_increment", 0, 0, 10,
    SQL_BIGINT, 0, 0, FIELD_TYPE_LONGLONG, 1
  },

/* END MORE STUFF */
};

/* 
  static const sql_type_info_t* native2sql (int t)
*/
static const sql_type_info_t *native2sql(int t)
{
  switch (t) {
    case FIELD_TYPE_VAR_STRING:  return &SQL_GET_TYPE_INFO_values[0];
    case FIELD_TYPE_DECIMAL:     return &SQL_GET_TYPE_INFO_values[1];
    case FIELD_TYPE_TINY:        return &SQL_GET_TYPE_INFO_values[2];
    case FIELD_TYPE_SHORT:       return &SQL_GET_TYPE_INFO_values[3];
    case FIELD_TYPE_LONG:        return &SQL_GET_TYPE_INFO_values[4];
    case FIELD_TYPE_FLOAT:       return &SQL_GET_TYPE_INFO_values[5];

    /* 6  */
    case FIELD_TYPE_DOUBLE:      return &SQL_GET_TYPE_INFO_values[7];
    case FIELD_TYPE_TIMESTAMP:   return &SQL_GET_TYPE_INFO_values[8];
    case FIELD_TYPE_LONGLONG:    return &SQL_GET_TYPE_INFO_values[9];
    case FIELD_TYPE_INT24:       return &SQL_GET_TYPE_INFO_values[10];
    case FIELD_TYPE_DATE:        return &SQL_GET_TYPE_INFO_values[11];
    case FIELD_TYPE_TIME:        return &SQL_GET_TYPE_INFO_values[12];
    case FIELD_TYPE_DATETIME:    return &SQL_GET_TYPE_INFO_values[13];
    case FIELD_TYPE_YEAR:        return &SQL_GET_TYPE_INFO_values[14];
    case FIELD_TYPE_NEWDATE:     return &SQL_GET_TYPE_INFO_values[15];
    case FIELD_TYPE_ENUM:        return &SQL_GET_TYPE_INFO_values[16];
    case FIELD_TYPE_SET:         return &SQL_GET_TYPE_INFO_values[17];
    case FIELD_TYPE_BLOB:        return &SQL_GET_TYPE_INFO_values[18];
    case FIELD_TYPE_TINY_BLOB:   return &SQL_GET_TYPE_INFO_values[19];
    case FIELD_TYPE_MEDIUM_BLOB: return &SQL_GET_TYPE_INFO_values[20];
    case FIELD_TYPE_LONG_BLOB:   return &SQL_GET_TYPE_INFO_values[21];
    case FIELD_TYPE_STRING:      return &SQL_GET_TYPE_INFO_values[22];
    default:                     return &SQL_GET_TYPE_INFO_values[0];
  }
}


#define SQL_GET_TYPE_INFO_num \
	(sizeof(SQL_GET_TYPE_INFO_values)/sizeof(sql_type_info_t))


/***************************************************************************
 *
 *  Name:    dbd_init
 *
 *  Purpose: Called when the driver is installed by DBI
 *
 *  Input:   dbistate - pointer to the DBIS variable, used for some
 *               DBI internal things
 *
 *  Returns: Nothing
 *
 **************************************************************************/

void dbd_init(dbistate_t* dbistate)
{
    DBIS = dbistate;
}


/**************************************************************************
 *
 *  Name:    do_error, do_warn
 *
 *  Purpose: Called to associate an error code and an error message
 *           to some handle
 *
 *  Input:   h - the handle in error condition
 *           rc - the error code
 *           what - the error message
 *
 *  Returns: Nothing
 *
 **************************************************************************/

void do_error(SV* h, int rc, const char* what)
{
  D_imp_xxh(h);
  STRLEN lna;
  SV *errstr;

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "\t\t--> do_error\n");
  errstr= DBIc_ERRSTR(imp_xxh);
  sv_setiv(DBIc_ERR(imp_xxh), (IV)rc);	/* set err early	*/
  sv_setpv(errstr, what);
  DBIh_EVENT2(h, ERROR_event, DBIc_ERR(imp_xxh), errstr);
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "%s error %d recorded: %s\n",
    what, rc, SvPV(errstr,lna));
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "\t\t<-- do_error\n");
}

/*
  void do_warn(SV* h, int rc, char* what)
*/
void do_warn(SV* h, int rc, char* what)
{
  D_imp_xxh(h);
  STRLEN lna;

  SV *errstr = DBIc_ERRSTR(imp_xxh);
  sv_setiv(DBIc_ERR(imp_xxh), (IV)rc);	/* set err early	*/
  sv_setpv(errstr, what);
  DBIh_EVENT2(h, WARN_event, DBIc_ERR(imp_xxh), errstr);
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "%s warning %d recorded: %s\n",
    what, rc, SvPV(errstr,lna));
  warn("%s", what);
}
/* }}} */

#if defined(DBD_MYSQL_EMBEDDED)
 #define DBD_MYSQL_NAMESPACE "DBD::mysqlEmb::QUIET";
#else
 #define DBD_MYSQL_NAMESPACE "DBD::mysql::QUIET";
#endif

#define doquietwarn(s) \
  { \
    SV* sv = perl_get_sv(DBD_MYSQL_NAMESPACE, FALSE);  \
    if (!sv  ||  !SvTRUE(sv)) { \
      warn s; \
    } \
  }


/***************************************************************************
 *
 *  Name:    mysql_dr_connect
 *
 *  Purpose: Replacement for mysql_connect
 *
 *  Input:   MYSQL* sock - Pointer to a MYSQL structure being
 *             initialized
 *           char* mysql_socket - Name of a UNIX socket being used
 *             or NULL
 *           char* host - Host name being used or NULL for localhost
 *           char* port - Port number being used or NULL for default
 *           char* user - User name being used or NULL
 *           char* password - Password being used or NULL
 *           char* dbname - Database name being used or NULL
 *           char* imp_dbh - Pointer to internal dbh structure
 *
 *  Returns: The sock argument for success, NULL otherwise;
 *           you have to call do_error in the latter case.
 *
 **************************************************************************/

MYSQL *mysql_dr_connect(SV* dbh, MYSQL* sock, char* mysql_socket, char* host,
			char* port, char* user, char* password,
			char* dbname, imp_dbh_t *imp_dbh) {
  int portNr;
  MYSQL* result;

  /* per Monty, already in client.c in API */
  /* but still not exist in libmysqld.c */
#if defined(DBD_MYSQL_EMBEDDED)
   if (host && !*host) host = NULL;
#endif

  portNr= (port && *port) ? atoi(port) : 0;

  /* already in client.c in API */
  /* if (user && !*user) user = NULL; */
  /* if (password && !*password) password = NULL; */


  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
		  "imp_dbh->mysql_dr_connect: host = |%s|, port = %d," \
		  " uid = %s, pwd = %s\n",
		  host ? host : "NULL", portNr,
		  user ? user : "NULL",
		  password ? password : "NULL");

  {

#if defined(DBD_MYSQL_EMBEDDED)
    if (imp_dbh)
    {
      D_imp_drh_from_dbh;
      SV* sv = DBIc_IMP_DATA(imp_dbh);

      if (sv  &&  SvROK(sv))
      {
        SV** svp;
        STRLEN lna;
        char * options;
        int server_args_cnt= 0;
        int server_groups_cnt= 0;
        int rc= 0;

        char ** server_args = NULL;
        char ** server_groups = NULL;

        HV* hv = (HV*) SvRV(sv);

        if (SvTYPE(hv) != SVt_PVHV)
          return NULL;

        if (!imp_drh->embedded.state)
        {
          /* Init embedded server */
          if ((svp = hv_fetch(hv, "mysql_embedded_groups", 21, FALSE))  &&
              *svp  &&  SvTRUE(*svp))
          {
            options = SvPV(*svp, lna);
            imp_drh->embedded.groups=newSVsv(*svp);

            if ((server_groups_cnt=count_embedded_options(options)))
            {
              /* number of server_groups always server_groups+1 */
              server_groups=fill_out_embedded_options(options, 0, (int)lna, ++server_groups_cnt);
              if (dbis->debug >= 2)
              {
                PerlIO_printf(DBILOGFP,
                              "Groups names passed to embedded server:\n");
                print_embedded_options(server_groups, server_groups_cnt);
              }
            }
          }
 
          if ((svp = hv_fetch(hv, "mysql_embedded_options", 22, FALSE))  &&
              *svp  &&  SvTRUE(*svp))
          {
            options = SvPV(*svp, lna);
            imp_drh->embedded.args=newSVsv(*svp);

            if ((server_args_cnt=count_embedded_options(options)))
            {
              /* number of server_options always server_options+1 */
              server_args=fill_out_embedded_options(options, 1, (int)lna, ++server_args_cnt);
              if (dbis->debug >= 2)
              {
                PerlIO_printf(DBILOGFP, "Server options passed to embedded server:\n");
                print_embedded_options(server_args, server_args_cnt);
              }
            }
          }
          if (mysql_server_init(server_args_cnt, server_args, server_groups))
          {
            do_warn(dbh, AS_ERR_EMBEDDED, "Embedded server was not started. \
                    Could not initialize environment.");
            return NULL;
          }
          imp_drh->embedded.state=1;

          if (server_args_cnt)
            free_embedded_options(server_args, server_args_cnt);
          if (server_groups_cnt)
            free_embedded_options(server_groups, server_groups_cnt);
        }
        else
        {
         /*
          * Check if embedded parameters passed to connect() differ from
          * first ones
          */

          if ( ((svp = hv_fetch(hv, "mysql_embedded_groups", 21, FALSE)) &&
            *svp  &&  SvTRUE(*svp)))
            rc =+ abs(sv_cmp(*svp, imp_drh->embedded.groups));

          if ( ((svp = hv_fetch(hv, "mysql_embedded_options", 22, FALSE)) &&
            *svp  &&  SvTRUE(*svp)) )
            rc =+ abs(sv_cmp(*svp, imp_drh->embedded.args));

          if (rc)
          {
            do_warn(dbh, AS_ERR_EMBEDDED,
                    "Embedded server was already started. You cannot pass init\
                    parameters to embedded server once");
            return NULL;
          }
        }
      }
    }
#endif

#ifdef MYSQL_NO_CLIENT_FOUND_ROWS
    unsigned int client_flag = 0;
#else
    unsigned int client_flag = CLIENT_FOUND_ROWS;
#endif
    mysql_init(sock);

    if (imp_dbh)
    {
      SV* sv = DBIc_IMP_DATA(imp_dbh);

      DBIc_set(imp_dbh, DBIcf_AutoCommit, &sv_yes);
      if (sv  &&  SvROK(sv))
      {
	HV* hv = (HV*) SvRV(sv);
	SV** svp;
	STRLEN lna;

	if ((svp = hv_fetch(hv, "mysql_compression", 17, FALSE))  &&
	    *svp && SvTRUE(*svp))
        {
	  if (dbis->debug >= 2)
	    PerlIO_printf(DBILOGFP,
			  "imp_dbh->mysql_dr_connect: Enabling" \
			  " compression.\n");
	  mysql_options(sock, MYSQL_OPT_COMPRESS, NULL);
	}
	if ((svp = hv_fetch(hv, "mysql_connect_timeout", 21, FALSE))
	    &&  *svp  &&  SvTRUE(*svp))
        {
	  int to = SvIV(*svp);
	  if (dbis->debug >= 2)
	    PerlIO_printf(DBILOGFP,
			  "imp_dbh->mysql_dr_connect: Setting" \
			  " connect timeout (%d).\n",to);
	  mysql_options(sock, MYSQL_OPT_CONNECT_TIMEOUT,
			(const char *)&to);
	}
	if ((svp = hv_fetch(hv, "mysql_read_default_file", 23, FALSE)) &&
	    *svp  &&  SvTRUE(*svp))
        {
	  char* df = SvPV(*svp, lna);
	  if (dbis->debug >= 2)
	    PerlIO_printf(DBILOGFP,
			  "imp_dbh->mysql_dr_connect: Reading" \
			  " default file %s.\n", df);
	  mysql_options(sock, MYSQL_READ_DEFAULT_FILE, df);
	}
	if ((svp = hv_fetch(hv, "mysql_read_default_group", 24,
			    FALSE))  &&
	    *svp  &&  SvTRUE(*svp)) {
	  char* gr = SvPV(*svp, lna);
	  if (dbis->debug >= 2)
	    PerlIO_printf(DBILOGFP,
			  "imp_dbh->mysql_dr_connect: Using" \
			  " default group %s.\n", gr);

	  mysql_options(sock, MYSQL_READ_DEFAULT_GROUP, gr);
	}
	if ((svp = hv_fetch(hv, "mysql_client_found_rows", 23, FALSE)) && *svp)
        {
	  if (SvTRUE(*svp))
	    client_flag |= CLIENT_FOUND_ROWS;
          else
            client_flag &= ~CLIENT_FOUND_ROWS;
	}
	if ((svp = hv_fetch(hv, "mysql_use_result", 16, FALSE)) && *svp)
        {
          imp_dbh->use_mysql_use_result = SvTRUE(*svp);
	  if (dbis->debug >= 2)
	    PerlIO_printf(DBILOGFP,
			  "imp_dbh->use_mysql_use_result: %d\n",
                          imp_dbh->use_mysql_use_result);
        }

#if MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION

        /*client_flag |= CLIENT_PROTOCOL_41;*/
        imp_dbh->use_server_side_prepare= TRUE;
        if (dbis->debug >= 2)
          PerlIO_printf(DBILOGFP, "server side prepare %d\n",imp_dbh->use_server_side_prepare);

	if ((svp = hv_fetch(hv, "mysql_emulated_prepare", 22, FALSE)) && *svp)
        {
	  if (SvTRUE(*svp))
          {
	    client_flag &= ~CLIENT_PROTOCOL_41;
            imp_dbh->use_server_side_prepare= FALSE;
	  }
	}
        if (dbis->debug >= 2)
          PerlIO_printf(DBILOGFP,
                        "imp_dbh->use_server_side_prepare: %d",
                        imp_dbh->use_server_side_prepare);
#endif

#if defined(DBD_MYSQL_WITH_SSL) && !defined(DBD_MYSQL_EMBEDDED) && \
    (defined(CLIENT_SSL) || (MYSQL_VERSION_ID >= 40000))
	if ((svp = hv_fetch(hv, "mysql_ssl", 9, FALSE))  &&  *svp)
        {
	  if (SvTRUE(*svp))
          {
	    char* client_key = NULL;
	    char* client_cert = NULL;
	    char* ca_file = NULL;
	    char* ca_path = NULL;
	    char* cipher = NULL;
	    STRLEN lna;
	    if ((svp = hv_fetch(hv, "mysql_ssl_client_key", 20, FALSE)) && *svp)
	      client_key = SvPV(*svp, lna);

	    if ((svp = hv_fetch(hv, "mysql_ssl_client_cert", 21, FALSE)) &&
                *svp)
	      client_cert = SvPV(*svp, lna);

	    if ((svp = hv_fetch(hv, "mysql_ssl_ca_file", 17, FALSE)) &&
		 *svp)
	      ca_file = SvPV(*svp, lna);

	    if ((svp = hv_fetch(hv, "mysql_ssl_ca_path", 17, FALSE)) &&
                *svp) 
	      ca_path = SvPV(*svp, lna);

	    if ((svp = hv_fetch(hv, "mysql_ssl_cipher", 16, FALSE)) &&
		*svp)
	      cipher = SvPV(*svp, lna);

	    mysql_ssl_set(sock, client_key, client_cert, ca_file,
			  ca_path, cipher);
	    client_flag |= CLIENT_SSL;
	  }
	}
#endif
#if (MYSQL_VERSION_ID >= 32349)
	/*
	 * MySQL 3.23.49 disables LOAD DATA LOCAL by default. Use
	 * mysql_local_infile=1 in the DSN to enable it.
	 */
     if ((svp = hv_fetch( hv, "mysql_local_infile", 18, FALSE))  &&  *svp)
     {
	  unsigned int flag = SvTRUE(*svp);
	  if (dbis->debug >= 2)
	    PerlIO_printf(DBILOGFP,
        "imp_dbh->mysql_dr_connect: Using" \
        " local infile %u.\n", flag);
	  mysql_options(sock, MYSQL_OPT_LOCAL_INFILE, (const char *) &flag);
	}
#endif
      }
    }
    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP, "imp_dbh->mysql_dr_connect: client_flags = %d\n",
		    client_flag);
 
#if MYSQL_VERSION_ID >= MULTIPLE_RESULT_SET_VERSION
    client_flag|= CLIENT_MULTI_RESULTS;
#endif
    result = mysql_real_connect(sock, host, user, password, dbname,
				portNr, mysql_socket, client_flag);
    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP, "imp_dbh->mysql_dr_connect: <-");

    if (result)
    {
#if MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION
      /* connection succeeded. */
      /* imp_dbh == NULL when mysql_dr_connect() is called from mysql.xs
         functions (_admin_internal(),_ListDBs()). */
      if (!(result->client_flag & CLIENT_PROTOCOL_41) && imp_dbh)
        imp_dbh->use_server_side_prepare = FALSE;
#endif

      /*
        we turn off Mysql's auto reconnect and handle re-connecting ourselves
        so that we can keep track of when this happens.
      */
      result->reconnect=0;
    }
    return result;
  }
}

/*
  safe_hv_fetch
*/
static char *safe_hv_fetch(HV *hv, const char *name, int name_length)
{
  SV** svp;
  STRLEN len;
  char *res= NULL;

  if ((svp= hv_fetch(hv, name, name_length, FALSE)))
  {
    res= SvPV(*svp, len);
    if (!len)
      res= NULL;
  }
  return res;
}

/*
 Frontend for mysql_dr_connect
*/
static int my_login(SV* dbh, imp_dbh_t *imp_dbh)
{
  SV* sv;
  HV* hv;
  char* dbname;
  char* host;
  char* port;
  char* user;
  char* password;
  char* mysql_socket;

#if TAKE_IMP_DATA_VERSION
  if (DBIc_has(imp_dbh, DBIcf_IMPSET))
  { /* eg from take_imp_data() */
    if (DBIc_has(imp_dbh, DBIcf_ACTIVE))
    {
      if (dbis->debug >= 2)
        PerlIO_printf(DBILOGFP, "my_login skip connect\n");
      /* tell our parent we've adopted an active child */
      ++DBIc_ACTIVE_KIDS(DBIc_PARENT_COM(imp_dbh));
      return TRUE;
    }
    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP,
                    "my_login IMPSET but not ACTIVE so connect not skipped\n");
  }
#endif

  sv = DBIc_IMP_DATA(imp_dbh);

  if (!sv  ||  !SvROK(sv))
    return FALSE;

  hv = (HV*) SvRV(sv);
  if (SvTYPE(hv) != SVt_PVHV)
    return FALSE;

  host=		safe_hv_fetch(hv, "host", 4);
  port=		safe_hv_fetch(hv, "port", 4);
  user=		safe_hv_fetch(hv, "user", 4);
  password=	safe_hv_fetch(hv, "password", 8);
  dbname=	safe_hv_fetch(hv, "database", 8);
  mysql_socket=	safe_hv_fetch(hv, "mysql_socket", 12);

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
		  "imp_dbh->my_login : dbname = %s, uid = %s, pwd = %s," \
		  "host = %s, port = %s\n",
		  dbname ? dbname : "NULL",
		  user ? user : "NULL",
		  password ? password : "NULL",
		  host ? host : "NULL",
		  port ? port : "NULL");

  return mysql_dr_connect(dbh, &imp_dbh->mysql, mysql_socket, host, port, user,
			  password, dbname, imp_dbh) ? TRUE : FALSE;
}


/**************************************************************************
 *
 *  Name:    dbd_db_login
 *
 *  Purpose: Called for connecting to a database and logging in.
 *
 *  Input:   dbh - database handle being initialized
 *           imp_dbh - drivers private database handle data
 *           dbname - the database we want to log into; may be like
 *               "dbname:host" or "dbname:host:port"
 *           user - user name to connect as
 *           password - passwort to connect with
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error has already
 *           been called in the latter case
 *
 **************************************************************************/

int dbd_db_login(SV* dbh, imp_dbh_t* imp_dbh, char* dbname, char* user,
		 char* password) {
#ifdef dTHR
  dTHR;
#endif

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
		  "imp_dbh->connect: dsn = %s, uid = %s, pwd = %s\n",
		  dbname ? dbname : "NULL",
		  user ? user : "NULL",
		  password ? password : "NULL");

  imp_dbh->stats.auto_reconnects_ok= 0;
  imp_dbh->stats.auto_reconnects_failed= 0;
  imp_dbh->bind_type_guessing= FALSE;
  imp_dbh->has_transactions= TRUE;
 /* Safer we flip this to TRUE perl side if we detect a mod_perl env. */
  imp_dbh->auto_reconnect = FALSE;

  if (!my_login(dbh, imp_dbh))
  {
    do_error(dbh, mysql_errno(&imp_dbh->mysql),
	     mysql_error(&imp_dbh->mysql));
    return FALSE;
  }

    /*
     *  Tell DBI, that dbh->disconnect should be called for this handle
     */
    DBIc_ACTIVE_on(imp_dbh);

    /* Tell DBI, that dbh->destroy should be called for this handle */
    DBIc_on(imp_dbh, DBIcf_IMPSET);

    return TRUE;
}


/***************************************************************************
 *
 *  Name:    dbd_db_commit
 *           dbd_db_rollback
 *
 *  Purpose: You guess what they should do. mSQL doesn't support
 *           transactions, so we stub commit to return OK
 *           and rollback to return ERROR in any case.
 *
 *  Input:   dbh - database handle being commited or rolled back
 *           imp_dbh - drivers private database handle data
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error has already
 *           been called in the latter case
 *
 **************************************************************************/

int
dbd_db_commit(SV* dbh, imp_dbh_t* imp_dbh)
{
  if (DBIc_has(imp_dbh, DBIcf_AutoCommit))
  {
    do_warn(dbh, TX_ERR_AUTOCOMMIT,
	    "Commmit ineffective while AutoCommit is on");
    return TRUE;
  }

  if (imp_dbh->has_transactions)
  {
#if MYSQL_VERSION_ID < SERVER_PREPARE_VERSION                 
    if (mysql_real_query(&imp_dbh->mysql, "COMMIT", 6))
#else
      if (mysql_commit(&imp_dbh->mysql))
#endif
      {
        do_error(dbh, mysql_errno(&imp_dbh->mysql),
                 mysql_error(&imp_dbh->mysql));
        return FALSE;
      }
  }
  else
    do_warn(dbh, JW_ERR_NOT_IMPLEMENTED,
            "Commmit ineffective while AutoCommit is on");
  return TRUE;
}

/*
 dbd_db_rollback
*/
int
dbd_db_rollback(SV* dbh, imp_dbh_t* imp_dbh) {
  /* croak, if not in AutoCommit mode */
  if (DBIc_has(imp_dbh, DBIcf_AutoCommit))
  {
    do_warn(dbh, TX_ERR_AUTOCOMMIT,
            "Rollback ineffective while AutoCommit is on");
    return FALSE;
  }

  if (imp_dbh->has_transactions)
  {
#if MYSQL_VERSION_ID < SERVER_PREPARE_VERSION
    if (mysql_real_query(&imp_dbh->mysql, "ROLLBACK", 8))
#else
      if (mysql_rollback(&imp_dbh->mysql))
#endif
      {
        do_error(dbh, mysql_errno(&imp_dbh->mysql),
                 mysql_error(&imp_dbh->mysql));
        return FALSE;
      }
  }
  else
    do_error(dbh, JW_ERR_NOT_IMPLEMENTED,
             "Rollback ineffective while AutoCommit is on");
  return TRUE;
}

/*
 ***************************************************************************
 *
 *  Name:    dbd_db_disconnect
 *
 *  Purpose: Disconnect a database handle from its database
 *
 *  Input:   dbh - database handle being disconnected
 *           imp_dbh - drivers private database handle data
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error has already
 *           been called in the latter case
 *
 **************************************************************************/

int dbd_db_disconnect(SV* dbh, imp_dbh_t* imp_dbh)
{
#ifdef dTHR
    dTHR;
#endif

    /* We assume that disconnect will always work       */
    /* since most errors imply already disconnected.    */
    DBIc_ACTIVE_off(imp_dbh);
    if (dbis->debug >= 2)
        PerlIO_printf(DBILOGFP, "&imp_dbh->mysql: %lx\n",
		      (long) &imp_dbh->mysql);
    mysql_close(&imp_dbh->mysql );

    /* We don't free imp_dbh since a reference still exists    */
    /* The DESTROY method is the only one to 'free' memory.    */
    return TRUE;
}


/***************************************************************************
 *
 *  Name:    dbd_discon_all
 *
 *  Purpose: Disconnect all database handles at shutdown time
 *
 *  Input:   dbh - database handle being disconnected
 *           imp_dbh - drivers private database handle data
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error has already
 *           been called in the latter case
 *
 **************************************************************************/

int dbd_discon_all (SV *drh, imp_drh_t *imp_drh) {
#if defined(dTHR)
    dTHR;
#endif

#if defined(DBD_MYSQL_EMBEDDED)
    if (imp_drh->embedded.state)
    {
      if (dbis->debug >= 2)
        PerlIO_printf(DBILOGFP, "Stop embedded server\n");

      mysql_server_end();
      if (imp_drh->embedded.groups)
      {
        (void) SvREFCNT_dec(imp_drh->embedded.groups);
        imp_drh->embedded.groups = NULL;
      }

      if (imp_drh->embedded.args)
      {
        (void) SvREFCNT_dec(imp_drh->embedded.args);
        imp_drh->embedded.args = NULL;
      }


    }
#endif

    /* The disconnect_all concept is flawed and needs more work */
    if (!dirty && !SvTRUE(perl_get_sv("DBI::PERL_ENDING",0))) {
	sv_setiv(DBIc_ERR(imp_drh), (IV)1);
	sv_setpv(DBIc_ERRSTR(imp_drh),
		(char*)"disconnect_all not implemented");
	DBIh_EVENT2(drh, ERROR_event,
		    DBIc_ERR(imp_drh), DBIc_ERRSTR(imp_drh));
	return FALSE;
    }
    perl_destruct_level = 0;
    return FALSE;
}


/****************************************************************************
 *
 *  Name:    dbd_db_destroy
 *
 *  Purpose: Our part of the dbh destructor
 *
 *  Input:   dbh - database handle being destroyed
 *           imp_dbh - drivers private database handle data
 *
 *  Returns: Nothing
 *
 **************************************************************************/

void dbd_db_destroy(SV* dbh, imp_dbh_t* imp_dbh) {

    /*
     *  Being on the safe side never hurts ...
     */
  if (DBIc_ACTIVE(imp_dbh))
  {
    if (imp_dbh->has_transactions)
    {
      if (!DBIc_has(imp_dbh, DBIcf_AutoCommit))
#if MYSQL_VERSION_ID < SERVER_PREPARE_VERSION
        if ( mysql_real_query(&imp_dbh->mysql, "ROLLBACK", 8))
#else
        if (mysql_rollback(&imp_dbh->mysql))
#endif
            do_error(dbh, TX_ERR_ROLLBACK,"ROLLBACK failed");
    }
    dbd_db_disconnect(dbh, imp_dbh);
  }

  /* Tell DBI, that dbh->destroy must no longer be called */
  DBIc_off(imp_dbh, DBIcf_IMPSET);
}

/* 
 ***************************************************************************
 *
 *  Name:    dbd_db_STORE_attrib
 *
 *  Purpose: Function for storing dbh attributes; we currently support
 *           just nothing. :-)
 *
 *  Input:   dbh - database handle being modified
 *           imp_dbh - drivers private database handle data
 *           keysv - the attribute name
 *           valuesv - the attribute value
 *
 *  Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/
int
dbd_db_STORE_attrib(
                    SV* dbh,
                    imp_dbh_t* imp_dbh,
                    SV* keysv,
                    SV* valuesv
                   )
{
  STRLEN kl;
  char *key = SvPV(keysv, kl);
  SV *cachesv = Nullsv;
  int cacheit = FALSE;
  bool bool_value = SvTRUE(valuesv);

  if (kl==10 && strEQ(key, "AutoCommit"))
  {
    if (imp_dbh->has_transactions)
    {
      int oldval = DBIc_has(imp_dbh,DBIcf_AutoCommit);

      if (bool_value == oldval)
        return TRUE;

#if MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION                 
      if (mysql_autocommit(&imp_dbh->mysql, bool_value))
      {
        do_error(dbh, TX_ERR_AUTOCOMMIT,
                 bool_value ? "Turning on AutoCommit failed" : "Turning off AutoCommit failed");
        return FALSE;
      }
#else
      /* if setting AutoCommit on ... */
      if (bool_value)
      {
        /* Setting autocommit will do a commit of any pending statement */
        if (mysql_real_query(&imp_dbh->mysql, "SET AUTOCOMMIT=1", 16))
        {
          do_error(dbh, TX_ERR_AUTOCOMMIT, "Turning on AutoCommit failed");
          return FALSE;
        }
      }
      else
      {
        if (mysql_real_query(&imp_dbh->mysql, "SET AUTOCOMMIT=0", 16))
        {
          do_error(dbh, TX_ERR_AUTOCOMMIT, "Turning off AutoCommit failed");
          return FALSE;
        }
      }
#endif
      DBIc_set(imp_dbh, DBIcf_AutoCommit, bool_value);
    }
    else
    {
      /*
       *  We do support neither transactions nor "AutoCommit".
       *  But we stub it. :-)
     */
      if (!SvTRUE(valuesv))
      {
        do_error(dbh, JW_ERR_NOT_IMPLEMENTED,
                 "Transactions not supported by database");
        croak("Transactions not supported by database");
      }
    }
  }
  else if (kl == 16 && strEQ(key,"mysql_use_result"))
    imp_dbh->use_mysql_use_result = bool_value;
  else if (kl == 20 && strEQ(key,"mysql_auto_reconnect"))
    imp_dbh->auto_reconnect = bool_value;
  else if (kl == 22 && strEQ(key, "mysql_emulated_prepare"))
    imp_dbh->use_server_side_prepare= SvTRUE(valuesv) ? 0 : 1;

  else if (kl == 31 && strEQ(key,"mysql_unsafe_bind_type_guessing"))
	imp_dbh->bind_type_guessing = SvIV(valuesv);
  else
    return FALSE;				/* Unknown key */

  if (cacheit) /* cache value for later DBI 'quick' fetch? */
    hv_store((HV*)SvRV(dbh), key, kl, cachesv, 0);
  return TRUE;
}

/***************************************************************************
 *
 *  Name:    dbd_db_FETCH_attrib
 *
 *  Purpose: Function for fetching dbh attributes
 *
 *  Input:   dbh - database handle being queried
 *           imp_dbh - drivers private database handle data
 *           keysv - the attribute name
 *
 *  Returns: An SV*, if sucessfull; NULL otherwise
 *
 *  Notes:   Do not forget to call sv_2mortal in the former case!
 *
 **************************************************************************/
static SV*
my_ulonglong2str(my_ulonglong val)
{
  char buf[64];
  char *ptr = buf + sizeof(buf) - 1;

  if (val == 0)
    return newSVpv("0", 1);

  *ptr = '\0';
  while (val > 0)
  {
    *(--ptr) = ('0' + (val % 10));
    val = val / 10;
  }
  return newSVpv(ptr, (buf+ sizeof(buf) - 1) - ptr);
}

SV*
dbd_db_FETCH_attrib(
                    SV* dbh,
                    imp_dbh_t* imp_dbh,
                    SV* keysv
                   )
{
  STRLEN kl;
  char *key = SvPV(keysv, kl);
  char* fine_key = NULL;
  SV* result = NULL;

  switch (*key) {
    case 'A':
      if (strEQ(key, "AutoCommit"))
      {
        if (imp_dbh->has_transactions)
          return sv_2mortal(boolSV(DBIc_has(imp_dbh,DBIcf_AutoCommit)));
        /* Default */
        return &sv_yes;
      }
      break;
  }
  if (strncmp(key, "mysql_", 6) == 0) {
    fine_key = key;
    key = key+6;
    kl = kl-6;
  }

  /* MONTY:  Check if kl should not be used or used everywhere */
  switch(*key) {
  case 'a':
    if (kl == strlen("auto_reconnect") && strEQ(key, "auto_reconnect"))
      result= sv_2mortal(newSViv(imp_dbh->auto_reconnect));
    break;
  case 'u':
    if (kl == strlen("unsafe_bind_type_guessing") &&
        strEQ(key, "unsafe_bind_type_guessing"))
      result = sv_2mortal(newSViv(imp_dbh->bind_type_guessing));
    break;
  case 'e':
    if (strEQ(key, "errno"))
      result= sv_2mortal(newSViv((IV)mysql_errno(&imp_dbh->mysql)));
    else if ( strEQ(key, "error") || strEQ(key, "errmsg"))
    {
    /* Note that errmsg is obsolete, as of 2.09! */
      const char* msg = mysql_error(&imp_dbh->mysql);
      result= sv_2mortal(newSVpv(msg, strlen(msg)));
    }
    break;

  case 'd':
    if (strEQ(key, "dbd_stats"))
    {
      HV* hv = newHV();
      hv_store(
               hv,
               "auto_reconnects_ok",
               strlen("auto_reconnects_ok"),
               newSViv(imp_dbh->stats.auto_reconnects_ok),
               0
              );
      hv_store(
               hv,
               "auto_reconnects_failed",
               strlen("auto_reconnects_failed"),
               newSViv(imp_dbh->stats.auto_reconnects_failed),
               0
              );

      result= (newRV_noinc((SV*)hv));
    }

  case 'h':
    if (strEQ(key, "hostinfo"))
    {
      const char* hostinfo = mysql_get_host_info(&imp_dbh->mysql);
      result= hostinfo ?
        sv_2mortal(newSVpv(hostinfo, strlen(hostinfo))) : &sv_undef;
    }
    break;

  case 'i':
    if (strEQ(key, "info"))
    {
      const char* info = mysql_info(&imp_dbh->mysql);
      result= info ? sv_2mortal(newSVpv(info, strlen(info))) : &sv_undef;
    }
    else if (kl == 8  &&  strEQ(key, "insertid"))
      /* We cannot return an IV, because the insertid is a long. */
      result= sv_2mortal(my_ulonglong2str(mysql_insert_id(&imp_dbh->mysql)));
    break;

  case 'p':
    if (kl == 9  &&  strEQ(key, "protoinfo"))
      result= sv_2mortal(newSViv(mysql_get_proto_info(&imp_dbh->mysql)));
    break;

  case 's':
    if (kl == 10  &&  strEQ(key, "serverinfo"))
    {
      const char* serverinfo = mysql_get_server_info(&imp_dbh->mysql);
      result= serverinfo ?
        sv_2mortal(newSVpv(serverinfo, strlen(serverinfo))) : &sv_undef;
    }
    else if (strEQ(key, "sock"))
      result= sv_2mortal(newSViv((IV) &imp_dbh->mysql));
    else if (strEQ(key, "sockfd"))
      result= sv_2mortal(newSViv((IV) imp_dbh->mysql.net.fd));
    else if (strEQ(key, "stat"))
    {
      const char* stats = mysql_stat(&imp_dbh->mysql);
      result= stats ?
        sv_2mortal(newSVpv(stats, strlen(stats))) : &sv_undef;
    }
    else if (strEQ(key, "stats"))
    {
      /* Obsolete, as of 2.09 */
      const char* stats = mysql_stat(&imp_dbh->mysql);
      result= stats ?
        sv_2mortal(newSVpv(stats, strlen(stats))) : &sv_undef;
    }
    else if (kl == 14 && strEQ(key,"server_prepare"))
      result= sv_2mortal(newSViv((IV) imp_dbh->use_server_side_prepare));
    break;

  case 't':
    if (kl == 9  &&  strEQ(key, "thread_id"))
      result= sv_2mortal(newSViv(mysql_thread_id(&imp_dbh->mysql)));
    break;
  }

  if (result== NULL)
    return Nullsv;

  return result;
}


/* 
 **************************************************************************
 *
 *  Name:    dbd_st_prepare
 *
 *  Purpose: Called for preparing an SQL statement; our part of the
 *           statement handle constructor
 *
 *  Input:   sth - statement handle being initialized
 *           imp_sth - drivers private statement handle data
 *           statement - pointer to string with SQL statement
 *           attribs - statement attributes, currently not in use
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error will
 *           be called in the latter case
 *
 **************************************************************************/
int
dbd_st_prepare(
  SV *sth,
  imp_sth_t *imp_sth,
  char *statement,
  SV *attribs)
{
  int i;
  SV **svp;
#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
  char *str_ptr;
  int col_type, prepare_retval, limit_flag=0;
  MYSQL_BIND *bind, *bind_end;
  imp_sth_phb_t *fbind;
#endif

  D_imp_dbh_from_sth;
        if (dbis->debug >= 2)
          PerlIO_printf(DBILOGFP,
                        "\t-> dbd_st_prepare MYSQL_VERSION_ID %d\n",
                        MYSQL_VERSION_ID);

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
 /* Set default value of 'mysql_emulated_prepare' attribute for sth from dbh */
  imp_sth->use_server_side_prepare= imp_dbh->use_server_side_prepare;
  if (attribs)
  {
    svp= DBD_ATTRIB_GET_SVP(attribs, "mysql_emulated_prepare", 22);
    imp_sth->use_server_side_prepare = (svp) ?
      SvTRUE(*svp) : imp_dbh->use_server_side_prepare;
  }

  imp_sth->fetch_done= 0;
#endif

  imp_sth->done_desc= 0;
  imp_sth->result= NULL;
  imp_sth->currow= 0;

 /* Set default value of 'mysql_use_result' attribute for sth from dbh */
  svp= DBD_ATTRIB_GET_SVP(attribs, "mysql_use_result", 16);
  imp_sth->use_mysql_use_result= svp ?
    SvTRUE(*svp) : imp_dbh->use_mysql_use_result;

  for (i= 0; i < AV_ATTRIB_LAST; i++)
    imp_sth->av_attr[i]= Nullav;

#if (MYSQL_VERSION_ID < LIMIT_PLACEHOLDER_VERSION) && \
      (MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION)
  if (imp_sth->use_server_side_prepare)
  {
    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP,
                    "\t\tuse_server_side_prepare set, check LIMIT\n");
/*
 This code is here because mysql 5.0 didn't support placeholders
 in prepared statements
 */ 
    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP,
                    "\t\tneed to test for LIMIT\n");
    for (str_ptr= statement; *str_ptr; str_ptr++)
    {
      i= (str_ptr - statement)/sizeof(char);
      /*
        If there is a 'limit' in the statement and placeholders are
        NOT supported
      */
      if ( (statement[i]   == 'l' || statement[i]   == 'L') &&
           (statement[i+1] == 'i' || statement[i+1] == 'I') &&
           (statement[i+2] == 'm' || statement[i+2] == 'M') &&
           (statement[i+3] == 'i' || statement[i+3] == 'I') &&
           (statement[i+4] == 't' || statement[i+4] == 'T'))
      {
        if (dbis->debug >= 2)
          PerlIO_printf(DBILOGFP, "LIMIT set limit flag to 1\n");
        limit_flag= 1;
      }
      if( limit_flag)
      {
        /* ... and place holders after the limit flag is set... */
        if (statement[i] == '?')
        {
          if (dbis->debug >= 2)
            PerlIO_printf(DBILOGFP,
                    "\t\tLIMIT and ? found, set to use_server_side_prepare=0\n");
          /* ... then we do not want to try server side prepare (use emulation) */
          imp_sth->use_server_side_prepare= 0;
          break;
        }
      }
    }
  }
#endif
#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
  if (imp_sth->use_server_side_prepare)
  {
    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP,
                    "\t\tuse_server_side_prepare set\n");
    /* do we really need this? If we do, we should return, not just continue */
    if (imp_sth->stmt)
      fprintf(stderr,
              "ERROR: Trying to prepare new stmt while we have \
              already not closed one \n");

    imp_sth->stmt= mysql_stmt_init(&imp_dbh->mysql);

    if (! imp_sth->stmt)
    {
      if (dbis->debug >= 2)
        PerlIO_printf(DBILOGFP,
                      "\t\tERROR: Unable to return MYSQL_STMT structure \
                      from mysql_stmt_init(): ERROR NO: %d ERROR MSG:%s\n",
                      mysql_errno(&imp_dbh->mysql),
                      mysql_error(&imp_dbh->mysql));
    }

    prepare_retval= mysql_stmt_prepare(imp_sth->stmt,
                                       statement,
                                       strlen(statement));
    if (dbis->debug >= 2)
        PerlIO_printf(DBILOGFP,
                      "\t\tmysql_stmt_prepare returned %d\n",
                      prepare_retval);

    if (prepare_retval)
    {
        if (dbis->debug >= 2)
          PerlIO_printf(DBILOGFP,
                    "\t\tmysql_stmt_prepare %d %s\n",
                    mysql_stmt_errno(imp_sth->stmt),
                    mysql_stmt_error(imp_sth->stmt));

      /* For commands that are not supported by server side prepared statement
         mechanism lets try to pass them through regular API */
      if (mysql_stmt_errno(imp_sth->stmt) == ER_UNSUPPORTED_PS)
      {
        if (dbis->debug >= 2)
          PerlIO_printf(DBILOGFP,
                    "\t\tSETTING imp_sth->use_server_side_prepare to 0\n");
        imp_sth->use_server_side_prepare= 0;
        mysql_stmt_close(imp_sth->stmt);
      }
      else
      {
        do_error(sth, mysql_stmt_errno(imp_sth->stmt),
                 mysql_stmt_error(imp_sth->stmt));
        mysql_stmt_close(imp_sth->stmt);
        imp_sth->stmt= NULL;
        return FALSE;
      }
    }
    else
    {
      DBIc_NUM_PARAMS(imp_sth)= mysql_stmt_param_count(imp_sth->stmt);
      /* mysql_stmt_param_count */

      if (DBIc_NUM_PARAMS(imp_sth) > 0)
      {
        int has_statement_fields= imp_sth->stmt->fields != 0;
        /* Allocate memory for bind variables */
        imp_sth->bind=            alloc_bind(DBIc_NUM_PARAMS(imp_sth));
        imp_sth->fbind=           alloc_fbind(DBIc_NUM_PARAMS(imp_sth));
        imp_sth->has_been_bound=  0;

        /* Initialize ph variables with  NULL values */
        for (bind=      imp_sth->bind,
             fbind=     imp_sth->fbind,
             bind_end=  bind+DBIc_NUM_PARAMS(imp_sth);
             bind < bind_end ;
             bind++, fbind++ )
        {
          /*
            if this statement has a result set, field types will be
            correctly identified. If there is no result set, such as
            with an INSERT, fields will not be defined, and all buffer_type
            will default to MYSQL_TYPE_VAR_STRING
          */
          col_type= (has_statement_fields ?
                     imp_sth->stmt->fields[i].type : MYSQL_TYPE_STRING);

          bind->buffer_type=  mysql_to_perl_type(col_type);

          if (dbis->debug >= 2)
            PerlIO_printf(DBILOGFP, "\t\tmysql_to_perl_type returned %d\n", col_type);

          bind->buffer=       NULL;
          bind->length=       &(fbind->length);
          bind->is_null=      (char*) &(fbind->is_null);
          fbind->is_null=     1;
          fbind->length=      0;
        }
      }
    }
  }
#endif

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
  /* Count the number of parameters (driver, vs server-side) */
  if (imp_sth->use_server_side_prepare == 0)
    DBIc_NUM_PARAMS(imp_sth) = count_params(statement);
#else
    DBIc_NUM_PARAMS(imp_sth) = count_params(statement);
#endif

  /* Allocate memory for parameters */
  imp_sth->params= alloc_param(DBIc_NUM_PARAMS(imp_sth));
  DBIc_IMPSET_on(imp_sth);

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "\t<- dbd_st_prepare\n");
  return 1;
}

/* My setup_fbav */
AV *my_setup_fbav(imp_sth_t *imp_sth)
{
  /*dPERINTERP;*/
  int i;
  AV *av;
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "\t-> my_setup_fbav\n");

  /*if (DBIc_FIELDS_AV(imp_sth))
    return DBIc_FIELDS_AV(imp_sth);*/

  i= DBIc_NUM_FIELDS(imp_sth);
  if (dbis->debug >= 2)
  {
    PerlIO_printf(DBILOGFP, "\t\tmy_setup_fbav num_fields=%d\n",i);
  }
  if (i <= 0 || i > 32000)	/* trap obvious mistakes */
    croak("my_setup_fbav: invalid number of fields: %d%s",
          i, ", NUM_OF_FIELDS attribute probably not set right");
  av = newAV();
  if (dbis->debug >= 2)
  {
    PerlIO_printf(DBILOGFP, "\t\tmy_setup_fbav: created new AV\n");
  }

  /* load array with writeable SV's. Do this backwards so	*/
  /* the array only gets extended once.			*/
  while(i--)			/* field 1 stored at index 0	*/
    av_store(av, i, newSV(0));
  SvREADONLY_on(av);		/* protect against shift @$row etc */
  /* row_count will need to be manually reset by the driver if the	*/
  /* sth is re-executed (since this code won't get rerun)		*/
  DBIc_ROW_COUNT(imp_sth)= 0;
  DBIc_FIELDS_AV(imp_sth)= av;
  if (dbis->debug >= 2)
  {
    PerlIO_printf(DBILOGFP, "\t<- my_setup_fbav");
  }
  return av;
}

/* *************************************************************
 * "My" version of get_fbav.  The DBI implementation seems to retain
 * a "memory" of the previous result set got by a statement, so this
 * one re-initializes the array every time
 *************************************************************/
AV * my_get_fbav(imp_sth_t *imp_sth)
{
    AV *av;

    if (dbis->debug >= 2)
    {
      PerlIO_printf(DBILOGFP, "\n-> my_get_fbav\n");
    }
    av=  my_setup_fbav(imp_sth);

    if (1)
    { /* XXX turn into option later */
	int i= DBIc_NUM_FIELDS(imp_sth);
	if (dbis->debug >= 2)
        {
	  PerlIO_printf(DBILOGFP,
		  "   my_get_fbav; DBIc_NUM_FIELDS=%llu\n", (u_long) i);
	}

	/* don't let SvUTF8 flag persist from one row to the next   */
	/* (only affects drivers that use sv_setpv, but most XS do) */
	while(i--)                  /* field 1 stored at index 0    */
	    SvUTF8_off(AvARRAY(av)[i]);
    }

    if (DBIc_is(imp_sth, DBIcf_TaintOut))
    {
	dTHR;
	TAINT;	/* affects sv_setsv()'s called within same perl statement */
    }

    /* XXX fancy stuff to happen here later (re scrolling etc)	*/
    ++DBIc_ROW_COUNT(imp_sth);
    if (dbis->debug >= 2)
    {
      PerlIO_printf(DBILOGFP, "\n<- my_get_fbav\n");
    }
    return av;
}

#if MYSQL_VERSION_ID >= MULTIPLE_RESULT_SET_VERSION
/***************************************************************************
 * Name: dbd_st_more_results
 *
 * Purpose: Move onto the next result set (if any)
 *
 * Inputs: sth - Statement handle
 *         imp_sth - driver's private statement handle
 *
 * Returns: 0 if there are more results sets
 *          1 if there are not
 *         <1 for errors.
 *************************************************************************/
int dbd_st_more_results(SV* sth, imp_sth_t* imp_sth)
{
  D_imp_dbh_from_sth;

  int use_mysql_use_result=imp_sth->use_mysql_use_result;
  int next_result_return_code, i;
  MYSQL_RES** result= &imp_sth->result;
  MYSQL* svsock= &imp_dbh->mysql;

  if (dbis->debug >= 2)
  {
    PerlIO_printf(DBILOGFP,
		  "\n    -> dbd_st_more_results for %08lx\n", (u_long) sth);
  }

  if (!SvROK(sth) || SvTYPE(SvRV(sth)) != SVt_PVHV)
    croak("Expected hash array");

  /*
   *  Free cached array attributes
   */
  for (i= 0;  i < AV_ATTRIB_LAST;  i++)
  {
    if (imp_sth->av_attr[i])
      SvREFCNT_dec(imp_sth->av_attr[i]);

    imp_sth->av_attr[i]= Nullav;
  }
  if (dbis->debug >= 2)
  {
    AV* av= my_get_fbav(imp_sth);
    PerlIO_printf(DBILOGFP,
      "\n      <- dbs_st_more_rows av_len(imp_sth->av_attr)=%d\n",
      AvFILL(av));
  }

  /* Release previous MySQL result*/
  mysql_free_result(imp_sth->result);

  if (mysql_errno(svsock))
    do_error(sth, mysql_errno(svsock), mysql_error(svsock));

  next_result_return_code= mysql_next_result(svsock);
  /*
    mysql_next_result returns
     0 if there are more results1
     -1 if there are no more results
     >1 if there was an error
   */
  if (next_result_return_code > 0)
  {
    do_error(sth,mysql_errno(svsock),mysql_error(svsock));
    return 0;
  }
  else if (next_result_return_code < 0)
  {
    /* No rowsets*/
    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP,
		    "\n      <- dbs_st_more_rows no more results\n");
    return 0;
  }
  else
  {
    int num_fields;

    *result= mysql_store_result(svsock);

    if (mysql_errno(svsock))
      do_error(sth, mysql_errno(svsock), mysql_error(svsock));

    if (*result == NULL)
    {
      /* No "real" rowset*/
      if (dbis->debug >= 2)
	PerlIO_printf(DBILOGFP,
		      "\n      <- dbs_st_more_rows: null result set\n");

	return 0;
    }
    /* We have a new rowset */
    imp_sth->currow=0;

    if (dbis->debug >= 5)
    {
      PerlIO_printf(DBILOGFP, "   <- dbd_st_more_results result set details\n");
      PerlIO_printf(DBILOGFP,
                    "             imp_sth->result=%08lx\n",
                    imp_sth->result);
      PerlIO_printf(DBILOGFP, "             mysql_num_fields=%llu\n",
                    mysql_num_fields(imp_sth->result));

      PerlIO_printf(DBILOGFP, "      <-     mysql_num_rows=%llu\n",
                    mysql_num_rows(imp_sth->result));
      PerlIO_printf(DBILOGFP, "      <-     mysql_affected_rows=%llu\n",
                    mysql_affected_rows(svsock));
    }

    /** Store the result in the current statement handle */
    DBIc_ACTIVE_on(imp_sth);
    num_fields=mysql_num_fields(imp_sth->result);

    DBIc_NUM_FIELDS(imp_sth) = num_fields;

    if (dbis->debug >= 5)
    {
      PerlIO_printf(DBILOGFP,
                    "      <- dbd_st_more_results num_fields=%d\n", num_fields);
      PerlIO_printf(DBILOGFP,
                    "         DBIc_NUM_FIELDS=%d\n",DBIc_NUM_FIELDS(imp_sth));
    }
    imp_sth->done_desc = 0;
    if (dbis->debug >= 2)
    {
      AV* av= my_get_fbav(imp_sth);
      PerlIO_printf(DBILOGFP,
                    "      <- dbs_st_more_rows av_len(imp_sth->av_attr)=%d\n",
                    AvFILL(av));
    }
    (imp_dbh->mysql).net.last_errno= 0;
    return 1;
  }
}
#endif
/**************************************************************************
 *
 *  Name:    mysql_st_internal_execute
 *
 *  Purpose: Internal version for executing a statement, called both from
 *           within the "do" and the "execute" method.
 *
 *  Inputs:  h - object handle, for storing error messages
 *           statement - query being executed
 *           attribs - statement attributes, currently ignored
 *           num_params - number of parameters being bound
 *           params - parameter array
 *           result - where to store results, if any
 *           svsock - socket connected to the database
 *
 **************************************************************************/


my_ulonglong mysql_st_internal_execute(
                                       SV *h, /* could be sth or dbh */
                                       SV *statement,
                                       SV *attribs,
                                       int num_params,
                                       imp_sth_ph_t *params,
                                       MYSQL_RES **result,
                                       MYSQL *svsock,
                                       int use_mysql_use_result
                                      )
{
  bool bind_type_guessing;
  STRLEN slen;
  char *sbuf = SvPV(statement, slen);
  char *table;
  char *salloc;
  int htype;
  my_ulonglong rows= 0;

  /* thank you DBI.c for this info! */
  D_imp_xxh(h);
  htype= DBIc_TYPE(imp_xxh);
  /*
    It is important to import imp_dbh properly according to the htype
    that it is! Also, one might ask why bind_type_guessing is assigned
    in each block. Well, it's because D_imp_ macros called in these
    blocks make it so imp_dbh is not "visible" or defined outside of the
    if/else (when compiled, it fails for imp_dbh not being defined).
  */
  /* h is a dbh */
  if (htype==DBIt_DB)
  {
    D_imp_dbh(h);
    bind_type_guessing= imp_dbh->bind_type_guessing;
  }
  /* h is a sth */
  else
  {
    D_imp_sth(h);
    D_imp_dbh_from_sth;
    bind_type_guessing= imp_dbh->bind_type_guessing;
  }

  salloc= parse_params(svsock,
                              sbuf,
                              &slen,
                              params,
                              num_params,
                              bind_type_guessing);

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "mysql_st_internal_execute\n");

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "mysql_st_internal_execute MYSQL_VERSION_ID %d\n",
                  MYSQL_VERSION_ID );

  if (salloc)
  {
    sbuf= salloc;
    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP, "Binding parameters: %s\n", sbuf);
  }

  if (*result)
  {
    mysql_free_result(*result);
    *result= NULL;
  }

  if (slen >= 11 && (!strncmp(sbuf, "listfields ", 11) ||
                     !strncmp(sbuf, "LISTFIELDS ", 11)))
  {
    /* remove pre-space */
    slen-= 10;
    sbuf+= 10;
    while (slen && isspace(*sbuf)) { --slen;  ++sbuf; }

    if (!slen)
    {
      do_error(h, JW_ERR_QUERY, "Missing table name");
      return -2;
    }
    if (!(table= malloc(slen+1)))
    {
      do_error(h, JW_ERR_MEM, "Out of memory");
      return -2;
    }

    strncpy(table, sbuf, slen);
    sbuf= table;

    while (slen && !isspace(*sbuf))
    {
      --slen;
      ++sbuf;
    }
    *sbuf++= '\0';

    *result= mysql_list_fields(svsock, table, NULL);
    free(table);

    if (!(*result))
    {
      do_error(h, mysql_errno(svsock), mysql_error(svsock));
      return -2;
    }

    return 0;
  }

  if ((mysql_real_query(svsock, sbuf, slen))  &&
      (!mysql_db_reconnect(h)  ||
       (mysql_real_query(svsock, sbuf, slen))))
  {
    Safefree(salloc);
    do_error(h, mysql_errno(svsock), mysql_error(svsock));
    return -2;
  }
  Safefree(salloc);

  /** Store the result from the Query */
  *result= use_mysql_use_result ?
    mysql_use_result(svsock) : mysql_store_result(svsock);

  if (mysql_errno(svsock))
    do_error(h, mysql_errno(svsock), mysql_error(svsock));

  if (!*result)
    rows= mysql_affected_rows(svsock);
  else
    rows= mysql_num_rows(*result);

  return(rows);
}

 /**************************************************************************
 *
 *  Name:    mysql_st_internal_execute41
 *
 *  Purpose: Internal version for executing a prepared statement, called both
 *           from within the "do" and the "execute" method.
 *           MYSQL 4.1 API
 *
 *
 *  Inputs:  h - object handle, for storing error messages
 *           statement - query being executed
 *           attribs - statement attributes, currently ignored
 *           num_params - number of parameters being bound
 *           params - parameter array
 *           result - where to store results, if any
 *           svsock - socket connected to the database
 *
 **************************************************************************/

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION

my_ulonglong mysql_st_internal_execute41(
                                         SV *sth,
                                         int num_params,
                                         MYSQL_RES **result,
                                         MYSQL_STMT *stmt,
                                         MYSQL_BIND *bind,
                                         int *has_been_bound
                                        )
{
  int execute_retval;
  my_ulonglong rows=0;

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
                  "\t-> mysql_st_internal_execute41\n");

  /* free result if exists */
  if (*result)
  {
    mysql_free_result(*result);
    *result= 0;
  }

  /*
    If were performed any changes with ph variables
    we have to rebind them
  */

  if (num_params > 0 && !(*has_been_bound))
  {
    if (mysql_stmt_bind_param(stmt,bind))
      goto error;

    *has_been_bound= 1;
  }

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
                  "\t\tmysql_st_internal_execute41 calling mysql_execute with %d num_params\n",
                  num_params);

  execute_retval= mysql_stmt_execute(stmt);
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
                  "\t\tmysql_stmt_execute returned %d\n",
                  execute_retval);
  if (execute_retval)
    goto error;

  /*
   This statement does not return a result set (INSERT, UPDATE...)
  */
  if (!(*result= mysql_stmt_result_metadata(stmt)))
  {
    if (mysql_stmt_errno(stmt))
      goto error;

    rows= mysql_stmt_affected_rows(stmt);
  }
  /*
    This statement returns a result set (SELECT...)
  */
  else
  {
    /* Get the total rows affected and return */
    if (mysql_stmt_store_result(stmt))
      goto error;
    else
      rows= mysql_stmt_num_rows(stmt);
  }
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
                  "\t<- mysql_internal_execute_41 returning %d rows\n",
                  rows);
  return(rows);

error:
  if (*result)
  {
    mysql_free_result(*result);
    *result= 0;
  }
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
                  "     errno %d err message %s\n",
                  mysql_stmt_errno(stmt),
                  mysql_stmt_error(stmt));
  do_error(sth, mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
  mysql_stmt_reset(stmt);

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
                  "\t<- mysql_st_internal_execute41\n");
  return -2;

}
#endif


/***************************************************************************
 *
 *  Name:    dbd_st_execute
 *
 *  Purpose: Called for preparing an SQL statement; our part of the
 *           statement handle constructor
 *
 *  Input:   sth - statement handle being initialized
 *           imp_sth - drivers private statement handle data
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error will
 *           be called in the latter case
 *
 **************************************************************************/

int dbd_st_execute(SV* sth, imp_sth_t* imp_sth)
{
  char actual_row_num[64];
  int i;
  SV **statement;
  D_imp_dbh_from_sth;
#if defined (dTHR)
  dTHR;
#endif

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
      " -> dbd_st_execute for %08lx\n", (u_long) sth);

  if (!SvROK(sth)  ||  SvTYPE(SvRV(sth)) != SVt_PVHV)
    croak("Expected hash array");

  /* Free cached array attributes */
  for (i= 0;  i < AV_ATTRIB_LAST;  i++)
  {
    if (imp_sth->av_attr[i])
      SvREFCNT_dec(imp_sth->av_attr[i]);

    imp_sth->av_attr[i]= Nullav;
  }

  statement= hv_fetch((HV*) SvRV(sth), "Statement", 9, FALSE);

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
                  "  mysql_version_id %d server_prepare_version %d\n",
                  MYSQL_VERSION_ID, SERVER_PREPARE_VERSION);
#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
                "  dbd_st_execute imp_dbh->use_server_side_prepare %d\
                imp_sth->use_server_side_prepare %d\n",
                imp_dbh->use_server_side_prepare,
                imp_sth->use_server_side_prepare);

  if (imp_sth->use_server_side_prepare && ! imp_sth->use_mysql_use_result)
  {
    if (DBIc_ACTIVE(imp_sth) && !(mysql_st_clean_cursor(sth, imp_sth)))
    {
      do_error(sth, JW_ERR_SEQUENCE,
               "Error happened while tried to clean up stmt");
      return 0;
    }

    imp_sth->row_num= mysql_st_internal_execute41(
                                                  sth,
                                                  DBIc_NUM_PARAMS(imp_sth),
                                                  &imp_sth->result,
                                                  imp_sth->stmt,
                                                  imp_sth->bind,
                                                  &imp_sth->has_been_bound
                                                 );
  }
  else
#endif
    imp_sth->row_num= mysql_st_internal_execute(
                                                sth,
                                                *statement,
                                                NULL,
                                                DBIc_NUM_PARAMS(imp_sth),
                                                imp_sth->params,
                                                &imp_sth->result,
                                                &imp_dbh->mysql,
                                                imp_sth->use_mysql_use_result
                                               );

  if (imp_sth->row_num+1 != (my_ulonglong)-1)
  {
    if (!imp_sth->result)
      imp_sth->insertid= mysql_insert_id(&imp_dbh->mysql);
    else
    {
      /** Store the result in the current statement handle */
      DBIc_ACTIVE_on(imp_sth);
	    DBIc_NUM_FIELDS(imp_sth)= mysql_num_fields(imp_sth->result);
            imp_sth->done_desc= 0;
            imp_sth->fetch_done= 0;
    }
  }

  if (dbis->debug >= 2)
  {
    /* 
      PerlIO_printf doesn't always handle imp_sth->row_num %llu 
      consistantly!!
    */
    sprintf(actual_row_num, "%llu", imp_sth->row_num);
    PerlIO_printf(DBILOGFP,
                  " <- dbd_st_execute returning imp_sth->row_num %s\n",
                  actual_row_num);
  }

  return (int)imp_sth->row_num;
}

 /**************************************************************************
 *
 *  Name:    dbd_describe
 *
 *  Purpose: Called from within the fetch method to describe the result
 *
 *  Input:   sth - statement handle being initialized
 *           imp_sth - our part of the statement handle, there's no
 *               need for supplying both; Tim just doesn't remove it
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error will
 *           be called in the latter case
 *
 **************************************************************************/

int dbd_describe(SV* sth, imp_sth_t* imp_sth)
{

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "\t--> dbd_describe\n");

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION

  if (imp_sth->use_server_side_prepare)
  {
    int i;
    int col_type;
    int num_fields= DBIc_NUM_FIELDS(imp_sth);
    imp_sth_fbh_t *fbh;
    MYSQL_BIND *bind;
    MYSQL_FIELD *fields;

    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP, "\t\tdbd_describe() num_fields %d\n",
                    num_fields);

    if (imp_sth->done_desc)
      return TRUE;

    if (!num_fields || !imp_sth->result)
    {
      /* no metadata */
      do_error(sth, JW_ERR_SEQUENCE,
               "no metadata information while trying describe result set");
      return 0;
    }

    /* allocate fields buffers  */
    if (  !(imp_sth->fbh= alloc_fbuffer(num_fields))
          || !(imp_sth->buffer= alloc_bind(num_fields)) )
    {
      /* Out of memory */
      do_error(sth, JW_ERR_SEQUENCE,
               "Out of memory in dbd_sescribe()");
      return 0;
    }

    fields= mysql_fetch_fields(imp_sth->result);

    for (
         fbh= imp_sth->fbh, bind= (MYSQL_BIND*)imp_sth->buffer, i= 0;
         i < num_fields;
         i++, fbh++, bind++
        )
    {
      /* get the column type */
      col_type = fields ? fields[i].type : MYSQL_TYPE_STRING;
      if (dbis->debug >= 2)
      {
        PerlIO_printf(DBILOGFP,"\t\tcol %d type %d len %d\n",
                      i, col_type, fbh->length);
        PerlIO_printf(DBILOGFP,"\t\tcol buf_len%d type %d chrset %d\n",
                      fields[i].length, fields[i].type,
                      fields[i].charsetnr);
      }
      fbh->charsetnr = fields[i].charsetnr;

      bind->buffer_type= mysql_to_perl_type(col_type);
      if (dbis->debug >= 2)
        PerlIO_printf(DBILOGFP, "\t\tmysql_to_perl_type returned %d\n",
                      col_type);
      bind->buffer_length= fields[i].length;
      bind->length= &(fbh->length);
      bind->is_null= &(fbh->is_null);
      Newz(908, fbh->data, fields[i].length, char);

      switch (bind->buffer_type) {
      case MYSQL_TYPE_DOUBLE:
        bind->buffer= (char*) &fbh->ddata;
        break;

      case MYSQL_TYPE_LONG:
        bind->buffer= (char*) &fbh->ldata;
        break;

      case MYSQL_TYPE_STRING:
        bind->buffer= (char *) fbh->data;

      default:
        bind->buffer= (char *) fbh->data;

      }
    }

    if (mysql_stmt_bind_result(imp_sth->stmt, imp_sth->buffer))
    {
      do_error(sth, mysql_stmt_errno(imp_sth->stmt),
               mysql_stmt_error(imp_sth->stmt));
      return 0;
    }
  }
#endif

  imp_sth->done_desc= 1;
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "\t<- dbd_describe\n");
  return TRUE;
}

/**************************************************************************
 *
 *  Name:    dbd_st_fetch
 *
 *  Purpose: Called for fetching a result row
 *
 *  Input:   sth - statement handle being initialized
 *           imp_sth - drivers private statement handle data
 *
 *  Returns: array of columns; the array is allocated by DBI via
 *           DBIS->get_fbav(imp_sth), even the values of the array
 *           are prepared, we just need to modify them appropriately
 *
 **************************************************************************/

AV*
dbd_st_fetch(SV *sth, imp_sth_t* imp_sth)
{
  int num_fields, ChopBlanks, i, rc;
  unsigned long *lengths;
  AV *av;
  MYSQL_ROW cols;
  imp_sth_fbh_t *fbh;
#if MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION
  MYSQL_BIND *bind;
#endif
  D_imp_dbh_from_sth;
  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "\t-> dbd_st_fetch\n");

#if MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION
  if (imp_sth->use_server_side_prepare)
  {
    if (!DBIc_ACTIVE(imp_sth) )
    {
      do_error(sth, JW_ERR_SEQUENCE, "no statement executing\n");
      return Nullav;
    }

    if (imp_sth->fetch_done)
    {
      do_error(sth, JW_ERR_SEQUENCE, "fetch() but fetch already done");
      return Nullav;
    }

    if (!imp_sth->done_desc)
    {
      if (!dbd_describe(sth, imp_sth))
      {
        do_error(sth, JW_ERR_SEQUENCE, "Error while describe result set.");
        return Nullav;
      }
    }
  }
#endif

  ChopBlanks = DBIc_is(imp_sth, DBIcf_ChopBlanks);

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
                  "\t\tdbd_st_fetch for %08lx, chopblanks %d\n",
                  (u_long) sth, ChopBlanks);

  if (!imp_sth->result)
  {
    do_error(sth, JW_ERR_SEQUENCE, "fetch() without execute()");
    return Nullav;
  }

  /* fix from 2.9008 */
  (imp_dbh->mysql).net.last_errno = 0;

#if MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION
  if (imp_sth->use_server_side_prepare)
  {
    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP, "\t\tdbd_st_fetch calling mysql_fetch\n");

    if ((rc= mysql_stmt_fetch(imp_sth->stmt)))
    {
      if (rc == 1)
        do_error(sth, mysql_stmt_errno(imp_sth->stmt),
                 mysql_stmt_error(imp_sth->stmt));

      if (rc == 100)
      {
        /* Update row_num to affected_rows value */
        imp_sth->row_num= mysql_stmt_affected_rows(imp_sth->stmt);
        imp_sth->fetch_done=1;
      }

      if (!DBIc_COMPAT(imp_sth))
        dbd_st_finish(sth, imp_sth);

      return Nullav;
    }

    imp_sth->currow++;

    av= DBIS->get_fbav(imp_sth);
    num_fields=mysql_stmt_field_count(imp_sth->stmt);
    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP,
                    "\t\tdbd_st_fetch called mysql_fetch, rc %d num_fields %d\n",
                    rc, num_fields);

    for (
         bind= imp_sth->buffer,
         fbh= imp_sth->fbh,
         i= 0;
         i < num_fields;
         i++,
         fbh++,
         bind++
        )
    {
      SV *sv= AvARRAY(av)[i]; /* Note: we (re)use the SV in the AV	*/

      /* This is wrong, null is not being set correctly
       * This is not the way to determine length (this would break blobs!)
       */
      if (fbh->is_null)
        (void) SvOK_off(sv);  /*  Field is NULL, return undef  */
      else
      {
        /* In case of BLOB/TEXT fields we allocate only 8192 bytes
           in dbd_describe() for data. Here we know real size of field
           so we should increase buffer size and refetch column value
        */
        if (fbh->length > bind->buffer_length)
        {
          if (dbis->debug > 2)
            PerlIO_printf(DBILOGFP,"\t\tRefetch BLOB/TEXT column: %d\n", i);

          Renew(fbh->data, fbh->length, char);
          bind->buffer_length= fbh->length;
          bind->buffer= (char *) fbh->data;
          /*TODO: Use offset instead of 0 to fetch only remain part of data*/
          if (mysql_stmt_fetch_column(imp_sth->stmt, bind , i, 0))
            do_error(sth, mysql_stmt_errno(imp_sth->stmt),
                     mysql_stmt_error(imp_sth->stmt));
        }

        /* This does look a lot like Georg's PHP driver doesn't it?  --Brian */
        /* Credit due to Georg - mysqli_api.c  ;) --PMG */
        switch (bind->buffer_type) {
        case MYSQL_TYPE_DOUBLE:
          if (dbis->debug > 2)
            PerlIO_printf(DBILOGFP, "\t\tst_fetch double data %f\n", fbh->ddata);
          sv_setnv(sv, fbh->ddata);
          break;

        case MYSQL_TYPE_LONG:
          if (dbis->debug > 2)
            PerlIO_printf(DBILOGFP, "\t\tst_fetch int data %d\n", fbh->ldata);
          sv_setuv(sv, fbh->ldata);
          break;

        case MYSQL_TYPE_STRING:
          if (dbis->debug > 2)
            PerlIO_printf(DBILOGFP, "\t\tst_fetch string data %s\n", fbh->data);
          sv_setpvn(sv, fbh->data, fbh->length);
          if (fbh->charsetnr == 33)
            SvUTF8_on(sv);
          break;

        default:
          if (dbis->debug > 2)
            PerlIO_printf(DBILOGFP, "\t\tERROR IN st_fetch_string");
          sv_setpvn(sv, fbh->data, fbh->length);
          break;

        }
      }
    }

    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP, "\t<- dbd_st_fetch, %d cols\n", num_fields);

    return av;
  }
  else
  {
#endif

    imp_sth->currow++;

    if (dbis->debug > 2)
    {
      PerlIO_printf(DBILOGFP, "\tdbd_st_fetch result set details\n");
      PerlIO_printf(DBILOGFP, "\timp_sth->result=%08lx\n",imp_sth->result);
      PerlIO_printf(DBILOGFP, "\tmysql_num_fields=%llu\n",
                    mysql_num_fields(imp_sth->result));
      PerlIO_printf(DBILOGFP, "\tmysql_num_rows=%llu\n",
                    mysql_num_rows(imp_sth->result));
      PerlIO_printf(DBILOGFP, "\tmysql_affected_rows=%llu\n",
                    mysql_affected_rows(&imp_dbh->mysql));
      PerlIO_printf(DBILOGFP, "\tdbd_st_fetch for %08lx, currow= %d\n",
                    (u_long) sth,imp_sth->currow);
    }

    if (!(cols= mysql_fetch_row(imp_sth->result)))
    {
      if (mysql_errno(&imp_dbh->mysql))
        do_error(sth, mysql_errno(&imp_dbh->mysql),
                 mysql_error(&imp_dbh->mysql));

      if (!DBIc_COMPAT(imp_sth))
        dbd_st_finish(sth, imp_sth);
      return Nullav;
    }

    lengths= mysql_fetch_lengths(imp_sth->result);
#if MYSQL_VERSION_ID >= MULTIPLE_RESULT_SET_VERSION
    /* 
      this check is for BUG# 15546. I would
     like to investigate why it doesn't work, TODO
     */ 
    if (imp_sth->use_server_side_prepare)
      av= my_get_fbav(imp_sth);
    else
      av= DBIS->get_fbav(imp_sth);
#else
    av= DBIS->get_fbav(imp_sth);
#endif
    num_fields=mysql_num_fields(imp_sth->result);

    for (i= 0;  i < num_fields; ++i)
    {
      char *col= cols[i];
      SV *sv= AvARRAY(av)[i]; /* Note: we (re)use the SV in the AV	*/

      if (col)
      {
        STRLEN len= lengths[i];
        if (ChopBlanks)
        {
          while (len && col[len-1] == ' ')
          {	--len; }
        }
        sv_setpvn(sv, col, len);
      }
      else
        (void) SvOK_off(sv);  /*  Field is NULL, return undef  */
    }

    if (dbis->debug >= 2)
      PerlIO_printf(DBILOGFP, "\t<- dbd_st_fetch, %d cols\n", num_fields);
    return av;

#if MYSQL_VERSION_ID  >= SERVER_PREPARE_VERSION
  }
#endif

}

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
/*
  We have to fetch all data from stmt
  There is may be usefull for 2 cases:
  1. st_finish when we have undef statement
  2. call st_execute again when we have some unfetched data in stmt
 */

int mysql_st_clean_cursor(SV* sth, imp_sth_t* imp_sth) {

  if (DBIc_ACTIVE(imp_sth) && dbd_describe(sth, imp_sth) && !imp_sth->fetch_done)
    mysql_stmt_free_result(imp_sth->stmt);
  return 1;
}
#endif

/***************************************************************************
 *
 *  Name:    dbd_st_finish
 *
 *  Purpose: Called for freeing a mysql result
 *
 *  Input:   sth - statement handle being finished
 *           imp_sth - drivers private statement handle data
 *
 *  Returns: TRUE for success, FALSE otherwise; do_error() will
 *           be called in the latter case
 *
 **************************************************************************/

int dbd_st_finish(SV* sth, imp_sth_t* imp_sth) {

#if defined (dTHR)
  dTHR;
#endif

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
  if (imp_sth->use_server_side_prepare)
  {
    if (imp_sth && imp_sth->stmt)
    {
      if (!mysql_st_clean_cursor(sth, imp_sth))
      {
        do_error(sth, JW_ERR_SEQUENCE,
                 "Error happened while tried to clean up stmt");
        return 0;
      }
    }
  }
#endif

  /*
    Cancel further fetches from this cursor.
    We don't close the cursor till DESTROY.
    The application may re execute it.
  */
  if (imp_sth && imp_sth->result)
  {
    mysql_free_result(imp_sth->result);
    imp_sth->result= NULL;
  }
  DBIc_ACTIVE_off(imp_sth);
  return 1;
}


/**************************************************************************
 *
 *  Name:    dbd_st_destroy
 *
 *  Purpose: Our part of the statement handles destructor
 *
 *  Input:   sth - statement handle being destroyed
 *           imp_sth - drivers private statement handle data
 *
 *  Returns: Nothing
 *
 **************************************************************************/

void dbd_st_destroy(SV *sth, imp_sth_t *imp_sth) {
  int i;

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
  int num_fields;
  imp_sth_fbh_t *fbh;

  if (imp_sth->use_server_side_prepare)
  {
    if (imp_sth->stmt)
    {
      if (dbis->debug >= 2)
        PerlIO_printf(DBILOGFP,
                      "\tdbd_st_destroy/server_side_prepare and stmt\n");
      if (mysql_stmt_close(imp_sth->stmt))
      {
        PerlIO_printf(DBILOGFP,
                      "DESTROY: Error %s while close stmt\n",
                      (char *) mysql_stmt_error(imp_sth->stmt));
        do_error(sth, mysql_stmt_errno(imp_sth->stmt),
                 mysql_stmt_error(imp_sth->stmt));
      }
      if (DBIc_NUM_PARAMS(imp_sth) > 0)
      {
       if (dbis->debug >= 2)
           PerlIO_printf(DBILOGFP,
                         "\tFreeing %d parameters\n",
                         DBIc_NUM_PARAMS(imp_sth));
        FreeBind(imp_sth->bind);
        FreeFBind(imp_sth->fbind);
        imp_sth->bind= NULL;
        imp_sth->fbind= NULL;
      }
      num_fields= DBIc_NUM_FIELDS(imp_sth);

      if (imp_sth->fbh)
      {
        num_fields= DBIc_NUM_FIELDS(imp_sth);

        for (fbh= imp_sth->fbh, i= 0; i < num_fields; i++, fbh++)
        {
          if (fbh->data)
            Safefree(fbh->data);
        }
        FreeFBuffer(imp_sth->fbh);
        FreeBind(imp_sth->buffer);
        imp_sth->buffer= NULL;
        imp_sth->fbh= NULL;
      }
    }
  }
#endif

  /* dbd_st_finish has already been called by .xs code if needed.	*/

  /* Free values allocated by dbd_bind_ph */
  FreeParam(imp_sth->params, DBIc_NUM_PARAMS(imp_sth));
  imp_sth->params= NULL;
/*
  if (imp_sth->params)
  {
    FreeParam(imp_sth->params, DBIc_NUM_PARAMS(imp_sth));
    imp_sth->params= NULL;
  }

*/
  /* Free cached array attributes */
  for (i= 0;  i < AV_ATTRIB_LAST;  i++)
  {
    if (imp_sth->av_attr[i])
      SvREFCNT_dec(imp_sth->av_attr[i]);
    imp_sth->av_attr[i]= Nullav;
  }
  /* let DBI know we've done it   */
  DBIc_IMPSET_off(imp_sth);
}


/*
 **************************************************************************
 *
 *  Name:    dbd_st_STORE_attrib
 *
 *  Purpose: Modifies a statement handles attributes; we currently
 *           support just nothing
 *
 *  Input:   sth - statement handle being destroyed
 *           imp_sth - drivers private statement handle data
 *           keysv - attribute name
 *           valuesv - attribute value
 *
 *  Returns: TRUE for success, FALSE otrherwise; do_error will
 *           be called in the latter case
 *
 **************************************************************************/
int
dbd_st_STORE_attrib(
                    SV *sth,
                    imp_sth_t *imp_sth,
                    SV *keysv,
                    SV *valuesv
                   )
{
  STRLEN(kl);
  char *key= SvPV(keysv, kl);
  int retval= FALSE;

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
                  "\t\t-> dbd_st_STORE_attrib for %08lx, key %s\n",
                  (u_long) sth, key);

  if (strEQ(key, "mysql_use_result"))
  {
    imp_sth->use_mysql_use_result= SvTRUE(valuesv);
  }

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
                  "\t\t<- dbd_st_STORE_attrib for %08lx, result %d\n",
                  (u_long) sth, retval);

  return retval;
}


/*
 **************************************************************************
 *
 *  Name:    dbd_st_FETCH_internal
 *
 *  Purpose: Retrieves a statement handles array attributes; we use
 *           a separate function, because creating the array
 *           attributes shares much code and it aids in supporting
 *           enhanced features like caching.
 *
 *  Input:   sth - statement handle; may even be a database handle,
 *               in which case this will be used for storing error
 *               messages only. This is only valid, if cacheit (the
 *               last argument) is set to TRUE.
 *           what - internal attribute number
 *           res - pointer to a DBMS result
 *           cacheit - TRUE, if results may be cached in the sth.
 *
 *  Returns: RV pointing to result array in case of success, NULL
 *           otherwise; do_error has already been called in the latter
 *           case.
 *
 **************************************************************************/

#ifndef IS_KEY
#define IS_KEY(A) (((A) & (PRI_KEY_FLAG | UNIQUE_KEY_FLAG | MULTIPLE_KEY_FLAG)) != 0)
#endif

#if !defined(IS_AUTO_INCREMENT) && defined(AUTO_INCREMENT_FLAG)
#define IS_AUTO_INCREMENT(A) (((A) & AUTO_INCREMENT_FLAG) != 0)
#endif

SV*
dbd_st_FETCH_internal(
  SV *sth,
  int what,
  MYSQL_RES *res,
  int cacheit
)
{
  D_imp_sth(sth);
  AV *av= Nullav;
  MYSQL_FIELD *curField;

  /* Are we asking for a legal value? */
  if (what < 0 ||  what >= AV_ATTRIB_LAST)
    do_error(sth, JW_ERR_NOT_IMPLEMENTED, "Not implemented");

  /* Return cached value, if possible */
  else if (cacheit  &&  imp_sth->av_attr[what])
    av= imp_sth->av_attr[what];

  /* Does this sth really have a result? */
  else if (!res)
    do_error(sth, JW_ERR_NOT_ACTIVE,
	     "statement contains no result");
  /* Do the real work. */
  else
  {
    av= newAV();
    mysql_field_seek(res, 0);
    while ((curField= mysql_fetch_field(res)))
    {
      SV *sv;

      switch(what) {
      case AV_ATTRIB_NAME:
        sv= newSVpv(curField->name, strlen(curField->name));
        break;

      case AV_ATTRIB_TABLE:
        sv= newSVpv(curField->table, strlen(curField->table));
        break;

      case AV_ATTRIB_TYPE:
        sv= newSViv((int) curField->type);
        break;

      case AV_ATTRIB_SQL_TYPE:
        sv= newSViv((int) native2sql(curField->type)->data_type);
        break;
      case AV_ATTRIB_IS_PRI_KEY:
        sv= boolSV(IS_PRI_KEY(curField->flags));
        break;

      case AV_ATTRIB_IS_NOT_NULL:
        sv= boolSV(IS_NOT_NULL(curField->flags));
        break;

      case AV_ATTRIB_NULLABLE:
        sv= boolSV(!IS_NOT_NULL(curField->flags));
        break;

      case AV_ATTRIB_LENGTH:
        sv= newSViv((int) curField->length);
        break;

      case AV_ATTRIB_IS_NUM:
        sv= newSViv((int) native2sql(curField->type)->is_num);
        break;

      case AV_ATTRIB_TYPE_NAME:
        sv= newSVpv((char*) native2sql(curField->type)->type_name, 0);
        break;

      case AV_ATTRIB_MAX_LENGTH:
        sv= newSViv((int) curField->max_length);
        break;

      case AV_ATTRIB_IS_AUTO_INCREMENT:
#if defined(AUTO_INCREMENT_FLAG)
        sv= boolSV(IS_AUTO_INCREMENT(curField->flags));
        break;
#else
        croak("AUTO_INCREMENT_FLAG is not supported on this machine");
#endif

      case AV_ATTRIB_IS_KEY:
        sv= boolSV(IS_KEY(curField->flags));
        break;

      case AV_ATTRIB_IS_BLOB:
        sv= boolSV(IS_BLOB(curField->flags));
        break;

      case AV_ATTRIB_SCALE:
        sv= newSViv((int) curField->decimals);
        break;

      case AV_ATTRIB_PRECISION:
        sv= newSViv((int) (curField->length > curField->max_length) ?
                     curField->length : curField->max_length);
        break;

      default:
        sv= &sv_undef;
        break;
      }
      av_push(av, sv);
    }

    /* Ensure that this value is kept, decremented in
     *  dbd_st_destroy and dbd_st_execute.  */
    if (!cacheit)
      return sv_2mortal(newRV_noinc((SV*)av));
    imp_sth->av_attr[what]= av;
  }

  if (av == Nullav)
    return &sv_undef;

  return sv_2mortal(newRV_inc((SV*)av));
}


/*
 **************************************************************************
 *
 *  Name:    dbd_st_FETCH_attrib
 *
 *  Purpose: Retrieves a statement handles attributes
 *
 *  Input:   sth - statement handle being destroyed
 *           imp_sth - drivers private statement handle data
 *           keysv - attribute name
 *
 *  Returns: NULL for an unknown attribute, "undef" for error,
 *           attribute value otherwise.
 *
 **************************************************************************/

#define ST_FETCH_AV(what) \
    dbd_st_FETCH_internal(sth, (what), imp_sth->result, TRUE)

  SV* dbd_st_FETCH_attrib(
                          SV *sth,
                          imp_sth_t *imp_sth,
                          SV *keysv
                         )
{
  STRLEN(kl);
  char *key= SvPV(keysv, kl);
  SV *retsv= Nullsv;
  if (kl < 2)
    return Nullsv;

  if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP,
                  "    -> dbd_st_FETCH_attrib for %08lx, key %s\n",
                  (u_long) sth, key);

  switch (*key) {
  case 'N':
    if (strEQ(key, "NAME"))
      retsv= ST_FETCH_AV(AV_ATTRIB_NAME);
    else if (strEQ(key, "NULLABLE"))
      retsv= ST_FETCH_AV(AV_ATTRIB_NULLABLE);
    break;
  case 'P':
    if (strEQ(key, "PRECISION"))
      retsv= ST_FETCH_AV(AV_ATTRIB_PRECISION);
    if (strEQ(key, "ParamValues"))
    {
        HV *pvhv= newHV();
        if (DBIc_NUM_PARAMS(imp_sth))
        {
            unsigned int n;
            SV *sv;
            char key[100];
            I32 keylen;
            for (n= 0; n < DBIc_NUM_PARAMS(imp_sth); n++)
            {
                keylen= sprintf(key, "%d", n);
                hv_store(pvhv, key,
                         keylen, newSVsv(imp_sth->params[n].value), 0);
            }
        }
        retsv= newRV_noinc((SV*)pvhv);
    }
    break;
  case 'S':
    if (strEQ(key, "SCALE"))
      retsv= ST_FETCH_AV(AV_ATTRIB_SCALE);
    break;
  case 'T':
    if (strEQ(key, "TYPE"))
      retsv= ST_FETCH_AV(AV_ATTRIB_SQL_TYPE);
    break;
  case 'm':
    switch (kl) {
    case 10:
      if (strEQ(key, "mysql_type"))
        retsv= ST_FETCH_AV(AV_ATTRIB_TYPE);
      break;
    case 11:
      if (strEQ(key, "mysql_table"))
        retsv= ST_FETCH_AV(AV_ATTRIB_TABLE);
      break;
    case 12:
      if (       strEQ(key, "mysql_is_key"))
        retsv= ST_FETCH_AV(AV_ATTRIB_IS_KEY);
      else if (strEQ(key, "mysql_is_num"))
        retsv= ST_FETCH_AV(AV_ATTRIB_IS_NUM);
      else if (strEQ(key, "mysql_length"))
        retsv= ST_FETCH_AV(AV_ATTRIB_LENGTH);
      else if (strEQ(key, "mysql_result"))
        retsv= sv_2mortal(newSViv((IV) imp_sth->result));
      break;
    case 13:
      if (strEQ(key, "mysql_is_blob"))
        retsv= ST_FETCH_AV(AV_ATTRIB_IS_BLOB);
      break;
    case 14:
      if (strEQ(key, "mysql_insertid"))
      {
        /* We cannot return an IV, because the insertid is a long.  */
        if (dbis->debug >= 2)
          PerlIO_printf(DBILOGFP, "INSERT ID %d\n", imp_sth->insertid);

        return sv_2mortal(my_ulonglong2str(imp_sth->insertid));
      }
      break;
    case 15:
      if (strEQ(key, "mysql_type_name"))
        retsv = ST_FETCH_AV(AV_ATTRIB_TYPE_NAME);
      break;
    case 16:
      if ( strEQ(key, "mysql_is_pri_key"))
        retsv= ST_FETCH_AV(AV_ATTRIB_IS_PRI_KEY);
      else if (strEQ(key, "mysql_max_length"))
        retsv= ST_FETCH_AV(AV_ATTRIB_MAX_LENGTH);
      else if (strEQ(key, "mysql_use_result"))
        retsv= boolSV(imp_sth->use_mysql_use_result);
      break;
    case 22:
      if (strEQ(key, "mysql_emulated_prepare"))
#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
        retsv= imp_sth->use_server_side_prepare ? boolSV(1) : boolSV(0);
#else
        retsv= boolSV(0);
#endif
      break;
    case 23:
      if (strEQ(key, "mysql_is_auto_increment"))
        retsv = ST_FETCH_AV(AV_ATTRIB_IS_AUTO_INCREMENT);
      break;
    }
    break;
  }
  return retsv;
}


/***************************************************************************
 *
 *  Name:    dbd_st_blob_read
 *
 *  Purpose: Used for blob reads if the statement handles "LongTruncOk"
 *           attribute (currently not supported by DBD::mysql)
 *
 *  Input:   SV* - statement handle from which a blob will be fetched
 *           imp_sth - drivers private statement handle data
 *           field - field number of the blob (note, that a row may
 *               contain more than one blob)
 *           offset - the offset of the field, where to start reading
 *           len - maximum number of bytes to read
 *           destrv - RV* that tells us where to store
 *           destoffset - destination offset
 *
 *  Returns: TRUE for success, FALSE otrherwise; do_error will
 *           be called in the latter case
 *
 **************************************************************************/

int dbd_st_blob_read (
  SV *sth,
  imp_sth_t *imp_sth,
  int field,
  long offset,
  long len,
  SV *destrv,
  long destoffset)
{
    return FALSE;
}


/***************************************************************************
 *
 *  Name:    dbd_bind_ph
 *
 *  Purpose: Binds a statement value to a parameter
 *
 *  Input:   sth - statement handle
 *           imp_sth - drivers private statement handle data
 *           param - parameter number, counting starts with 1
 *           value - value being inserted for parameter "param"
 *           sql_type - SQL type of the value
 *           attribs - bind parameter attributes, currently this must be
 *               one of the values SQL_CHAR, ...
 *           inout - TRUE, if parameter is an output variable (currently
 *               this is not supported)
 *           maxlen - ???
 *
 *  Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

int dbd_bind_ph (SV *sth, imp_sth_t *imp_sth, SV *param, SV *value,
		 IV sql_type, SV *attribs, int is_inout, IV maxlen) {
  int rc;
  int param_num= SvIV(param);
  int idx= param_num - 1;   
  char err_msg[64];

#if MYSQL_VERSION_ID >=40101
  STRLEN slen;
  char *buffer;
  int buffer_is_null= 0;
  int buffer_length= slen;
  int buffer_type= 0;
#endif

  if (param_num <= 0  ||  param_num > DBIc_NUM_PARAMS(imp_sth))
  {
    do_error(sth, JW_ERR_ILLEGAL_PARAM_NUM,
             "Illegal parameter number");
    return FALSE;
  }

  /* 
     This fixes the bug whereby no warning was issued upone binding a 
     defined non-numeric as numeric
   */
  if (SvOK(value) &&
      (sql_type == SQL_NUMERIC  ||
       sql_type == SQL_DECIMAL  ||
       sql_type == SQL_INTEGER  ||
       sql_type == SQL_SMALLINT ||
       sql_type == SQL_FLOAT    ||
       sql_type == SQL_REAL     ||
       sql_type == SQL_DOUBLE) )
  {
    if (! looks_like_number(value))
    {
      sprintf(err_msg,
              "Binding non-numeric field %d, value %s as a numeric!",
              param_num, neatsvpv(value,0));
      do_error(sth, JW_ERR_ILLEGAL_PARAM_NUM, err_msg);
    }
  }

  if (is_inout)
  {
    do_error(sth, JW_ERR_NOT_IMPLEMENTED,
             "Output parameters not implemented");
    return FALSE;
  }

  rc = bind_param(&imp_sth->params[idx], value, sql_type);

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
    if (imp_sth->use_server_side_prepare)
    {
      if (SvOK(imp_sth->params[idx].value) && imp_sth->params[idx].value)
      {
       // buffer= SvPV(imp_sth->params[idx].value, slen);
        buffer_is_null= 0;
       // buffer_length= slen;
      }
      else
      {
        buffer= NULL;
        buffer_is_null= 1;
        //buffer_length= 0;
      }

      switch(sql_type) {
        case SQL_NUMERIC:
        case SQL_INTEGER:
        case SQL_SMALLINT:
        case SQL_BIGINT:
        case SQL_TINYINT:
          /* INT */
        if (!SvIOK(imp_sth->params[idx].value) && dbis->debug >= 2)
          PerlIO_printf(DBILOGFP, "\t\tTRY TO BIND AN INT NUMBER\n");

        buffer_type= MYSQL_TYPE_LONG;
        imp_sth->fbind[idx].numeric_val.lval= SvIV(imp_sth->params[idx].value);
        buffer=(void*)&(imp_sth->fbind[idx].numeric_val.lval);
        if (dbis->debug >= 2)
          PerlIO_printf(DBILOGFP,
                        "   SCALAR type %d ->%ld<- IS A INT NUMBER\n",
                        sql_type, (long) (*buffer));
          break;

        case SQL_DOUBLE:
        case SQL_DECIMAL:
        case SQL_FLOAT:
        case SQL_REAL:
        if (!SvNOK(imp_sth->params[idx].value) && dbis->debug >= 2)
          PerlIO_printf(DBILOGFP, "\t\tTRY TO BIND A FLOAT NUMBER\n");

          /*if (SvNOK(imp_sth->params[idx].value))
            {*/
            buffer_type= MYSQL_TYPE_DOUBLE;
            imp_sth->fbind[idx].numeric_val.dval= SvNV(imp_sth->params[idx].value);
            buffer=(char*)&(imp_sth->fbind[idx].numeric_val.dval);
        
            if (dbis->debug >= 2)
              PerlIO_printf(DBILOGFP,
                          "   SCALAR type %d ->%f<- IS A FLOAT NUMBER\n",
                          sql_type, (double)(*buffer));
            /*}*/
          break;

        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_DATE:
        case SQL_TIME:
        case SQL_TIMESTAMP:
        case SQL_LONGVARCHAR:
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY:
          buffer_type= MYSQL_TYPE_STRING;
          break;

        default:
          buffer_type= MYSQL_TYPE_STRING;
          break;
      }

      if (buffer_is_null)
        buffer_type= MYSQL_TYPE_NULL;
      if (buffer_type == MYSQL_TYPE_STRING)
      {
        buffer= SvPV(imp_sth->params[idx].value, slen);
        buffer_length= slen;
        if (dbis->debug >= 2)
          PerlIO_printf(DBILOGFP,
                        "   SCALAR type %d ->%s<- IS A STRING\n",
                        sql_type, buffer);
      }

      /* Type of column was changed. Force to rebind */
      if (imp_sth->bind[idx].buffer_type != buffer_type)
        imp_sth->has_been_bound = 0;

      /* prepare has not been called */
      if (imp_sth->has_been_bound == 0)
      {
        imp_sth->bind[idx].buffer_type= buffer_type;
        imp_sth->bind[idx].buffer= buffer;
        imp_sth->bind[idx].buffer_length= buffer_length;
      }
      else /* prepare has been called */
      {
        //imp_sth->stmt->params[idx].buffer_type= buffer_type;
        imp_sth->stmt->params[idx].buffer= buffer;
        imp_sth->stmt->params[idx].buffer_length= buffer_length;
      }
      imp_sth->fbind[idx].length= buffer_length;
      imp_sth->fbind[idx].is_null= buffer_is_null;
    }
#endif
    return rc;
}


/***************************************************************************
 *
 *  Name:    mysql_db_reconnect
 *
 *  Purpose: If the server has disconnected, try to reconnect.
 *
 *  Input:   h - database or statement handle
 *
 *  Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

int mysql_db_reconnect(SV* h)
{
  D_imp_xxh(h);
  imp_dbh_t* imp_dbh;
  MYSQL save_socket;

  if (DBIc_TYPE(imp_xxh) == DBIt_ST)
  {
    imp_dbh = (imp_dbh_t*) DBIc_PARENT_COM(imp_xxh);
    h = DBIc_PARENT_H(imp_xxh);
  }
  else
    imp_dbh= (imp_dbh_t*) imp_xxh;

  if (mysql_errno(&imp_dbh->mysql) != CR_SERVER_GONE_ERROR)
    /* Other error */
    return FALSE;

  if (!DBIc_has(imp_dbh, DBIcf_AutoCommit) || !imp_dbh->auto_reconnect)
  {
    /* We never reconnect if AutoCommit is turned off.
     * Otherwise we might get an inconsistent transaction
     * state.
     */
    return FALSE;
  }

  /* my_login will blow away imp_dbh->mysql so we save a copy of
   * imp_dbh->mysql and put it back where it belongs if the reconnect
   * fail.  Think server is down & reconnect fails but the application eval{}s
   * the execute, so next time $dbh->quote() gets called, instant SIGSEGV!
   */
  save_socket= imp_dbh->mysql;
  memcpy (&save_socket, &imp_dbh->mysql,sizeof(save_socket));
  memset (&imp_dbh->mysql,0,sizeof(imp_dbh->mysql));

  if (!my_login(h, imp_dbh))
  {
    do_error(h, mysql_errno(&imp_dbh->mysql), mysql_error(&imp_dbh->mysql));
    memcpy (&imp_dbh->mysql, &save_socket, sizeof(save_socket));
    ++imp_dbh->stats.auto_reconnects_failed;
    return FALSE;
  }
  ++imp_dbh->stats.auto_reconnects_ok;
  return TRUE;
}


/**************************************************************************
 *
 *  Name:    dbd_db_type_info_all
 *
 *  Purpose: Implements $dbh->type_info_all
 *
 *  Input:   dbh - database handle
 *           imp_sth - drivers private database handle data
 *
 *  Returns: RV to AV of types
 *
 **************************************************************************/

#define PV_PUSH(c)                              \
    if (c) {                                    \
	sv= newSVpv((char*) (c), 0);           \
	SvREADONLY_on(sv);                      \
    } else {                                    \
        sv= &sv_undef;                         \
    }                                           \
    av_push(row, sv);

#define IV_PUSH(i) sv= newSViv((i)); SvREADONLY_on(sv); av_push(row, sv);

AV *dbd_db_type_info_all(SV *dbh, imp_dbh_t *imp_dbh)
{
  AV *av= newAV();
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

  hv= newHV();
  av_push(av, newRV_noinc((SV*) hv));
  for (i= 0;  i < (int)(sizeof(cols) / sizeof(const char*));  i++)
  {
    if (!hv_store(hv, (char*) cols[i], strlen(cols[i]), newSViv(i), 0))
    {
      SvREFCNT_dec((SV*) av);
      return Nullav;
    }
  }
  for (i= 0;  i < (int)SQL_GET_TYPE_INFO_num;  i++)
  {
    const sql_type_info_t *t= &SQL_GET_TYPE_INFO_values[i];

    row= newAV();
    av_push(av, newRV_noinc((SV*) row));
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

    if (t->num_prec_radix)
    {
      IV_PUSH(t->num_prec_radix);
    }
    else
      av_push(row, &sv_undef);

    IV_PUSH(t->sql_datatype); /* SQL_DATATYPE*/
    IV_PUSH(t->sql_datetime_sub); /* SQL_DATETIME_SUB*/
    IV_PUSH(t->interval_precision); /* INTERVAL_PERCISION */
    IV_PUSH(t->native_type);
    IV_PUSH(t->is_num);
  }
  return av;
}


/*
  dbd_db_quote

  Properly quotes a value 
*/
SV* dbd_db_quote(SV *dbh, SV *str, SV *type)
{
  SV *result;

  if (SvGMAGICAL(str))
    mg_get(str);

  if (!SvOK(str))
    result= newSVpv("NULL", 4);
  else
  {
    char *ptr, *sptr;
    STRLEN len;

    D_imp_dbh(dbh);

    if (type  &&  SvOK(type))
    {
      int i;
      int tp= SvIV(type);
      for (i= 0;  i < (int)SQL_GET_TYPE_INFO_num;  i++)
      {
        const sql_type_info_t *t= &SQL_GET_TYPE_INFO_values[i];
        if (t->data_type == tp)
        {
          if (!t->literal_prefix)
            return Nullsv;
          break;
        }
      }
    }

    ptr= SvPV(str, len);
    result= newSV(len*2+3);
    sptr= SvPVX(result);

    *sptr++ = '\'';
    sptr+= mysql_real_escape_string(&imp_dbh->mysql, sptr,
                                     ptr, len);
    *sptr++= '\'';
    SvPOK_on(result);
    SvCUR_set(result, sptr - SvPVX(result));
    /* Never hurts NUL terminating a Per string */
    *sptr++= '\0';
  }
  return result;
}

#ifdef DBD_MYSQL_INSERT_ID_IS_GOOD
SV *mysql_db_last_insert_id(SV *dbh, imp_dbh_t *imp_dbh,
        SV *catalog, SV *schema, SV *table, SV *field, SV *attr)
{
  return sv_2mortal(my_ulonglong2str(mysql_insert_id(&((imp_dbh_t*)imp_dbh)->mysql)));
}
#endif
