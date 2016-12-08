/*
 *  DBD::mysql - DBI driver for the mysql database
 *
 *  Copyright (c) 2015-2017 Pali Rohár
 *  Copyright (c) 2004-2017 Patrick Galbraith
 *  Copyright (c) 2013-2017 Michiel Beijen 
 *  Copyright (c) 2004-2007 Alexey Stroganov 
 *  Copyright (c) 2003-2005  Rudolf Lippan
 *  Copyright (c) 1997-2003  Jochen Wiedmann
 *
 *  You may distribute this under the terms of either the GNU General Public
 *  License or the Artistic License, as specified in the Perl README file.
 */


#ifdef WIN32
#include "windows.h"
#include "winsock.h"
#endif

#include "dbdimp.h"

#if defined(WIN32)  &&  defined(WORD)
#undef WORD
typedef short WORD;
#endif

#ifdef WIN32
#define MIN min
#else
#ifndef MIN
#define MIN(a, b)       ((a) < (b) ? (a) : (b))
#endif
#endif

#if MYSQL_ASYNC
#  include <poll.h>
#  include <errno.h>
#  define ASYNC_CHECK_RETURN(h, value)\
    if(imp_dbh->async_query_in_flight) {\
        do_error(h, 2000, "Calling a synchronous function on an asynchronous handle", "HY000");\
        return (value);\
    }
#else
#  define ASYNC_CHECK_RETURN(h, value)
#endif

static int parse_number(char *string, STRLEN len, char **end);

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
count_params(imp_xxh_t *imp_xxh, pTHX_ char *statement, bool bind_comment_placeholders)
{
  bool comment_end= false;
  char* ptr= statement;
  int num_params= 0;
  int comment_length= 0;
  char c;

  if (DBIc_DBISTATE(imp_xxh)->debug >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), ">count_params statement %s\n", statement);

  while ( (c = *ptr++) )
  {
    switch (c) {
      /* so, this is a -- comment, so let's burn up characters */
    case '-':
      {
          if (bind_comment_placeholders)
          {
              c = *ptr++;
              break;
          }
          else
          {
              comment_length= 1;
              /* let's see if the next one is a dash */
              c = *ptr++;

              if  (c == '-') {
                  /* if two dashes, ignore everything until newline */
                  while ((c = *ptr))
                  {
                      if (DBIc_DBISTATE(imp_xxh)->debug >= 2)
                          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "%c\n", c);
                      ptr++;
                      comment_length++;
                      if (c == '\n')
                      {
                          comment_end= true;
                          break;
                      }
                  }
                  /*
                    if not comment_end, the comment never ended and we need to iterate
                    back to the beginning of where we started and let the database 
                    handle whatever is in the statement
                */
                  if (! comment_end)
                      ptr-= comment_length;
              }
              /* otherwise, only one dash/hyphen, backtrack by one */
              else
                  ptr--;
              break;
          }
      }
    /* c-type comments */
    case '/':
      {
          if (bind_comment_placeholders)
          {
              c = *ptr++;
              break;
          }
          else
          {
              c = *ptr++;
              /* let's check if the next one is an asterisk */
              if  (c == '*')
              {
                  comment_length= 0;
                  comment_end= false;
                  /* ignore everything until closing comment */
                  while ((c= *ptr))
                  {
                      ptr++;
                      comment_length++;

                      if (c == '*')
                      {
                          c = *ptr++;
                          /* alas, end of comment */
                          if (c == '/')
                          {
                              comment_end= true;
                              break;
                          }
                          /*
                            nope, just an asterisk, not so fast, not
                            end of comment, go back one
                        */
                          else
                              ptr--;
                      }
                  }
                  /*
                    if the end of the comment was never found, we have
                    to backtrack to wherever we first started skipping
                    over the possible comment.
                    This means we will pass the statement to the database
                    to see its own fate and issue the error
                */
                  if (!comment_end)
                      ptr -= comment_length;
              }
              else
                  ptr--;
              break;
          }
      }
    case '`':
    case '"':
    case '\'':
      /* Skip string */
      {
        char end_token = c;
        while ((c = *ptr)  &&  c != end_token)
        {
          if (c == '\\')
            if (! *(++ptr))
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
    Newz(908, params, (unsigned int) num_params, imp_sth_ph_t);
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
    Newz(908, bind, (unsigned int) num_params, MYSQL_BIND);
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
    Newz(908, fbind, (unsigned int) num_params, imp_sth_phb_t);
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
    Newz(908, fbh, (unsigned int) num_fields, imp_sth_fbh_t);
  else
    fbh= NULL;

  return fbh;
}

/*
  free MYSQL_BIND bind struct
*/
static void free_bind(MYSQL_BIND *bind)
{
  if (bind)
    Safefree(bind);
}

/*
   free imp_sth_phb_t fbind structure
*/
static void free_fbind(imp_sth_phb_t *fbind)
{
  if (fbind)
    Safefree(fbind);
}

/*
  free imp_sth_fbh_t fbh structure
*/
static void free_fbuffer(imp_sth_fbh_t *fbh)
{
  if (fbh)
    Safefree(fbh);
}

#endif

/*
  free statement param structure per num_params
*/
static void
free_param(pTHX_ imp_sth_ph_t *params, int num_params)
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

/* 
  Convert a MySQL type to a type that perl can handle

  NOTE: In the future we may want to return a struct with a lot of
  information for each type
*/

static enum enum_field_types mysql_to_perl_type(enum enum_field_types type)
{
  static enum enum_field_types enum_type;

  switch (type) {
  case MYSQL_TYPE_DOUBLE:
  case MYSQL_TYPE_FLOAT:
    enum_type= MYSQL_TYPE_DOUBLE;
    break;

  case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_TINY:
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_YEAR:
#if IVSIZE >= 8
  case MYSQL_TYPE_LONGLONG:
    enum_type= MYSQL_TYPE_LONGLONG;
#else
    enum_type= MYSQL_TYPE_LONG;
#endif
    break;

#if MYSQL_VERSION_ID > NEW_DATATYPE_VERSION
  case MYSQL_TYPE_BIT:
    enum_type= MYSQL_TYPE_BIT;
    break;
#endif

#if MYSQL_VERSION_ID > NEW_DATATYPE_VERSION
  case MYSQL_TYPE_NEWDECIMAL:
#endif
  case MYSQL_TYPE_DECIMAL:
    enum_type= MYSQL_TYPE_DECIMAL;
    break;

#if IVSIZE < 8
  case MYSQL_TYPE_LONGLONG:
#endif
  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_TIME:
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_NEWDATE:
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_VAR_STRING:
#if MYSQL_VERSION_ID > NEW_DATATYPE_VERSION
  case MYSQL_TYPE_VARCHAR:
#endif
  case MYSQL_TYPE_STRING:
    enum_type= MYSQL_TYPE_STRING;
    break;

#if MYSQL_VERSION_ID > GEO_DATATYPE_VERSION
  case MYSQL_TYPE_GEOMETRY:
#endif
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_TINY_BLOB:
    enum_type= MYSQL_TYPE_BLOB;
    break;

  default:
    enum_type= MYSQL_TYPE_STRING;    /* MySQL can handle all types as strings */
  }
  return(enum_type);
}

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
  Free embedded options
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
 Print out embedded option settings

*/
int print_embedded_options(PerlIO *stream, char ** options_list, int options_count)
{
  int i;

  for (i=0; i<options_count; i++)
  {
    if (options_list[i])
        PerlIO_printf(stream,
                      "Embedded server, parameter[%d]=%s\n",
                      i, options_list[i]);
  }
  return 1;
}

/*

*/
char **fill_out_embedded_options(PerlIO *stream,
                                 char *options,
                                 int options_type,
                                 int slen, int cnt)
{
  int  ind, len;
  char c;
  char *ptr;
  char **options_list= NULL;

  if (!(options_list= (char **) calloc(cnt, sizeof(char *))))
  {
    PerlIO_printf(stream,
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
      return NULL;

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
        return NULL;

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
                          imp_xxh_t *imp_xxh,
                          pTHX_ MYSQL *sock,
                          char *statement,
                          STRLEN *slen_ptr,
                          imp_sth_ph_t* params,
                          int num_params,
                          bool bind_type_guessing,
                          bool bind_comment_placeholders)
{
  bool comment_end= false;
  char *salloc, *statement_ptr;
  char *statement_ptr_end, *ptr, *valbuf;
  char *cp, *end;
  int alen, i;
  int slen= *slen_ptr;
  int limit_flag= 0;
  int comment_length=0;
  STRLEN vallen;
  imp_sth_ph_t *ph;

  if (DBIc_DBISTATE(imp_xxh)->debug >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), ">parse_params statement %s\n", statement);

  if (num_params == 0)
    return NULL;

  while (isspace(*statement))
  {
    ++statement;
    --slen;
  }

  /* Calculate the number of bytes being allocated for the statement */
  alen= slen;

  for (i= 0, ph= params; i < num_params; i++, ph++)
  {
    int defined= 0;
    if (ph->value)
    {
      if (SvMAGICAL(ph->value))
        mg_get(ph->value);
      if (SvOK(ph->value))
        defined=1;
    }
    if (!defined)
      alen+= 3;  /* Erase '?', insert 'NULL' */
    else
    {
      valbuf= SvPV(ph->value, vallen);
      alen+= 2+vallen+1;
      /* this will most likely not happen since line 214 */
      /* of mysql.xs hardcodes all types to SQL_VARCHAR */
      if (!ph->type)
      {
        if (bind_type_guessing)
        {
          valbuf= SvPV(ph->value, vallen);
          ph->type= SQL_INTEGER;

          if (parse_number(valbuf, vallen, &end) != 0)
          {
              ph->type= SQL_VARCHAR;
          }
        }
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
      /* comment detection. Anything goes in a comment */
      case '-':
      {
          if (bind_comment_placeholders)
          {
              *ptr++= *statement_ptr++;
              break;
          }
          else
          {
              comment_length= 1;
              comment_end= false;
              *ptr++ = *statement_ptr++;
              if  (*statement_ptr == '-')
              {
                  /* ignore everything until newline or end of string */
                  while (*statement_ptr)
                  {
                      comment_length++;
                      *ptr++ = *statement_ptr++;
                      if (!*statement_ptr || *statement_ptr == '\n')
                      {
                          comment_end= true;
                          break;
                      }
                  }
                  /* if not end of comment, go back to where we started, no end found */
                  if (! comment_end)
                  {
                      statement_ptr -= comment_length;
                      ptr -= comment_length;
                  }
              }
              break;
          }
      }
      /* c-type comments */
      case '/':
      {
          if (bind_comment_placeholders)
          {
              *ptr++= *statement_ptr++;
              break;
          }
          else
          {
              comment_length= 1;
              comment_end= false;
              *ptr++ = *statement_ptr++;
              if  (*statement_ptr == '*')
              {
                  /* use up characters everything until newline */
                  while (*statement_ptr)
                  {
                      *ptr++ = *statement_ptr++;
                      comment_length++;
                      if (!strncmp(statement_ptr, "*/", 2))
                      {
                          comment_length += 2;
                          comment_end= true;
                          break;
                      }
                  }
                  /* Go back to where started if comment end not found */
                  if (! comment_end)
                  {
                      statement_ptr -= comment_length;
                      ptr -= comment_length;
                  }
              }
              break;
          }
      }
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

            /* (note this sets *end, which we use if is_num) */
            if ( parse_number(valbuf, vallen, &end) != 0 && is_num)
            {
              if (bind_type_guessing) {
                /* .. not a number, so apparently we guessed wrong */
                is_num = 0;
                ph->type = SQL_VARCHAR;
              }
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
              for (cp= valbuf; cp < end; cp++)
                  *ptr++= *cp;
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

  return(salloc);
}

int bind_param(imp_sth_ph_t *ph, SV *value, IV sql_type)
{
  dTHX;
  if (ph->value)
  {
    if (SvMAGICAL(ph->value))
      mg_get(ph->value);
    (void) SvREFCNT_dec(ph->value);
  }

  ph->value= newSVsv(value);

  if (sql_type)
    ph->type = sql_type;

  return TRUE;
}

static const sql_type_info_t SQL_GET_TYPE_INFO_values[]= {
  { "varchar",    SQL_VARCHAR,                    255, "'",  "'",  "max length",
    1, 0, 3, 0, 0, 0, "variable length string",
    0, 0, 0,
    SQL_VARCHAR, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_VAR_STRING,  0,
#else
    MYSQL_TYPE_STRING,  0,
#endif
  },
  { "decimal",   SQL_DECIMAL,                      15, NULL, NULL, "precision,scale",
    1, 0, 3, 0, 0, 0, "double",
    0, 6, 2,
    SQL_DECIMAL, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_DECIMAL,     1
#else
    MYSQL_TYPE_DECIMAL,     1
#endif
  },
  { "tinyint",   SQL_TINYINT,                       3, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Tiny integer",
    0, 0, 10,
    SQL_TINYINT, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_TINY,        1
#else
    MYSQL_TYPE_TINY,     1
#endif
  },
  { "smallint",  SQL_SMALLINT,                      5, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Short integer",
    0, 0, 10,
    SQL_SMALLINT, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_SHORT,       1
#else
    MYSQL_TYPE_SHORT,     1
#endif
  },
  { "integer",   SQL_INTEGER,                      10, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_LONG,        1
#else
    MYSQL_TYPE_LONG,     1
#endif
  },
  { "float",     SQL_REAL,                          7,  NULL, NULL, NULL,
    1, 0, 0, 0, 0, 0, "float",
    0, 2, 10,
    SQL_FLOAT, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_FLOAT,       1
#else
    MYSQL_TYPE_FLOAT,     1
#endif
  },
  { "double",    SQL_FLOAT,                       15,  NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "double",
    0, 4, 2,
    SQL_FLOAT, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_DOUBLE,      1
#else
    MYSQL_TYPE_DOUBLE,     1
#endif
  },
  { "double",    SQL_DOUBLE,                       15,  NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "double",
    0, 4, 10,
    SQL_DOUBLE, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_DOUBLE,      1
#else
    MYSQL_TYPE_DOUBLE,     1
#endif
  },
  /*
    FIELD_TYPE_NULL ?
  */
  { "timestamp", SQL_TIMESTAMP,                    14, "'", "'", NULL,
    0, 0, 3, 0, 0, 0, "timestamp",
    0, 0, 0,
    SQL_TIMESTAMP, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_TIMESTAMP,   0
#else
    MYSQL_TYPE_TIMESTAMP,     0
#endif
  },
  { "bigint",    SQL_BIGINT,                       19, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Longlong integer",
    0, 0, 10,
    SQL_BIGINT, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_LONGLONG,    1
#else
    MYSQL_TYPE_LONGLONG,     1
#endif
  },
  { "mediumint", SQL_INTEGER,                       8, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Medium integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_INT24,       1
#else
    MYSQL_TYPE_INT24,     1
#endif
  },
  { "date", SQL_DATE, 10, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "date",
    0, 0, 0,
    SQL_DATE, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_DATE, 0
#else
    MYSQL_TYPE_DATE, 0
#endif
  },
  { "time", SQL_TIME, 6, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "time",
    0, 0, 0,
    SQL_TIME, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_TIME,        0
#else
    MYSQL_TYPE_TIME,     0
#endif
  },
  { "datetime",  SQL_TIMESTAMP, 21, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "datetime",
    0, 0, 0,
    SQL_TIMESTAMP, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_DATETIME,    0
#else
    MYSQL_TYPE_DATETIME,     0
#endif
  },
  { "year", SQL_SMALLINT, 4, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "year",
    0, 0, 10,
    SQL_SMALLINT, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_YEAR,        0
#else
    MYSQL_TYPE_YEAR,     0
#endif
  },
  { "date", SQL_DATE, 10, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "date",
    0, 0, 0,
    SQL_DATE, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_NEWDATE,     0
#else
    MYSQL_TYPE_NEWDATE,     0
#endif
  },
  { "enum",      SQL_VARCHAR,                     255, "'",  "'",  NULL,
    1, 0, 1, 0, 0, 0, "enum(value1,value2,value3...)",
    0, 0, 0,
    0, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_ENUM,        0
#else
    MYSQL_TYPE_ENUM,     0
#endif
  },
  { "set",       SQL_VARCHAR,                     255, "'",  "'",  NULL,
    1, 0, 1, 0, 0, 0, "set(value1,value2,value3...)",
    0, 0, 0,
    0, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_SET,         0
#else
    MYSQL_TYPE_SET,     0
#endif
  },
  { "blob",       SQL_LONGVARBINARY,              65535, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object (0-65535)",
    0, 0, 0,
    SQL_LONGVARBINARY, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_BLOB,        0
#else
    MYSQL_TYPE_BLOB,     0
#endif
  },
  { "tinyblob",  SQL_VARBINARY,                 255, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object (0-255) ",
    0, 0, 0,
    SQL_VARBINARY, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_TINY_BLOB,   0
#else
    FIELD_TYPE_TINY_BLOB,        0
#endif
  },
  { "mediumblob", SQL_LONGVARBINARY,           16777215, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object",
    0, 0, 0,
    SQL_LONGVARBINARY, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0 
    FIELD_TYPE_MEDIUM_BLOB, 0
#else
    MYSQL_TYPE_MEDIUM_BLOB, 0
#endif
  },
  { "longblob",   SQL_LONGVARBINARY,         2147483647, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object, use mediumblob instead",
    0, 0, 0,
    SQL_LONGVARBINARY, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0 
    FIELD_TYPE_LONG_BLOB,   0
#else
    MYSQL_TYPE_LONG_BLOB,   0
#endif
  },
  { "char",       SQL_CHAR,                       255, "'",  "'",  "max length",
    1, 0, 3, 0, 0, 0, "string",
    0, 0, 0,
    SQL_CHAR, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0 
    FIELD_TYPE_STRING,      0
#else
    MYSQL_TYPE_STRING,   0
#endif
  },

  { "decimal",            SQL_NUMERIC,            15,  NULL, NULL, "precision,scale",
    1, 0, 3, 0, 0, 0, "double",
    0, 6, 2,
    SQL_NUMERIC, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_DECIMAL,     1
#else
    MYSQL_TYPE_DECIMAL,   1 
#endif
  },
  { "tinyint unsigned",   SQL_TINYINT,              3, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Tiny integer unsigned",
    0, 0, 10,
    SQL_TINYINT, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_TINY,        1
#else
    MYSQL_TYPE_TINY,        1
#endif
  },
  { "smallint unsigned",  SQL_SMALLINT,             5, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Short integer unsigned",
    0, 0, 10,
    SQL_SMALLINT, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_SHORT,       1
#else
    MYSQL_TYPE_SHORT,       1
#endif
  },
  { "mediumint unsigned", SQL_INTEGER,              8, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Medium integer unsigned",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_INT24,       1
#else
    MYSQL_TYPE_INT24,       1
#endif
  },
  { "int unsigned",       SQL_INTEGER,             10, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "integer unsigned",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_LONG,        1
#else
    MYSQL_TYPE_LONG,        1
#endif
  },
  { "int",                SQL_INTEGER,             10, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_LONG,        1
#else
    MYSQL_TYPE_LONG,        1
#endif
  },
  { "integer unsigned",   SQL_INTEGER,             10, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_LONG,        1
#else
    MYSQL_TYPE_LONG,        1
#endif
  },
  { "bigint unsigned",    SQL_BIGINT,              20, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Longlong integer unsigned",
    0, 0, 10,
    SQL_BIGINT, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_LONGLONG,    1
#else
    MYSQL_TYPE_LONGLONG,    1
#endif
  },
  { "text",               SQL_LONGVARCHAR,      65535, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "large text object (0-65535)",
    0, 0, 0,
    SQL_LONGVARCHAR, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_BLOB,        0
#else
    MYSQL_TYPE_BLOB,        0
#endif
  },
  { "mediumtext",         SQL_LONGVARCHAR,   16777215, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "large text object",
    0, 0, 0,
    SQL_LONGVARCHAR, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    FIELD_TYPE_MEDIUM_BLOB, 0
#else
    MYSQL_TYPE_MEDIUM_BLOB, 0
#endif
  },
  { "mediumint unsigned auto_increment", SQL_INTEGER, 8, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "Medium integer unsigned auto_increment", 0, 0, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_INTEGER, 0, 0, FIELD_TYPE_INT24, 1,
#else
    SQL_INTEGER, 0, 0, MYSQL_TYPE_INT24, 1,
#endif
  },
  { "tinyint unsigned auto_increment", SQL_TINYINT, 3, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "tinyint unsigned auto_increment", 0, 0, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_TINYINT, 0, 0, FIELD_TYPE_TINY, 1
#else
    SQL_TINYINT, 0, 0, MYSQL_TYPE_TINY, 1
#endif
  },

  { "smallint auto_increment", SQL_SMALLINT, 5, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "smallint auto_increment", 0, 0, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_SMALLINT, 0, 0, FIELD_TYPE_SHORT, 1
#else
    SQL_SMALLINT, 0, 0, MYSQL_TYPE_SHORT, 1
#endif
  },

  { "int unsigned auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "integer unsigned auto_increment", 0, 0, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1
#else
    SQL_INTEGER, 0, 0, MYSQL_TYPE_LONG, 1
#endif
  },

  { "mediumint", SQL_INTEGER, 7, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Medium integer", 0, 0, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_INTEGER, 0, 0, FIELD_TYPE_INT24, 1
#else
    SQL_INTEGER, 0, 0, MYSQL_TYPE_INT24, 1
#endif
  },

  { "bit", SQL_BIT, 1, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "char(1)", 0, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_BIT, 0, 0, FIELD_TYPE_TINY, 0
#else
    SQL_BIT, 0, 0, MYSQL_TYPE_TINY, 0
#endif
  },

  { "numeric", SQL_NUMERIC, 19, NULL, NULL, "precision,scale",
    1, 0, 3, 0, 0, 0, "numeric", 0, 19, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_NUMERIC, 0, 0, FIELD_TYPE_DECIMAL, 1,
#else
    SQL_NUMERIC, 0, 0, MYSQL_TYPE_DECIMAL, 1,
#endif
  },

  { "integer unsigned auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "integer unsigned auto_increment", 0, 0, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1,
#else
    SQL_INTEGER, 0, 0, MYSQL_TYPE_LONG, 1,
#endif
  },

  { "mediumint unsigned", SQL_INTEGER, 8, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Medium integer unsigned", 0, 0, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_INTEGER, 0, 0, FIELD_TYPE_INT24, 1
#else
    SQL_INTEGER, 0, 0, MYSQL_TYPE_INT24, 1
#endif
  },

  { "smallint unsigned auto_increment", SQL_SMALLINT, 5, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "smallint unsigned auto_increment", 0, 0, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_SMALLINT, 0, 0, FIELD_TYPE_SHORT, 1
#else
    SQL_SMALLINT, 0, 0, MYSQL_TYPE_SHORT, 1
#endif
  },

  { "int auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "integer auto_increment", 0, 0, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1
#else
    SQL_INTEGER, 0, 0, MYSQL_TYPE_LONG, 1
#endif
  },

  { "long varbinary", SQL_LONGVARBINARY, 16777215, "0x", NULL, NULL,
    1, 0, 3, 0, 0, 0, "mediumblob", 0, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_LONGVARBINARY, 0, 0, FIELD_TYPE_LONG_BLOB, 0
#else
    SQL_LONGVARBINARY, 0, 0, MYSQL_TYPE_LONG_BLOB, 0
#endif
  },

  { "double auto_increment", SQL_FLOAT, 15, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "double auto_increment", 0, 4, 2,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_FLOAT, 0, 0, FIELD_TYPE_DOUBLE, 1
#else
    SQL_FLOAT, 0, 0, MYSQL_TYPE_DOUBLE, 1
#endif
  },

  { "double auto_increment", SQL_DOUBLE, 15, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "double auto_increment", 0, 4, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_DOUBLE, 0, 0, FIELD_TYPE_DOUBLE, 1
#else
    SQL_DOUBLE, 0, 0, MYSQL_TYPE_DOUBLE, 1
#endif
  },

  { "integer auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "integer auto_increment", 0, 0, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_INTEGER, 0, 0, FIELD_TYPE_LONG, 1,
#else
    SQL_INTEGER, 0, 0, MYSQL_TYPE_LONG, 1,
#endif
  },

  { "bigint auto_increment", SQL_BIGINT, 19, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "bigint auto_increment", 0, 0, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_BIGINT, 0, 0, FIELD_TYPE_LONGLONG, 1
#else
    SQL_BIGINT, 0, 0, MYSQL_TYPE_LONGLONG, 1
#endif
  },

  { "bit auto_increment", SQL_BIT, 1, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "char(1) auto_increment", 0, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_BIT, 0, 0, FIELD_TYPE_TINY, 1
#else
    SQL_BIT, 0, 0, MYSQL_TYPE_TINY, 1
#endif
  },

  { "mediumint auto_increment", SQL_INTEGER, 7, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "Medium integer auto_increment", 0, 0, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_INTEGER, 0, 0, FIELD_TYPE_INT24, 1
#else
    SQL_INTEGER, 0, 0, MYSQL_TYPE_INT24, 1
#endif
  },

  { "float auto_increment", SQL_REAL, 7, NULL, NULL, NULL,
    0, 0, 0, 0, 0, 1, "float auto_increment", 0, 2, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_FLOAT, 0, 0, FIELD_TYPE_FLOAT, 1
#else
    SQL_FLOAT, 0, 0, MYSQL_TYPE_FLOAT, 1
#endif
  },

  { "long varchar", SQL_LONGVARCHAR, 16777215, "'", "'", NULL,
    1, 0, 3, 0, 0, 0, "mediumtext", 0, 0, 0,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_LONGVARCHAR, 0, 0, FIELD_TYPE_MEDIUM_BLOB, 1
#else
    SQL_LONGVARCHAR, 0, 0, MYSQL_TYPE_MEDIUM_BLOB, 1
#endif

  },

  { "tinyint auto_increment", SQL_TINYINT, 3, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "tinyint auto_increment", 0, 0, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_TINYINT, 0, 0, FIELD_TYPE_TINY, 1
#else
    SQL_TINYINT, 0, 0, MYSQL_TYPE_TINY, 1
#endif
  },

  { "bigint unsigned auto_increment", SQL_BIGINT, 20, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "bigint unsigned auto_increment", 0, 0, 10,
#if MYSQL_VERSION_ID < MYSQL_VERSION_5_0
    SQL_BIGINT, 0, 0, FIELD_TYPE_LONGLONG, 1
#else
    SQL_BIGINT, 0, 0, MYSQL_TYPE_LONGLONG, 1
#endif
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
#ifdef FIELD_TYPE_NEWDECIMAL
    case FIELD_TYPE_NEWDECIMAL:  return &SQL_GET_TYPE_INFO_values[1];
#endif
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
 *  Input:   dbistate - pointer to the DBI state variable, used for some
 *               DBI internal things
 *
 *  Returns: Nothing
 *
 **************************************************************************/

void dbd_init(dbistate_t* dbistate)
{
    dTHX;
    DBISTATE_INIT;
    PERL_UNUSED_ARG(dbistate);
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

void do_error(SV* h, int rc, const char* what, const char* sqlstate)
{
  dTHX;
  D_imp_xxh(h);
  SV *errstr;
  SV *errstate;

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\t--> do_error\n");
  errstr= DBIc_ERRSTR(imp_xxh);
  sv_setiv(DBIc_ERR(imp_xxh), (IV)rc);	/* set err early	*/
  sv_setpv(errstr, what);

#if MYSQL_VERSION_ID >= SQL_STATE_VERSION
  if (sqlstate)
  {
    errstate= DBIc_STATE(imp_xxh);
    sv_setpvn(errstate, sqlstate, 5);
  }
#endif

  /* NO EFFECT DBIh_EVENT2(h, ERROR_event, DBIc_ERR(imp_xxh), errstr); */
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "%s error %d recorded: %s\n",
    what, rc, SvPV_nolen(errstr));
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\t<-- do_error\n");
}

/*
  void do_warn(SV* h, int rc, char* what)
*/
void do_warn(SV* h, int rc, char* what)
{
  dTHX;
  D_imp_xxh(h);

  SV *errstr = DBIc_ERRSTR(imp_xxh);
  sv_setiv(DBIc_ERR(imp_xxh), (IV)rc);	/* set err early	*/
  sv_setpv(errstr, what);
  /* NO EFFECT DBIh_EVENT2(h, WARN_event, DBIc_ERR(imp_xxh), errstr);*/
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "%s warning %d recorded: %s\n",
    what, rc, SvPV_nolen(errstr));
  warn("%s", what);
}

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

static void set_ssl_error(MYSQL *sock, const char *error)
{
  const char *prefix = "SSL connection error: ";
  STRLEN prefix_len;
  STRLEN error_len;

  sock->net.last_errno = CR_SSL_CONNECTION_ERROR;
  strcpy(sock->net.sqlstate, "HY000");

  prefix_len = strlen(prefix);
  if (prefix_len > sizeof(sock->net.last_error) - 1)
    prefix_len = sizeof(sock->net.last_error) - 1;
  memcpy(sock->net.last_error, prefix, prefix_len);

  error_len = strlen(error);
  if (prefix_len + error_len > sizeof(sock->net.last_error) - 1)
    error_len = sizeof(sock->net.last_error) - prefix_len - 1;
  if (prefix_len + error_len > 100)
    error_len = 100 - prefix_len;
  memcpy(sock->net.last_error + prefix_len, error, error_len);

  sock->net.last_error[prefix_len + error_len] = 0;
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

MYSQL *mysql_dr_connect(
                        SV* dbh,
                        MYSQL* sock,
                        char* mysql_socket,
                        char* host,
			                  char* port,
                        char* user,
                        char* password,
			                  char* dbname,
                        imp_dbh_t *imp_dbh)
{
  int portNr;
  unsigned int client_flag;
  MYSQL* result;
  dTHX;
  D_imp_xxh(dbh);

  /* per Monty, already in client.c in API */
  /* but still not exist in libmysqld.c */
#if defined(DBD_MYSQL_EMBEDDED)
   if (host && !*host) host = NULL;
#endif

  portNr= (port && *port) ? atoi(port) : 0;

  /* already in client.c in API */
  /* if (user && !*user) user = NULL; */
  /* if (password && !*password) password = NULL; */


  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
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
              server_groups=fill_out_embedded_options(DBIc_LOGPIO(imp_xxh), options, 0, 
                                                      (int)lna, ++server_groups_cnt);
              if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
              {
                PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                              "Groups names passed to embedded server:\n");
                print_embedded_options(DBIc_LOGPIO(imp_xxh), server_groups, server_groups_cnt);
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
              server_args=fill_out_embedded_options(DBIc_LOGPIO(imp_xxh), options, 1, (int)lna, ++server_args_cnt);
              if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
              {
                PerlIO_printf(DBIc_LOGPIO(imp_xxh), "Server options passed to embedded server:\n");
                print_embedded_options(DBIc_LOGPIO(imp_xxh), server_args, server_args_cnt);
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

#ifdef DBD_MYSQL_NO_CLIENT_FOUND_ROWS
    client_flag = 0;
#else
    client_flag = CLIENT_FOUND_ROWS;
#endif
    mysql_init(sock);

    if (imp_dbh)
    {
      SV* sv = DBIc_IMP_DATA(imp_dbh);

      DBIc_set(imp_dbh, DBIcf_AutoCommit, TRUE);
      if (sv  &&  SvROK(sv))
      {
        HV* hv = (HV*) SvRV(sv);
        SV** svp;
        STRLEN lna;

        /* thanks to Peter John Edwards for mysql_init_command */ 
        if ((svp = hv_fetch(hv, "mysql_init_command", 18, FALSE)) &&
            *svp && SvTRUE(*svp))
        {
          char* df = SvPV(*svp, lna);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                           "imp_dbh->mysql_dr_connect: Setting" \
                           " init command (%s).\n", df);
          mysql_options(sock, MYSQL_INIT_COMMAND, df);
        }
        if ((svp = hv_fetch(hv, "mysql_compression", 17, FALSE))  &&
            *svp && SvTRUE(*svp))
        {
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->mysql_dr_connect: Enabling" \
                          " compression.\n");
          mysql_options(sock, MYSQL_OPT_COMPRESS, NULL);
        }
        if ((svp = hv_fetch(hv, "mysql_connect_timeout", 21, FALSE))
            &&  *svp  &&  SvTRUE(*svp))
        {
          int to = SvIV(*svp);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->mysql_dr_connect: Setting" \
                          " connect timeout (%d).\n",to);
          mysql_options(sock, MYSQL_OPT_CONNECT_TIMEOUT,
                        (const char *)&to);
        }
        if ((svp = hv_fetch(hv, "mysql_write_timeout", 19, FALSE))
            &&  *svp  &&  SvTRUE(*svp))
        {
          int to = SvIV(*svp);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->mysql_dr_connect: Setting" \
                          " write timeout (%d).\n",to);
          mysql_options(sock, MYSQL_OPT_WRITE_TIMEOUT,
                        (const char *)&to);
        }
        if ((svp = hv_fetch(hv, "mysql_read_timeout", 18, FALSE))
            &&  *svp  &&  SvTRUE(*svp))
        {
          int to = SvIV(*svp);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->mysql_dr_connect: Setting" \
                          " read timeout (%d).\n",to);
          mysql_options(sock, MYSQL_OPT_READ_TIMEOUT,
                        (const char *)&to);
        }
        if ((svp = hv_fetch(hv, "mysql_skip_secure_auth", 22, FALSE)) &&
            *svp  &&  SvTRUE(*svp))
        {
#if LIBMYSQL_VERSION_ID > SECURE_AUTH_LAST_VERSION
          croak("mysql_skip_secure_auth not supported");
#endif
#if MYSQL_VERSION_ID <= SECURE_AUTH_LAST_VERSION
          my_bool secauth = 0;
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->mysql_dr_connect: Skipping" \
                          " secure auth\n");
          mysql_options(sock, MYSQL_SECURE_AUTH, &secauth);
#endif
        }
        if ((svp = hv_fetch(hv, "mysql_read_default_file", 23, FALSE)) &&
            *svp  &&  SvTRUE(*svp))
        {
          char* df = SvPV(*svp, lna);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->mysql_dr_connect: Reading" \
                          " default file %s.\n", df);
          mysql_options(sock, MYSQL_READ_DEFAULT_FILE, df);
        }
        if ((svp = hv_fetch(hv, "mysql_read_default_group", 24,
                            FALSE))  &&
            *svp  &&  SvTRUE(*svp)) {
          char* gr = SvPV(*svp, lna);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                    "imp_dbh->mysql_dr_connect: Using" \
                    " default group %s.\n", gr);

          mysql_options(sock, MYSQL_READ_DEFAULT_GROUP, gr);
        }
        #if (MYSQL_VERSION_ID >= 50606)
          if ((svp = hv_fetch(hv, "mysql_conn_attrs", 16, FALSE)) && *svp) {
              HV* attrs = (HV*) SvRV(*svp);
              HE* entry = NULL;
              I32 num_entries = hv_iterinit(attrs);
              while (num_entries && (entry = hv_iternext(attrs))) {
                  I32 retlen = 0;
                  char *attr_name = hv_iterkey(entry, &retlen);
                  SV *sv_attr_val = hv_iterval(attrs, entry);
                  char *attr_val  = SvPV(sv_attr_val, lna);
                  mysql_options4(sock, MYSQL_OPT_CONNECT_ATTR_ADD, attr_name, attr_val);
              }
          }
        #endif
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
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->use_mysql_use_result: %d\n",
                          imp_dbh->use_mysql_use_result);
        }
        if ((svp = hv_fetch(hv, "mysql_bind_type_guessing", 24, TRUE)) && *svp)
        {
          imp_dbh->bind_type_guessing= SvTRUE(*svp);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->bind_type_guessing: %d\n",
                          imp_dbh->bind_type_guessing);
        }
        if ((svp = hv_fetch(hv, "mysql_bind_comment_placeholders", 31, FALSE)) && *svp)
        {
          imp_dbh->bind_comment_placeholders = SvTRUE(*svp);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->bind_comment_placeholders: %d\n",
                          imp_dbh->bind_comment_placeholders);
        }
        if ((svp = hv_fetch(hv, "mysql_no_autocommit_cmd", 23, FALSE)) && *svp)
        {
          imp_dbh->no_autocommit_cmd= SvTRUE(*svp);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->no_autocommit_cmd: %d\n",
                          imp_dbh->no_autocommit_cmd);
        }
#if FABRIC_SUPPORT
        if ((svp = hv_fetch(hv, "mysql_use_fabric", 16, FALSE)) &&
            *svp && SvTRUE(*svp))
        {
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->use_fabric: Enabling use of" \
                          " MySQL Fabric.\n");
          mysql_options(sock, MYSQL_OPT_USE_FABRIC, NULL);
        }
#endif

#if defined(CLIENT_MULTI_STATEMENTS)
	if ((svp = hv_fetch(hv, "mysql_multi_statements", 22, FALSE)) && *svp)
        {
	  if (SvTRUE(*svp))
	    client_flag |= CLIENT_MULTI_STATEMENTS;
          else
            client_flag &= ~CLIENT_MULTI_STATEMENTS;
	}
#endif

#if MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION
	/* took out  client_flag |= CLIENT_PROTOCOL_41; */
	/* because libmysql.c already sets this no matter what */
	if ((svp = hv_fetch(hv, "mysql_server_prepare", 20, FALSE))
            && *svp)
        {
	  if (SvTRUE(*svp))
          {
	    client_flag |= CLIENT_PROTOCOL_41;
            imp_dbh->use_server_side_prepare = TRUE;
	  }
          else
          {
	    client_flag &= ~CLIENT_PROTOCOL_41;
            imp_dbh->use_server_side_prepare = FALSE;
	  }
	}
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                        "imp_dbh->use_server_side_prepare: %d\n",
                        imp_dbh->use_server_side_prepare);

        if ((svp = hv_fetch(hv, "mysql_server_prepare_disable_fallback", 37, FALSE)) && *svp)
          imp_dbh->disable_fallback_for_server_prepare = SvTRUE(*svp);
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                        "imp_dbh->disable_fallback_for_server_prepare: %d\n",
                        imp_dbh->disable_fallback_for_server_prepare);
#endif

        if ((svp = hv_fetch(hv, "mysql_enable_utf8mb4", 20, FALSE)) && *svp && SvTRUE(*svp)) {
          mysql_options(sock, MYSQL_SET_CHARSET_NAME, "utf8mb4");
        }
        else if ((svp = hv_fetch(hv, "mysql_enable_utf8", 17, FALSE)) && *svp) {
          /* Do not touch imp_dbh->enable_utf8 as we are called earlier
           * than it is set and mysql_options() must be before:
           * mysql_real_connect()
          */
         mysql_options(sock, MYSQL_SET_CHARSET_NAME,
                       (SvTRUE(*svp) ? "utf8" : "latin1"));
         if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
           PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                         "mysql_options: MYSQL_SET_CHARSET_NAME=%s\n",
                         (SvTRUE(*svp) ? "utf8" : "latin1"));
        }

	if ((svp = hv_fetch(hv, "mysql_ssl", 9, FALSE)) && *svp && SvTRUE(*svp))
          {
	    my_bool ssl_enforce = 1;
#if defined(DBD_MYSQL_WITH_SSL) && !defined(DBD_MYSQL_EMBEDDED) && \
    (defined(CLIENT_SSL) || (MYSQL_VERSION_ID >= 40000))
	    char *client_key = NULL;
	    char *client_cert = NULL;
	    char *ca_file = NULL;
	    char *ca_path = NULL;
	    char *cipher = NULL;
	    STRLEN lna;
	    unsigned int ssl_mode;
	    my_bool ssl_verify = 0;
	    my_bool ssl_verify_set = 0;

            /* Verify if the hostname we connect to matches the hostname in the certificate */
	    if ((svp = hv_fetch(hv, "mysql_ssl_verify_server_cert", 28, FALSE)) && *svp) {
  #if defined(HAVE_SSL_VERIFY) || defined(HAVE_SSL_MODE)
	      ssl_verify = SvTRUE(*svp);
	      ssl_verify_set = 1;
  #else
	      set_ssl_error(sock, "mysql_ssl_verify_server_cert=1 is not supported");
	      return NULL;
  #endif
	    }
        if ((svp = hv_fetch(hv, "mysql_ssl_optional", 18, FALSE)) && *svp)
            ssl_enforce = !SvTRUE(*svp);

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

	    if (ssl_verify && !(ca_file || ca_path)) {
	      set_ssl_error(sock, "mysql_ssl_verify_server_cert=1 is not supported without mysql_ssl_ca_file or mysql_ssl_ca_path");
	      return NULL;
	    }

  #ifdef HAVE_SSL_MODE

        if (!ssl_enforce)
          ssl_mode = SSL_MODE_PREFERRED;
        else if (ssl_verify)
	      ssl_mode = SSL_MODE_VERIFY_IDENTITY;
	    else if (ca_file || ca_path)
	      ssl_mode = SSL_MODE_VERIFY_CA;
	    else
	      ssl_mode = SSL_MODE_REQUIRED;
	    if (mysql_options(sock, MYSQL_OPT_SSL_MODE, &ssl_mode) != 0) {
	      set_ssl_error(sock, "Enforcing SSL encryption is not supported");
	      return NULL;
	    }

  #else

        if (ssl_enforce) {
    #if defined(HAVE_SSL_MODE_ONLY_REQUIRED)
	      ssl_mode = SSL_MODE_REQUIRED;
	      if (mysql_options(sock, MYSQL_OPT_SSL_MODE, &ssl_mode) != 0) {
	        set_ssl_error(sock, "Enforcing SSL encryption is not supported");
	        return NULL;
	      }
    #elif defined(HAVE_SSL_ENFORCE)
	      if (mysql_options(sock, MYSQL_OPT_SSL_ENFORCE, &ssl_enforce) != 0) {
	        set_ssl_error(sock, "Enforcing SSL encryption is not supported");
	        return NULL;
	      }
    #elif defined(HAVE_SSL_VERIFY)
	      if (!ssl_verify_also_enforce_ssl()) {
	        set_ssl_error(sock, "Enforcing SSL encryption is not supported");
	        return NULL;
	      }
	      if (ssl_verify_set && !ssl_verify) {
	        set_ssl_error(sock, "Enforcing SSL encryption is not supported without mysql_ssl_verify_server_cert=1");
	        return NULL;
	      }
	      ssl_verify = 1;
    #else
	      set_ssl_error(sock, "Enforcing SSL encryption is not supported");
	      return NULL;
    #endif
        }

    #ifdef HAVE_SSL_VERIFY
        if (!ssl_enforce && ssl_verify && ssl_verify_also_enforce_ssl()) {
            set_ssl_error(sock, "mysql_ssl_optional=1 with mysql_ssl_verify_server_cert=1 is not supported");
            return NULL;
        }
    #endif

	    if (ssl_verify) {
          if (!ssl_verify_usable() && ssl_enforce && ssl_verify_set) {
	        set_ssl_error(sock, "mysql_ssl_verify_server_cert=1 is broken by current version of MySQL client");
	        return NULL;
	      }
    #ifdef HAVE_SSL_VERIFY
	      if (mysql_options(sock, MYSQL_OPT_SSL_VERIFY_SERVER_CERT, &ssl_verify) != 0) {
	        set_ssl_error(sock, "mysql_ssl_verify_server_cert=1 is not supported");
	        return NULL;
	      }
    #else
	      set_ssl_error(sock, "mysql_ssl_verify_server_cert=1 is not supported");
	      return NULL;
    #endif
	    }

  #endif

	    client_flag |= CLIENT_SSL;
#else
	    if ((svp = hv_fetch(hv, "mysql_ssl_optional", 18, FALSE)) && *svp)
	      ssl_enforce = !SvTRUE(*svp);
            if (ssl_enforce)
            {
	      set_ssl_error(sock, "mysql_ssl=1 is not supported and mysql_ssl_optional is not enabled.");
	      return NULL;
            }
            else
            {
              do_warn(dbh, SL_ERR_NOTAVAILBLE, "mysql_ssl is set but SSL support is not available.");
            }
#endif
	  }
	else
	  {
#ifdef HAVE_SSL_MODE
	    unsigned int ssl_mode = SSL_MODE_DISABLED;
	    mysql_options(sock, MYSQL_OPT_SSL_MODE, &ssl_mode);
#endif
	  }
#if (MYSQL_VERSION_ID >= 32349)
	/*
	 * MySQL 3.23.49 disables LOAD DATA LOCAL by default. Use
	 * mysql_local_infile=1 in the DSN to enable it.
	 */
     if ((svp = hv_fetch( hv, "mysql_local_infile", 18, FALSE))  &&  *svp)
     {
	  unsigned int flag = SvTRUE(*svp);
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
	    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
        "imp_dbh->mysql_dr_connect: Using" \
        " local infile %u.\n", flag);
	  mysql_options(sock, MYSQL_OPT_LOCAL_INFILE, (const char *) &flag);
	}
#endif
      }
    }
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "imp_dbh->mysql_dr_connect: client_flags = %d\n",
		    client_flag);

#if MYSQL_VERSION_ID >= MULTIPLE_RESULT_SET_VERSION
    client_flag|= CLIENT_MULTI_RESULTS;
#endif
    result = mysql_real_connect(sock, host, user, password, dbname,
				portNr, mysql_socket, client_flag);
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "imp_dbh->mysql_dr_connect: <-");

    if (result)
    {
      /*
        we turn off Mysql's auto reconnect and handle re-connecting ourselves
        so that we can keep track of when this happens.
      */
#if MYSQL_VERSION_ID >= 50013
      my_bool reconnect = FALSE;
      mysql_options(result, MYSQL_OPT_RECONNECT, &reconnect);
#else
      result->reconnect = 0;
#endif
#if MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION
      /* connection succeeded. */
      /* imp_dbh == NULL when mysql_dr_connect() is called from mysql.xs
         functions (_admin_internal(),_ListDBs()). */
      if (!(result->client_flag & CLIENT_PROTOCOL_41) && imp_dbh)
        imp_dbh->use_server_side_prepare = FALSE;
#endif

#if MYSQL_ASYNC
      if(imp_dbh) {
          imp_dbh->async_query_in_flight = NULL;
      }
#endif
    }
    else {
      /* 
         sock was allocated with mysql_init() 
         fixes: https://rt.cpan.org/Ticket/Display.html?id=86153

      Safefree(sock);

         rurban: No, we still need this handle later in mysql_dr_error().
         RT #97625. It will be freed as imp_dbh->pmysql in dbd_db_destroy(),
         which is called by the DESTROY handler.
      */
    }
    return result;
  }
}

/*
  safe_hv_fetch
*/
static char *safe_hv_fetch(pTHX_ HV *hv, const char *name, int name_length)
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
static int my_login(pTHX_ SV* dbh, imp_dbh_t *imp_dbh)
{
  SV* sv;
  HV* hv;
  char* dbname;
  char* host;
  char* port;
  char* user;
  char* password;
  char* mysql_socket;
  int   result;
  D_imp_xxh(dbh);

  /* TODO- resolve this so that it is set only if DBI is 1.607 */
#define TAKE_IMP_DATA_VERSION 1
#if TAKE_IMP_DATA_VERSION
  if (DBIc_has(imp_dbh, DBIcf_IMPSET))
  { /* eg from take_imp_data() */
    if (DBIc_has(imp_dbh, DBIcf_ACTIVE))
    {
      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
        PerlIO_printf(DBIc_LOGPIO(imp_xxh), "my_login skip connect\n");
      /* tell our parent we've adopted an active child */
      ++DBIc_ACTIVE_KIDS(DBIc_PARENT_COM(imp_dbh));
      return TRUE;
    }
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                    "my_login IMPSET but not ACTIVE so connect not skipped\n");
  }
#endif

  sv = DBIc_IMP_DATA(imp_dbh);

  if (!sv  ||  !SvROK(sv))
    return FALSE;

  hv = (HV*) SvRV(sv);
  if (SvTYPE(hv) != SVt_PVHV)
    return FALSE;

  host=		safe_hv_fetch(aTHX_ hv, "host", 4);
  port=		safe_hv_fetch(aTHX_ hv, "port", 4);
  user=		safe_hv_fetch(aTHX_ hv, "user", 4);
  password=	safe_hv_fetch(aTHX_ hv, "password", 8);
  dbname=	safe_hv_fetch(aTHX_ hv, "database", 8);
  mysql_socket=	safe_hv_fetch(aTHX_ hv, "mysql_socket", 12);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
		  "imp_dbh->my_login : dbname = %s, uid = %s, pwd = %s," \
		  "host = %s, port = %s\n",
		  dbname ? dbname : "NULL",
		  user ? user : "NULL",
		  password ? password : "NULL",
		  host ? host : "NULL",
		  port ? port : "NULL");

  if (!imp_dbh->pmysql) {
     Newz(908, imp_dbh->pmysql, 1, MYSQL);
     imp_dbh->pmysql->net.fd = -1;
  }
  result = mysql_dr_connect(dbh, imp_dbh->pmysql, mysql_socket, host, port, user,
			  password, dbname, imp_dbh) ? TRUE : FALSE;
  return result;
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
 *           password - password to connect with
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
  dTHX; 
  D_imp_xxh(dbh);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
		  "imp_dbh->connect: dsn = %s, uid = %s, pwd = %s\n",
		  dbname ? dbname : "NULL",
		  user ? user : "NULL",
		  password ? password : "NULL");

  imp_dbh->stats.auto_reconnects_ok= 0;
  imp_dbh->stats.auto_reconnects_failed= 0;
  imp_dbh->bind_type_guessing= FALSE;
  imp_dbh->bind_comment_placeholders= FALSE;
  imp_dbh->has_transactions= TRUE;
 /* Safer we flip this to TRUE perl side if we detect a mod_perl env. */
  imp_dbh->auto_reconnect = FALSE;

  imp_dbh->enable_utf8 = FALSE;     /* initialize mysql_enable_utf8 */
  imp_dbh->enable_utf8mb4 = FALSE;  /* initialize mysql_enable_utf8mb4 */

  if (!my_login(aTHX_ dbh, imp_dbh))
  {
    if(imp_dbh->pmysql) {
        do_error(dbh, mysql_errno(imp_dbh->pmysql),
                mysql_error(imp_dbh->pmysql) ,mysql_sqlstate(imp_dbh->pmysql));
        Safefree(imp_dbh->pmysql);

    }
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
 *  Purpose: You guess what they should do. 
 *
 *  Input:   dbh - database handle being committed or rolled back
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
    return FALSE;

  ASYNC_CHECK_RETURN(dbh, FALSE);

  if (imp_dbh->has_transactions)
  {
#if MYSQL_VERSION_ID < SERVER_PREPARE_VERSION
    if (mysql_real_query(imp_dbh->pmysql, "COMMIT", 6))
#else
    if (mysql_commit(imp_dbh->pmysql))
#endif
    {
      do_error(dbh, mysql_errno(imp_dbh->pmysql), mysql_error(imp_dbh->pmysql)
               ,mysql_sqlstate(imp_dbh->pmysql));
      return FALSE;
    }
  }
  else
    do_warn(dbh, JW_ERR_NOT_IMPLEMENTED,
            "Commit ineffective because transactions are not available");
  return TRUE;
}

/*
 dbd_db_rollback
*/
int
dbd_db_rollback(SV* dbh, imp_dbh_t* imp_dbh) {
  /* croak, if not in AutoCommit mode */
  if (DBIc_has(imp_dbh, DBIcf_AutoCommit))
    return FALSE;

  ASYNC_CHECK_RETURN(dbh, FALSE);

  if (imp_dbh->has_transactions)
  {
#if MYSQL_VERSION_ID < SERVER_PREPARE_VERSION
    if (mysql_real_query(imp_dbh->pmysql, "ROLLBACK", 8))
#else
      if (mysql_rollback(imp_dbh->pmysql))
#endif
      {
        do_error(dbh, mysql_errno(imp_dbh->pmysql),
                 mysql_error(imp_dbh->pmysql) ,mysql_sqlstate(imp_dbh->pmysql));
        return FALSE;
      }
  }
  else
    do_error(dbh, JW_ERR_NOT_IMPLEMENTED,
             "Rollback ineffective because transactions are not available" ,NULL);
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
  dTHX;
  D_imp_xxh(dbh);

  /* We assume that disconnect will always work       */
  /* since most errors imply already disconnected.    */
  DBIc_ACTIVE_off(imp_dbh);
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "imp_dbh->pmysql: %p\n",
		              imp_dbh->pmysql);
  mysql_close(imp_dbh->pmysql );
  imp_dbh->pmysql->net.fd = -1;

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
  dTHX;
#if defined(DBD_MYSQL_EMBEDDED)
  D_imp_xxh(drh);
#else
  PERL_UNUSED_ARG(drh);
#endif

#if defined(DBD_MYSQL_EMBEDDED)
  if (imp_drh->embedded.state)
  {
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "Stop embedded server\n");

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
#else
  mysql_server_end();
#endif

  /* The disconnect_all concept is flawed and needs more work */
  if (!PL_dirty && !SvTRUE(perl_get_sv("DBI::PERL_ENDING",0))) {
    sv_setiv(DBIc_ERR(imp_drh), (IV)1);
    sv_setpv(DBIc_ERRSTR(imp_drh),
             (char*)"disconnect_all not implemented");
    /* NO EFFECT DBIh_EVENT2(drh, ERROR_event,
      DBIc_ERR(imp_drh), DBIc_ERRSTR(imp_drh)); */
    return FALSE;
  }
  PL_perl_destruct_level = 0;
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
        if ( mysql_real_query(imp_dbh->pmysql, "ROLLBACK", 8))
#else
        if (mysql_rollback(imp_dbh->pmysql))
#endif
            do_error(dbh, TX_ERR_ROLLBACK,"ROLLBACK failed" ,NULL);
    }
    dbd_db_disconnect(dbh, imp_dbh);
  }
  Safefree(imp_dbh->pmysql);

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
  dTHX;
  STRLEN kl;
  char *key = SvPV(keysv, kl);
  SV *cachesv = Nullsv;
  int cacheit = FALSE;
  const bool bool_value = SvTRUE(valuesv);

  if (kl==10 && strEQ(key, "AutoCommit"))
  {
    if (imp_dbh->has_transactions)
    {
      bool oldval = DBIc_has(imp_dbh,DBIcf_AutoCommit) ? 1 : 0;

      if (bool_value == oldval)
        return TRUE;

      /* if setting AutoCommit on ... */
      if (!imp_dbh->no_autocommit_cmd)
      {
        if (
#if MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION
            mysql_autocommit(imp_dbh->pmysql, bool_value)
#else
            mysql_real_query(imp_dbh->pmysql,
                             bool_value ? "SET AUTOCOMMIT=1" : "SET AUTOCOMMIT=0",
                             16)
#endif
           )
        {
          do_error(dbh, TX_ERR_AUTOCOMMIT,
                   bool_value ?
                   "Turning on AutoCommit failed" :
                   "Turning off AutoCommit failed"
                   ,NULL);
          return TRUE;  /* TRUE means we handled it - important to avoid spurious errors */
        }
      }
      DBIc_set(imp_dbh, DBIcf_AutoCommit, bool_value);
    }
    else
    {
      /*
       *  We do support neither transactions nor "AutoCommit".
       *  But we stub it. :-)
      */
      if (!bool_value)
      {
        do_error(dbh, JW_ERR_NOT_IMPLEMENTED,
                 "Transactions not supported by database" ,NULL);
        croak("Transactions not supported by database");
      }
    }
  }
  else if (kl == 16 && strEQ(key,"mysql_use_result"))
    imp_dbh->use_mysql_use_result = bool_value;
  else if (kl == 20 && strEQ(key,"mysql_auto_reconnect"))
    imp_dbh->auto_reconnect = bool_value;
  else if (kl == 20 && strEQ(key, "mysql_server_prepare"))
    imp_dbh->use_server_side_prepare = bool_value;
  else if (kl == 37 && strEQ(key, "mysql_server_prepare_disable_fallback"))
    imp_dbh->disable_fallback_for_server_prepare = bool_value;
  else if (kl == 23 && strEQ(key,"mysql_no_autocommit_cmd"))
    imp_dbh->no_autocommit_cmd = bool_value;
  else if (kl == 24 && strEQ(key,"mysql_bind_type_guessing"))
    imp_dbh->bind_type_guessing = bool_value;
  else if (kl == 31 && strEQ(key,"mysql_bind_comment_placeholders"))
    imp_dbh->bind_type_guessing = bool_value;
  else if (kl == 17 && strEQ(key, "mysql_enable_utf8"))
    imp_dbh->enable_utf8 = bool_value;
  else if (kl == 20 && strEQ(key, "mysql_enable_utf8mb4"))
    imp_dbh->enable_utf8mb4 = bool_value;
#if FABRIC_SUPPORT
  else if (kl == 22 && strEQ(key, "mysql_fabric_opt_group"))
    mysql_options(imp_dbh->pmysql, FABRIC_OPT_GROUP, (void *)SvPVbyte_nolen(valuesv));
  else if (kl == 29 && strEQ(key, "mysql_fabric_opt_default_mode"))
  {
    if (SvOK(valuesv)) {
      STRLEN len;
      const char *str = SvPVbyte(valuesv, len);
      if ( len == 0 || ( len == 2 && (strnEQ(str, "ro", 3) || strnEQ(str, "rw", 3)) ) )
        mysql_options(imp_dbh->pmysql, FABRIC_OPT_DEFAULT_MODE, len == 0 ? NULL : str);
      else
        croak("Valid settings for FABRIC_OPT_DEFAULT_MODE are 'ro', 'rw', or undef/empty string");
    }
    else {
      mysql_options(imp_dbh->pmysql, FABRIC_OPT_DEFAULT_MODE, NULL);
    }
  }
  else if (kl == 21 && strEQ(key, "mysql_fabric_opt_mode"))
  {
    STRLEN len;
    const char *str = SvPVbyte(valuesv, len);
    if (len != 2 || (strnNE(str, "ro", 3) && strnNE(str, "rw", 3)))
      croak("Valid settings for FABRIC_OPT_MODE are 'ro' or 'rw'");

    mysql_options(imp_dbh->pmysql, FABRIC_OPT_MODE, str);
  }
  else if (kl == 34 && strEQ(key, "mysql_fabric_opt_group_credentials"))
  {
    croak("'fabric_opt_group_credentials' is not supported");
  }
#endif
  else
    return FALSE;				/* Unknown key */

  if (cacheit) /* cache value for later DBI 'quick' fetch? */
    (void)hv_store((HV*)SvRV(dbh), key, kl, cachesv, 0);
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
 *  Returns: An SV*, if successful; NULL otherwise
 *
 *  Notes:   Do not forget to call sv_2mortal in the former case!
 *
 **************************************************************************/
static SV*
my_ulonglong2str(pTHX_ my_ulonglong val)
{
  char buf[64];
  char *ptr = buf + sizeof(buf) - 1;

  if (val == 0)
    return newSVpvn("0", 1);

  *ptr = '\0';
  while (val > 0)
  {
    *(--ptr) = ('0' + (val % 10));
    val = val / 10;
  }
  return newSVpvn(ptr, (buf+ sizeof(buf) - 1) - ptr);
}

SV* dbd_db_FETCH_attrib(SV *dbh, imp_dbh_t *imp_dbh, SV *keysv)
{
  dTHX;
  STRLEN kl;
  char *key = SvPV(keysv, kl);
  SV* result = NULL;
  dbh= dbh;

  switch (*key) {
    case 'A':
      if (strEQ(key, "AutoCommit"))
      {
        if (imp_dbh->has_transactions)
          return sv_2mortal(boolSV(DBIc_has(imp_dbh,DBIcf_AutoCommit)));
        /* Default */
        return &PL_sv_yes;
      }
      break;
  }
  if (strncmp(key, "mysql_", 6) == 0) {
    key = key+6;
    kl = kl-6;
  }

  /* MONTY:  Check if kl should not be used or used everywhere */
  switch(*key) {
  case 'a':
    if (kl == strlen("auto_reconnect") && strEQ(key, "auto_reconnect"))
      result= sv_2mortal(newSViv(imp_dbh->auto_reconnect));
    break;
  case 'b':
    if (kl == strlen("bind_type_guessing") &&
        strEQ(key, "bind_type_guessing"))
    {
      result = sv_2mortal(newSViv(imp_dbh->bind_type_guessing));
    }
    else if (kl == strlen("bind_comment_placeholders") &&
        strEQ(key, "bind_comment_placeholders"))
    {
      result = sv_2mortal(newSViv(imp_dbh->bind_comment_placeholders));
    }
    break;
  case 'c':
    if (kl == 10 && strEQ(key, "clientinfo"))
    {
      const char* clientinfo = mysql_get_client_info();
      result= clientinfo ?
        sv_2mortal(newSVpvn(clientinfo, strlen(clientinfo))) : &PL_sv_undef;
    }
    else if (kl == 13 && strEQ(key, "clientversion"))
    {
      result= sv_2mortal(my_ulonglong2str(aTHX_ mysql_get_client_version()));
    }
    break;
  case 'e':
    if (strEQ(key, "errno"))
      result= sv_2mortal(newSViv((IV)mysql_errno(imp_dbh->pmysql)));
    else if ( strEQ(key, "error") || strEQ(key, "errmsg"))
    {
    /* Note that errmsg is obsolete, as of 2.09! */
      const char* msg = mysql_error(imp_dbh->pmysql);
      result= sv_2mortal(newSVpvn(msg, strlen(msg)));
    }
    else if (kl == strlen("enable_utf8mb4") && strEQ(key, "enable_utf8mb4"))
        result = sv_2mortal(newSViv(imp_dbh->enable_utf8mb4));
    else if (kl == strlen("enable_utf8") && strEQ(key, "enable_utf8"))
        result = sv_2mortal(newSViv(imp_dbh->enable_utf8));
    break;

  case 'd':
    if (strEQ(key, "dbd_stats"))
    {
      HV* hv = newHV();
      (void)hv_store(
               hv,
               "auto_reconnects_ok",
               strlen("auto_reconnects_ok"),
               newSViv(imp_dbh->stats.auto_reconnects_ok),
               0
              );
      (void)hv_store(
               hv,
               "auto_reconnects_failed",
               strlen("auto_reconnects_failed"),
               newSViv(imp_dbh->stats.auto_reconnects_failed),
               0
              );

      result= sv_2mortal((newRV_noinc((SV*)hv)));
    }

  case 'h':
    if (strEQ(key, "hostinfo"))
    {
      const char* hostinfo = mysql_get_host_info(imp_dbh->pmysql);
      result= hostinfo ?
        sv_2mortal(newSVpvn(hostinfo, strlen(hostinfo))) : &PL_sv_undef;
    }
    break;

  case 'i':
    if (strEQ(key, "info"))
    {
      const char* info = mysql_info(imp_dbh->pmysql);
      result= info ? sv_2mortal(newSVpvn(info, strlen(info))) : &PL_sv_undef;
    }
    else if (kl == 8  &&  strEQ(key, "insertid"))
      /* We cannot return an IV, because the insertid is a long. */
      result= sv_2mortal(my_ulonglong2str(aTHX_ mysql_insert_id(imp_dbh->pmysql)));
    break;
  case 'n':
    if (kl == strlen("no_autocommit_cmd") &&
        strEQ(key, "no_autocommit_cmd"))
      result = sv_2mortal(newSViv(imp_dbh->no_autocommit_cmd));
    break;

  case 'p':
    if (kl == 9  &&  strEQ(key, "protoinfo"))
      result= sv_2mortal(newSViv(mysql_get_proto_info(imp_dbh->pmysql)));
    break;

  case 's':
    if (kl == 10 && strEQ(key, "serverinfo")) {
      const char* serverinfo = mysql_get_server_info(imp_dbh->pmysql);
      result= serverinfo ?
        sv_2mortal(newSVpvn(serverinfo, strlen(serverinfo))) : &PL_sv_undef;
    } 
    else if (kl == 13 && strEQ(key, "serverversion"))
      result= sv_2mortal(my_ulonglong2str(aTHX_ mysql_get_server_version(imp_dbh->pmysql)));
    else if (strEQ(key, "sock"))
      result= sv_2mortal(newSViv(PTR2IV(imp_dbh->pmysql)));
    else if (strEQ(key, "sockfd"))
      result= (imp_dbh->pmysql->net.fd != -1) ?
        sv_2mortal(newSViv((IV) imp_dbh->pmysql->net.fd)) : &PL_sv_undef;
    else if (strEQ(key, "stat"))
    {
      const char* stats = mysql_stat(imp_dbh->pmysql);
      result= stats ?
        sv_2mortal(newSVpvn(stats, strlen(stats))) : &PL_sv_undef;
    }
    else if (strEQ(key, "stats"))
    {
      /* Obsolete, as of 2.09 */
      const char* stats = mysql_stat(imp_dbh->pmysql);
      result= stats ?
        sv_2mortal(newSVpvn(stats, strlen(stats))) : &PL_sv_undef;
    }
    else if (kl == 14 && strEQ(key,"server_prepare"))
        result= sv_2mortal(newSViv((IV) imp_dbh->use_server_side_prepare));
    else if (kl == 31 && strEQ(key, "server_prepare_disable_fallback"))
        result= sv_2mortal(newSViv((IV) imp_dbh->disable_fallback_for_server_prepare));
    break;

  case 't':
    if (kl == 9  &&  strEQ(key, "thread_id"))
      result= sv_2mortal(newSViv(mysql_thread_id(imp_dbh->pmysql)));
    break;

  case 'w':
    if (kl == 13 && strEQ(key, "warning_count"))
      result= sv_2mortal(newSViv(mysql_warning_count(imp_dbh->pmysql)));
    break;
  case 'u':
    if (strEQ(key, "use_result"))
    {
      result= sv_2mortal(newSViv((IV) imp_dbh->use_mysql_use_result));
    }
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
  dTHX;
#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
#if MYSQL_VERSION_ID < CALL_PLACEHOLDER_VERSION
  char *str_ptr, *str_last_ptr;
#if MYSQL_VERSION_ID < LIMIT_PLACEHOLDER_VERSION
  int limit_flag=0;
#endif
#endif
  int prepare_retval;
  MYSQL_BIND *bind, *bind_end;
  imp_sth_phb_t *fbind;
#endif
  D_imp_xxh(sth);
  D_imp_dbh_from_sth;

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                 "\t-> dbd_st_prepare MYSQL_VERSION_ID %d, SQL statement: %s\n",
                  MYSQL_VERSION_ID, statement);

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
 /* Set default value of 'mysql_server_prepare' attribute for sth from dbh */
  imp_sth->use_server_side_prepare= imp_dbh->use_server_side_prepare;
  imp_sth->disable_fallback_for_server_prepare= imp_dbh->disable_fallback_for_server_prepare;
  if (attribs)
  {
    svp= DBD_ATTRIB_GET_SVP(attribs, "mysql_server_prepare", 20);
    imp_sth->use_server_side_prepare = (svp) ?
      SvTRUE(*svp) : imp_dbh->use_server_side_prepare;

    svp= DBD_ATTRIB_GET_SVP(attribs, "mysql_server_prepare_disable_fallback", 37);
    imp_sth->disable_fallback_for_server_prepare = (svp) ?
      SvTRUE(*svp) : imp_dbh->disable_fallback_for_server_prepare;

    svp = DBD_ATTRIB_GET_SVP(attribs, "async", 5);

    if(svp && SvTRUE(*svp)) {
#if MYSQL_ASYNC
        imp_sth->is_async = TRUE;
        if (imp_sth->disable_fallback_for_server_prepare)
        {
          do_error(sth, ER_UNSUPPORTED_PS,
                   "Async option not supported with server side prepare", "HY000");
          return 0;
        }
        imp_sth->use_server_side_prepare = FALSE;
#else
        do_error(sth, 2000,
                 "Async support was not built into this version of DBD::mysql", "HY000");
        return 0;
#endif
    }
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

  /*
     Clean-up previous result set(s) for sth to prevent
     'Commands out of sync' error 
  */
  mysql_st_free_result_sets(sth, imp_sth);

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION && MYSQL_VERSION_ID < CALL_PLACEHOLDER_VERSION
  if (imp_sth->use_server_side_prepare)
  {
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                    "\t\tuse_server_side_prepare set, check restrictions\n");
    /*
      This code is here because placeholder support is not implemented for
      statements with :-
      1. LIMIT < 5.0.7
      2. CALL < 5.5.3 (Added support for out & inout parameters)
      In these cases we have to disable server side prepared statements
      NOTE: These checks could cause a false positive on statements which
      include columns / table names that match "call " or " limit "
    */ 
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh),
#if MYSQL_VERSION_ID < LIMIT_PLACEHOLDER_VERSION
                    "\t\tneed to test for LIMIT & CALL\n");
#else
                    "\t\tneed to test for restrictions\n");
#endif
    str_last_ptr = statement + strlen(statement);
    for (str_ptr= statement; str_ptr < str_last_ptr; str_ptr++)
    {
#if MYSQL_VERSION_ID < LIMIT_PLACEHOLDER_VERSION
      /*
        Place holders not supported in LIMIT's
      */
      if (limit_flag)
      {
        if (*str_ptr == '?')
        {
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                    "\t\tLIMIT and ? found, set to use_server_side_prepare=0\n");
          if (imp_sth->disable_fallback_for_server_prepare)
          {
            do_error(sth, ER_UNSUPPORTED_PS,
                     "\"LIMIT ?\" not supported with server side prepare",
                     "HY000");
            mysql_stmt_close(imp_sth->stmt);
            imp_sth->stmt= NULL;
            return FALSE;
          }
          /* ... then we do not want to try server side prepare (use emulation) */
          imp_sth->use_server_side_prepare= 0;
          break;
        }
      }
      else if (str_ptr < str_last_ptr - 6 &&
          isspace(*(str_ptr + 0)) &&
          tolower(*(str_ptr + 1)) == 'l' &&
          tolower(*(str_ptr + 2)) == 'i' &&
          tolower(*(str_ptr + 3)) == 'm' &&
          tolower(*(str_ptr + 4)) == 'i' &&
          tolower(*(str_ptr + 5)) == 't' &&
          isspace(*(str_ptr + 6)))
      {
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "LIMIT set limit flag to 1\n");
        limit_flag= 1;
      }
#endif
      /*
        Place holders not supported in CALL's
      */
      if (str_ptr < str_last_ptr - 4 &&
           tolower(*(str_ptr + 0)) == 'c' &&
           tolower(*(str_ptr + 1)) == 'a' &&
           tolower(*(str_ptr + 2)) == 'l' &&
           tolower(*(str_ptr + 3)) == 'l' &&
           isspace(*(str_ptr + 4)))
      {
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "Disable PS mode for CALL()\n");
          if (imp_sth->disable_fallback_for_server_prepare)
          {
            do_error(sth, ER_UNSUPPORTED_PS,
                     "\"CALL()\" not supported with server side prepare",
                     "HY000");
            mysql_stmt_close(imp_sth->stmt);
            imp_sth->stmt= NULL;
            return FALSE;
          }
        imp_sth->use_server_side_prepare= 0;
        break;
      }
    }
  }
#endif

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
  if (imp_sth->use_server_side_prepare)
  {
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                    "\t\tuse_server_side_prepare set\n");
    /* do we really need this? If we do, we should return, not just continue */
    if (imp_sth->stmt)
      fprintf(stderr,
              "ERROR: Trying to prepare new stmt while we have \
              already not closed one \n");

    imp_sth->stmt= mysql_stmt_init(imp_dbh->pmysql);

    if (! imp_sth->stmt)
    {
      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
        PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                      "\t\tERROR: Unable to return MYSQL_STMT structure \
                      from mysql_stmt_init(): ERROR NO: %d ERROR MSG:%s\n",
                      mysql_errno(imp_dbh->pmysql),
                      mysql_error(imp_dbh->pmysql));
    }

    prepare_retval= mysql_stmt_prepare(imp_sth->stmt,
                                       statement,
                                       strlen(statement));
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
        PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                      "\t\tmysql_stmt_prepare returned %d\n",
                      prepare_retval);

    if (prepare_retval)
    {
      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
        PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                      "\t\tmysql_stmt_prepare %d %s\n",
                      mysql_stmt_errno(imp_sth->stmt),
                      mysql_stmt_error(imp_sth->stmt));

      /* For commands that are not supported by server side prepared statement
         mechanism lets try to pass them through regular API */
      if (!imp_sth->disable_fallback_for_server_prepare && mysql_stmt_errno(imp_sth->stmt) == ER_UNSUPPORTED_PS)
      {
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                    "\t\tSETTING imp_sth->use_server_side_prepare to 0\n");
        imp_sth->use_server_side_prepare= 0;
      }
      else
      {
        do_error(sth, mysql_stmt_errno(imp_sth->stmt),
                 mysql_stmt_error(imp_sth->stmt),
                mysql_sqlstate(imp_dbh->pmysql));
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
        /* Allocate memory for bind variables */
        imp_sth->bind=            alloc_bind(DBIc_NUM_PARAMS(imp_sth));
        imp_sth->fbind=           alloc_fbind(DBIc_NUM_PARAMS(imp_sth));
        imp_sth->has_been_bound=  0;

        /* Initialize ph variables with  NULL values */
        for (i= 0,
             bind=      imp_sth->bind,
             fbind=     imp_sth->fbind,
             bind_end=  bind+DBIc_NUM_PARAMS(imp_sth);
             bind < bind_end ;
             bind++, fbind++, i++ )
        {
          bind->buffer_type=  MYSQL_TYPE_STRING;
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
    DBIc_NUM_PARAMS(imp_sth) = count_params((imp_xxh_t *)imp_dbh, aTHX_ statement,
                                            imp_dbh->bind_comment_placeholders);
#else
  DBIc_NUM_PARAMS(imp_sth) = count_params((imp_xxh_t *)imp_dbh, aTHX_ statement,
                                          imp_dbh->bind_comment_placeholders);
#endif

  /* Allocate memory for parameters */
  imp_sth->params= alloc_param(DBIc_NUM_PARAMS(imp_sth));
  DBIc_IMPSET_on(imp_sth);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- dbd_st_prepare\n");
  return 1;
}

/***************************************************************************
 * Name: dbd_st_free_result_sets
 *
 * Purpose: Clean-up single or multiple result sets (if any)
 *
 * Inputs: sth - Statement handle
 *         imp_sth - driver's private statement handle
 *
 * Returns: 1 ok
 *          0 error
 *************************************************************************/
int mysql_st_free_result_sets (SV * sth, imp_sth_t * imp_sth)
{
  dTHX;
  D_imp_dbh_from_sth;
  D_imp_xxh(sth);
  int next_result_rc= -1;

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t>- dbd_st_free_result_sets\n");

#if MYSQL_VERSION_ID >= MULTIPLE_RESULT_SET_VERSION
  do
  {
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- dbd_st_free_result_sets RC %d\n", next_result_rc);

    if (next_result_rc == 0)
    {
      if (!(imp_sth->result = mysql_use_result(imp_dbh->pmysql)))
      {
        /* Check for possible error */
        if (mysql_field_count(imp_dbh->pmysql))
        {
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- dbd_st_free_result_sets ERROR: %s\n",
                                  mysql_error(imp_dbh->pmysql));

          do_error(sth, mysql_errno(imp_dbh->pmysql), mysql_error(imp_dbh->pmysql),
                   mysql_sqlstate(imp_dbh->pmysql));
          return 0;
        }
      }
    }
    if (imp_sth->result)
    {
      mysql_free_result(imp_sth->result);
      imp_sth->result=NULL;
    }
  } while ((next_result_rc=mysql_next_result(imp_dbh->pmysql))==0);

  if (next_result_rc > 0)
  {
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- dbd_st_free_result_sets: Error while processing multi-result set: %s\n",
                    mysql_error(imp_dbh->pmysql));

    do_error(sth, mysql_errno(imp_dbh->pmysql), mysql_error(imp_dbh->pmysql),
             mysql_sqlstate(imp_dbh->pmysql));
  }

#else

  if (imp_sth->result)
  {
    mysql_free_result(imp_sth->result);
    imp_sth->result=NULL;
  }
#endif

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- dbd_st_free_result_sets\n");

  return 1;
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
 * Returns: 1 if there are more results sets
 *          0 if there are not
 *         -1 for errors.
 *************************************************************************/
int dbd_st_more_results(SV* sth, imp_sth_t* imp_sth)
{
  dTHX;
  D_imp_dbh_from_sth;
  D_imp_xxh(sth);

  int use_mysql_use_result=imp_sth->use_mysql_use_result;
  int next_result_return_code, i;
  MYSQL* svsock= imp_dbh->pmysql;

  if (!SvROK(sth) || SvTYPE(SvRV(sth)) != SVt_PVHV)
    croak("Expected hash array");

  if (!mysql_more_results(svsock))
  {
    /* No more pending result set(s)*/
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh),
		    "\n      <- dbs_st_more_results no more results\n");
    return 0;
  }

  if (imp_sth->use_server_side_prepare)
  {
    do_warn(sth, JW_ERR_NOT_IMPLEMENTED,
            "Processing of multiple result set is not possible with server side prepare");
    return 0;
  }

  /*
   *  Free cached array attributes
   */
  for (i= 0; i < AV_ATTRIB_LAST;  i++)
  {
    if (imp_sth->av_attr[i])
      SvREFCNT_dec(imp_sth->av_attr[i]);

    imp_sth->av_attr[i]= Nullav;
  }

  /* Release previous MySQL result*/
  if (imp_sth->result)
  {
    mysql_free_result(imp_sth->result);
    imp_sth->result= NULL;
  }

  if (DBIc_ACTIVE(imp_sth))
    DBIc_ACTIVE_off(imp_sth);

  next_result_return_code= mysql_next_result(svsock);

  imp_sth->warning_count = mysql_warning_count(imp_dbh->pmysql);

  /*
    mysql_next_result returns
      0 if there are more results
     -1 if there are no more results
     >0 if there was an error
   */
  if (next_result_return_code > 0)
  {
    do_error(sth, mysql_errno(svsock), mysql_error(svsock),
             mysql_sqlstate(svsock));

    return 0;
  }
  else if(next_result_return_code == -1)                                                                                                                  
  {                                                                                                                                                       
    return 0;                                                                                                                                             
  }  
  else
  {
    /* Store the result from the Query */
    imp_sth->result = use_mysql_use_result ?
     mysql_use_result(svsock) : mysql_store_result(svsock);

    if (mysql_errno(svsock))
    {
      do_error(sth, mysql_errno(svsock), mysql_error(svsock), 
               mysql_sqlstate(svsock));
      return 0;
    }

    imp_sth->row_num= mysql_affected_rows(imp_dbh->pmysql);

    if (imp_sth->result == NULL)
    {
      /* No "real" rowset*/
      DBIc_NUM_FIELDS(imp_sth)= 0; /* for DBI <= 1.53 */
      DBIS->set_attr_k(sth, sv_2mortal(newSVpvn("NUM_OF_FIELDS",13)), 0,
			               sv_2mortal(newSViv(0)));
      return 1;
    }
    else
    {
      /* We have a new rowset */
      imp_sth->currow=0;


      /* delete cached handle attributes */
      /* XXX should be driven by a list to ease maintenance */
      (void)hv_delete((HV*)SvRV(sth), "NAME", 4, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "NULLABLE", 8, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "NUM_OF_FIELDS", 13, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "PRECISION", 9, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "SCALE", 5, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "TYPE", 4, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "mysql_insertid", 14, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "mysql_is_auto_increment", 23, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "mysql_is_blob", 13, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "mysql_is_key", 12, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "mysql_is_num", 12, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "mysql_is_pri_key", 16, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "mysql_length", 12, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "mysql_max_length", 16, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "mysql_table", 11, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "mysql_type", 10, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "mysql_type_name", 15, G_DISCARD);
      (void)hv_delete((HV*)SvRV(sth), "mysql_warning_count", 20, G_DISCARD);

      /* Adjust NUM_OF_FIELDS - which also adjusts the row buffer size */
      DBIc_NUM_FIELDS(imp_sth)= 0; /* for DBI <= 1.53 */
      DBIc_DBISTATE(imp_sth)->set_attr_k(sth, sv_2mortal(newSVpvn("NUM_OF_FIELDS",13)), 0,
          sv_2mortal(newSViv(mysql_num_fields(imp_sth->result)))
      );

      DBIc_ACTIVE_on(imp_sth);

      imp_sth->done_desc = 0;
    }
    imp_dbh->pmysql->net.last_errno= 0;
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
  dTHX;
  bool bind_type_guessing= FALSE;
  bool bind_comment_placeholders= TRUE;
  STRLEN slen;
  char *sbuf = SvPV(statement, slen);
  char *table;
  char *salloc;
  int htype;
#if MYSQL_ASYNC
  bool async = FALSE;
#endif
  my_ulonglong rows= 0;
  /* thank you DBI.c for this info! */
  D_imp_xxh(h);
  attribs= attribs;

  htype= DBIc_TYPE(imp_xxh);
  /*
    It is important to import imp_dbh properly according to the htype
    that it is! Also, one might ask why bind_type_guessing is assigned
    in each block. Well, it's because D_imp_ macros called in these
    blocks make it so imp_dbh is not "visible" or defined outside of the
    if/else (when compiled, it fails for imp_dbh not being defined).
  */
  /* h is a dbh */
  if (htype == DBIt_DB)
  {
    D_imp_dbh(h);
    /* if imp_dbh is not available, it causes segfault (proper) on OpenBSD */
    if (imp_dbh)
    {
      bind_type_guessing= imp_dbh->bind_type_guessing;
      bind_comment_placeholders= imp_dbh->bind_comment_placeholders;
    }
#if MYSQL_ASYNC
    async = (bool) (imp_dbh->async_query_in_flight != NULL);
#endif
  }
  /* h is a sth */
  else
  {
    D_imp_sth(h);
    D_imp_dbh_from_sth;
    /* if imp_dbh is not available, it causes segfault (proper) on OpenBSD */
    if (imp_dbh)
    {
      bind_type_guessing= imp_dbh->bind_type_guessing;
      bind_comment_placeholders= imp_dbh->bind_comment_placeholders;
    }
#if MYSQL_ASYNC
    async = imp_sth->is_async;
    if(async) {
        imp_dbh->async_query_in_flight = imp_sth;
    } else {
        imp_dbh->async_query_in_flight = NULL;
    }
#endif
  }

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "mysql_st_internal_execute MYSQL_VERSION_ID %d\n",
                  MYSQL_VERSION_ID );

  salloc= parse_params(imp_xxh,
                              aTHX_ svsock,
                              sbuf,
                              &slen,
                              params,
                              num_params,
                              bind_type_guessing,
                              bind_comment_placeholders);

  if (salloc)
  {
    sbuf= salloc;
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "Binding parameters: %s\n", sbuf);
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
      do_error(h, JW_ERR_QUERY, "Missing table name" ,NULL);
      return -2;
    }
    if (!(table= malloc(slen+1)))
    {
      do_error(h, JW_ERR_MEM, "Out of memory" ,NULL);
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
      do_error(h, mysql_errno(svsock), mysql_error(svsock)
               ,mysql_sqlstate(svsock));
      return -2;
    }

    return 0;
  }

#if MYSQL_ASYNC
  if(async) {
    if((mysql_send_query(svsock, sbuf, slen)) &&
       (!mysql_db_reconnect(h) ||
        (mysql_send_query(svsock, sbuf, slen))))
    {
        rows = -2;
    } else {
        rows = 0;
    }
  } else {
#endif
      if ((mysql_real_query(svsock, sbuf, slen))  &&
          (!mysql_db_reconnect(h)  ||
           (mysql_real_query(svsock, sbuf, slen))))
      {
        rows = -2;
      } else {
          /** Store the result from the Query */
          *result= use_mysql_use_result ?
            mysql_use_result(svsock) : mysql_store_result(svsock);

          if (mysql_errno(svsock))
            rows = -2;
          else if (*result)
            rows = mysql_num_rows(*result);
          else {
            rows = mysql_affected_rows(svsock);
            /* mysql_affected_rows(): -1 indicates that the query returned an error */
            if (rows == (my_ulonglong)-1)
              rows = -2;
          }
      }
#if MYSQL_ASYNC
  }
#endif

  if (salloc)
    Safefree(salloc);

  if(rows == (my_ulonglong)-2) {
    do_error(h, mysql_errno(svsock), mysql_error(svsock), 
             mysql_sqlstate(svsock));
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "IGNORING ERROR errno %d\n", mysql_errno(svsock));
  }
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
  int i;
  enum enum_field_types enum_type;
  dTHX;
  int execute_retval;
  my_ulonglong rows=0;
  D_imp_xxh(sth);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
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

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "\t\tmysql_st_internal_execute41 calling mysql_execute with %d num_params\n",
                  num_params);

  execute_retval= mysql_stmt_execute(stmt);
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
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

    /* mysql_stmt_affected_rows(): -1 indicates that the query returned an error */
    if (rows == (my_ulonglong)-1)
      goto error;
  }
  /*
    This statement returns a result set (SELECT...)
  */
  else
  {
    for (i = mysql_stmt_field_count(stmt) - 1; i >=0; --i) {
        enum_type = mysql_to_perl_type(stmt->fields[i].type);
        if (enum_type != MYSQL_TYPE_DOUBLE && enum_type != MYSQL_TYPE_LONG && enum_type != MYSQL_TYPE_LONGLONG && enum_type != MYSQL_TYPE_BIT)
        {
            /* mysql_stmt_store_result to update MYSQL_FIELD->max_length */
            my_bool on = 1;
            mysql_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, &on);
            break;
        }
    }
    /* Get the total rows affected and return */
    if (mysql_stmt_store_result(stmt))
      goto error;
    else
      rows= mysql_stmt_num_rows(stmt);
  }
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "\t<- mysql_internal_execute_41 returning %llu rows\n",
                  rows);
  return(rows);

error:
  if (*result)
  {
    mysql_free_result(*result);
    *result= 0;
  }
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "     errno %d err message %s\n",
                  mysql_stmt_errno(stmt),
                  mysql_stmt_error(stmt));
  do_error(sth, mysql_stmt_errno(stmt), mysql_stmt_error(stmt),
           mysql_stmt_sqlstate(stmt));
  mysql_stmt_reset(stmt);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
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
  dTHX;
  char actual_row_num[64];
  int i;
  SV **statement;
  D_imp_dbh_from_sth;
  D_imp_xxh(sth);
#if defined (dTHR)
  dTHR;
#endif
#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
  int use_server_side_prepare = imp_sth->use_server_side_prepare;
  int disable_fallback_for_server_prepare = imp_sth->disable_fallback_for_server_prepare;
#endif

  ASYNC_CHECK_RETURN(sth, -2);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
      " -> dbd_st_execute for %p\n", sth);

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

  /* 
     Clean-up previous result set(s) for sth to prevent
     'Commands out of sync' error 
  */
  mysql_st_free_result_sets (sth, imp_sth);

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
  if (use_server_side_prepare)
  {
    if (imp_sth->use_mysql_use_result)
    {
      if (disable_fallback_for_server_prepare)
      {
        do_error(sth, ER_UNSUPPORTED_PS,
                 "\"mysql_use_result\" not supported with server side prepare",
                 "HY000");
        return 0;
      }
      use_server_side_prepare = 0;
    }

    if (use_server_side_prepare)
    {
      imp_sth->row_num= mysql_st_internal_execute41(
                                                    sth,
                                                    DBIc_NUM_PARAMS(imp_sth),
                                                    &imp_sth->result,
                                                    imp_sth->stmt,
                                                    imp_sth->bind,
                                                    &imp_sth->has_been_bound
                                                   );
      if (imp_sth->row_num == (my_ulonglong)-2) /* -2 means error */
      {
        SV *err = DBIc_ERR(imp_xxh);
        if (!disable_fallback_for_server_prepare && SvIV(err) == ER_UNSUPPORTED_PS)
        {
          use_server_side_prepare = 0;
        }
      }
    }
  }

  if (!use_server_side_prepare)
#endif
  {
    imp_sth->row_num= mysql_st_internal_execute(
                                                sth,
                                                *statement,
                                                NULL,
                                                DBIc_NUM_PARAMS(imp_sth),
                                                imp_sth->params,
                                                &imp_sth->result,
                                                imp_dbh->pmysql,
                                                imp_sth->use_mysql_use_result
                                               );
#if MYSQL_ASYNC
    if(imp_dbh->async_query_in_flight) {
        DBIc_ACTIVE_on(imp_sth);
        return 0;
    }
#endif
  }

  if (imp_sth->row_num+1 != (my_ulonglong)-1)
  {
    if (!imp_sth->result)
    {
      imp_sth->insertid= mysql_insert_id(imp_dbh->pmysql);
#if MYSQL_VERSION_ID >= MULTIPLE_RESULT_SET_VERSION
      if (mysql_more_results(imp_dbh->pmysql))
        DBIc_ACTIVE_on(imp_sth);
#endif
    }
    else
    {
      /** Store the result in the current statement handle */
      DBIc_NUM_FIELDS(imp_sth)= mysql_num_fields(imp_sth->result);
      DBIc_ACTIVE_on(imp_sth);
      if (!use_server_side_prepare)
        imp_sth->done_desc= 0;
      imp_sth->fetch_done= 0;
    }
  }

  imp_sth->warning_count = mysql_warning_count(imp_dbh->pmysql);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
  {
    /* 
      PerlIO_printf doesn't always handle imp_sth->row_num %llu 
      consistently!!
    */
    sprintf(actual_row_num, "%llu", imp_sth->row_num);
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
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
  dTHX;
  D_imp_xxh(sth);
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t--> dbd_describe\n");


#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION

  if (imp_sth->use_server_side_prepare)
  {
    int i;
    int col_type;
    int num_fields= DBIc_NUM_FIELDS(imp_sth);
    imp_sth_fbh_t *fbh;
    MYSQL_BIND *buffer;
    MYSQL_FIELD *fields;

    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tdbd_describe() num_fields %d\n",
                    num_fields);

    if (imp_sth->done_desc)
      return TRUE;

    if (!num_fields || !imp_sth->result)
    {
      /* no metadata */
      do_error(sth, JW_ERR_SEQUENCE,
               "no metadata information while trying describe result set",
               NULL);
      return 0;
    }

    /* allocate fields buffers  */
    if (  !(imp_sth->fbh= alloc_fbuffer(num_fields))
          || !(imp_sth->buffer= alloc_bind(num_fields)) )
    {
      /* Out of memory */
      do_error(sth, JW_ERR_SEQUENCE,
               "Out of memory in dbd_sescribe()",NULL);
      return 0;
    }

    fields= mysql_fetch_fields(imp_sth->result);

    for (
         fbh= imp_sth->fbh, buffer= (MYSQL_BIND*)imp_sth->buffer, i= 0;
         i < num_fields;
         i++, fbh++, buffer++
        )
    {
      /* get the column type */
      col_type = fields ? fields[i].type : MYSQL_TYPE_STRING;

      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      {
        PerlIO_printf(DBIc_LOGPIO(imp_xxh),"\t\ti %d col_type %d fbh->length %lu\n",
                      i, col_type, fbh->length);
        PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                      "\t\tfields[i].length %lu fields[i].max_length %lu fields[i].type %d fields[i].charsetnr %d\n",
                      fields[i].length, fields[i].max_length, fields[i].type,
                      fields[i].charsetnr);
      }
      fbh->charsetnr = fields[i].charsetnr;
#if MYSQL_VERSION_ID < FIELD_CHARSETNR_VERSION 
      fbh->flags     = fields[i].flags;
#endif

      buffer->buffer_type= mysql_to_perl_type(col_type);
      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
        PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tmysql_to_perl_type returned %d\n",
                      col_type);
      buffer->length= &(fbh->length);
      buffer->is_null= (my_bool*) &(fbh->is_null);
#if MYSQL_VERSION_ID >= NEW_DATATYPE_VERSION
      buffer->error= (my_bool*) &(fbh->error);
#endif

      if (fields[i].flags & ZEROFILL_FLAG)
        buffer->buffer_type = MYSQL_TYPE_STRING;

      switch (buffer->buffer_type) {
      case MYSQL_TYPE_DOUBLE:
        buffer->buffer_length= sizeof(fbh->ddata);
        buffer->buffer= (char*) &fbh->ddata;
        break;

      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_LONGLONG:
        buffer->buffer_length= sizeof(fbh->ldata);
        buffer->buffer= (char*) &fbh->ldata;
        buffer->is_unsigned= (fields[i].flags & UNSIGNED_FLAG) ? 1 : 0;
        break;

      case MYSQL_TYPE_BIT:
        buffer->buffer_length= 8;
        Newz(908, fbh->data, buffer->buffer_length, char);
        buffer->buffer= (char *) fbh->data;
        break;

      default:
        buffer->buffer_length= fields[i].max_length ? fields[i].max_length : 1;
        Newz(908, fbh->data, buffer->buffer_length, char);
        buffer->buffer= (char *) fbh->data;
      }
    }

    if (mysql_stmt_bind_result(imp_sth->stmt, imp_sth->buffer))
    {
      do_error(sth, mysql_stmt_errno(imp_sth->stmt),
               mysql_stmt_error(imp_sth->stmt),
               mysql_stmt_sqlstate(imp_sth->stmt));
      return 0;
    }
  }
#endif

  imp_sth->done_desc= 1;
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- dbd_describe\n");
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
 *           DBIc_DBISTATE(imp_sth)->get_fbav(imp_sth), even the values
 *           of the array are prepared, we just need to modify them
 *           appropriately
 *
 **************************************************************************/

AV*
dbd_st_fetch(SV *sth, imp_sth_t* imp_sth)
{
  dTHX;
  int num_fields, ChopBlanks, i, rc;
  unsigned long *lengths;
  AV *av;
  int av_length, av_readonly;
  MYSQL_ROW cols;
  D_imp_dbh_from_sth;
  MYSQL* svsock= imp_dbh->pmysql;
  imp_sth_fbh_t *fbh;
  D_imp_xxh(sth);
#if MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION
  MYSQL_BIND *buffer;
#endif
  MYSQL_FIELD *fields;
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t-> dbd_st_fetch\n");

#if MYSQL_ASYNC
  if(imp_dbh->async_query_in_flight) {
      if(mysql_db_async_result(sth, &imp_sth->result) <= 0) {
        return Nullav;
      }
  }
#endif

#if MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION
  if (imp_sth->use_server_side_prepare)
  {
    if (!DBIc_ACTIVE(imp_sth) )
    {
      do_error(sth, JW_ERR_SEQUENCE, "no statement executing\n",NULL);
      return Nullav;
    }

    if (imp_sth->fetch_done)
    {
      do_error(sth, JW_ERR_SEQUENCE, "fetch() but fetch already done",NULL);
      return Nullav;
    }

    if (!imp_sth->done_desc)
    {
      if (!dbd_describe(sth, imp_sth))
      {
        do_error(sth, JW_ERR_SEQUENCE, "Error while describe result set.",
                 NULL);
        return Nullav;
      }
    }
  }
#endif

  ChopBlanks = DBIc_is(imp_sth, DBIcf_ChopBlanks);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "\t\tdbd_st_fetch for %p, chopblanks %d\n",
                  sth, ChopBlanks);

  if (!imp_sth->result)
  {
    do_error(sth, JW_ERR_SEQUENCE, "fetch() without execute()" ,NULL);
    return Nullav;
  }

  /* fix from 2.9008 */
  imp_dbh->pmysql->net.last_errno = 0;

#if MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION
  if (imp_sth->use_server_side_prepare)
  {
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tdbd_st_fetch calling mysql_fetch\n");

    if ((rc= mysql_stmt_fetch(imp_sth->stmt)))
    {
      if (rc == 1)
        do_error(sth, mysql_stmt_errno(imp_sth->stmt),
                 mysql_stmt_error(imp_sth->stmt),
                mysql_stmt_sqlstate(imp_sth->stmt));

#if MYSQL_VERSION_ID >= MYSQL_VERSION_5_0 
      if (rc == MYSQL_DATA_TRUNCATED) {
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tdbd_st_fetch data truncated\n");
        goto process;
      }
#endif

      if (rc == MYSQL_NO_DATA)
      {
        /* Update row_num to affected_rows value */
        imp_sth->row_num= mysql_stmt_affected_rows(imp_sth->stmt);
        imp_sth->fetch_done=1;
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tdbd_st_fetch no data\n");
      }

      dbd_st_finish(sth, imp_sth);

      return Nullav;
    }

process:
    imp_sth->currow++;

    av= DBIc_DBISTATE(imp_sth)->get_fbav(imp_sth);
    num_fields=mysql_stmt_field_count(imp_sth->stmt);
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                    "\t\tdbd_st_fetch called mysql_fetch, rc %d num_fields %d\n",
                    rc, num_fields);

    for (
         buffer= imp_sth->buffer,
         fbh= imp_sth->fbh,
         i= 0;
         i < num_fields;
         i++,
         fbh++,
         buffer++
        )
    {
      SV *sv= AvARRAY(av)[i]; /* Note: we (re)use the SV in the AV	*/
      STRLEN len;

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
        if (fbh->length > buffer->buffer_length || fbh->error)
        {
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
              "\t\tRefetch BLOB/TEXT column: %d, length: %lu, error: %d\n",
              i, fbh->length, fbh->error);

          Renew(fbh->data, fbh->length, char);
          buffer->buffer_length= fbh->length;
          buffer->buffer= (char *) fbh->data;
          imp_sth->stmt->bind[i].buffer_length = fbh->length;
          imp_sth->stmt->bind[i].buffer = (char *)fbh->data;

          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2) {
            int j;
            int m = MIN(*buffer->length, buffer->buffer_length);
            char *ptr = (char*)buffer->buffer;
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),"\t\tbefore buffer->buffer: ");
            for (j = 0; j < m; j++) {
              PerlIO_printf(DBIc_LOGPIO(imp_xxh), "%c", *ptr++);
            }
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),"\n");
          }

          /*TODO: Use offset instead of 0 to fetch only remain part of data*/
          if (mysql_stmt_fetch_column(imp_sth->stmt, buffer , i, 0))
            do_error(sth, mysql_stmt_errno(imp_sth->stmt),
                     mysql_stmt_error(imp_sth->stmt),
                     mysql_stmt_sqlstate(imp_sth->stmt));

          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2) {
            int j;
            int m = MIN(*buffer->length, buffer->buffer_length);
            char *ptr = (char*)buffer->buffer;
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),"\t\tafter buffer->buffer: ");
            for (j = 0; j < m; j++) {
              PerlIO_printf(DBIc_LOGPIO(imp_xxh), "%c", *ptr++);
            }
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),"\n");
          }
        }

        /* This does look a lot like Georg's PHP driver doesn't it?  --Brian */
        /* Credit due to Georg - mysqli_api.c  ;) --PMG */
        switch (buffer->buffer_type) {
        case MYSQL_TYPE_DOUBLE:
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tst_fetch double data %f\n", fbh->ddata);
          sv_setnv(sv, fbh->ddata);
          break;

        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tst_fetch int data %"IVdf", unsigned? %d\n",
                          fbh->ldata, buffer->is_unsigned);
          if (buffer->is_unsigned)
            sv_setuv(sv, fbh->ldata);
          else
            sv_setiv(sv, fbh->ldata);

          break;

        case MYSQL_TYPE_BIT:
          sv_setpvn(sv, fbh->data, fbh->length);

          break;

        default:
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tERROR IN st_fetch_string");
          len= fbh->length;
	  /* ChopBlanks server-side prepared statement */
          if (ChopBlanks)
          {
            /* 
              see bottom of:
              http://www.mysql.org/doc/refman/5.0/en/c-api-datatypes.html
            */
            if (fbh->charsetnr != 63)
              while (len && fbh->data[len-1] == ' ') { --len; }
          }
	  /* END OF ChopBlanks */

          sv_setpvn(sv, fbh->data, len);

#if MYSQL_VERSION_ID >= FIELD_CHARSETNR_VERSION 
  /* SHOW COLLATION WHERE Id = 63; -- 63 == charset binary, collation binary */
        if ((imp_dbh->enable_utf8 || imp_dbh->enable_utf8mb4) && fbh->charsetnr != 63)
#else
	if ((imp_dbh->enable_utf8 || imp_dbh->enable_utf8mb4) && !(fbh->flags & BINARY_FLAG))
#endif
	  sv_utf8_decode(sv);
          break;

        }

      }
    }

    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- dbd_st_fetch, %d cols\n", num_fields);

    return av;
  }
  else
  {
#endif

    imp_sth->currow++;

    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    {
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\tdbd_st_fetch result set details\n");
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\timp_sth->result=%p\n", imp_sth->result);
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\tmysql_num_fields=%u\n",
                    mysql_num_fields(imp_sth->result));
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\tmysql_num_rows=%llu\n",
                    mysql_num_rows(imp_sth->result));
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\tmysql_affected_rows=%llu\n",
                    mysql_affected_rows(imp_dbh->pmysql));
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\tdbd_st_fetch for %p, currow= %d\n",
                    sth,imp_sth->currow);
    }

    if (!(cols= mysql_fetch_row(imp_sth->result)))
    {
      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      {
        PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\tdbd_st_fetch, no more rows to fetch");
      }
      if (mysql_errno(imp_dbh->pmysql))
        do_error(sth, mysql_errno(imp_dbh->pmysql),
                 mysql_error(imp_dbh->pmysql),
                 mysql_sqlstate(imp_dbh->pmysql));


#if MYSQL_VERSION_ID >= MULTIPLE_RESULT_SET_VERSION
      if (!mysql_more_results(svsock))
#endif
        dbd_st_finish(sth, imp_sth);
      return Nullav;
    }

    num_fields= mysql_num_fields(imp_sth->result);
    fields= mysql_fetch_fields(imp_sth->result);
    lengths= mysql_fetch_lengths(imp_sth->result);

    if ((av= DBIc_FIELDS_AV(imp_sth)) != Nullav)
    {
      av_length= av_len(av)+1;

      if (av_length != num_fields)              /* Resize array if necessary */
      {
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- dbd_st_fetch, size of results array(%d) != num_fields(%d)\n",
                                   av_length, num_fields);

        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- dbd_st_fetch, result fields(%d)\n",
                                   DBIc_NUM_FIELDS(imp_sth));

        av_readonly = SvREADONLY(av);

        if (av_readonly)
          SvREADONLY_off( av );              /* DBI sets this readonly */

        while (av_length < num_fields)
        {
          av_store(av, av_length++, newSV(0));
        }

        while (av_length > num_fields)
        {
          SvREFCNT_dec(av_pop(av));
          av_length--;
        }
        if (av_readonly)
          SvREADONLY_on(av);
      }
    }

    av= DBIc_DBISTATE(imp_sth)->get_fbav(imp_sth);

    for (i= 0;  i < num_fields; ++i)
    {
      char *col= cols[i];
      SV *sv= AvARRAY(av)[i]; /* Note: we (re)use the SV in the AV	*/

      if (col)
      {
        STRLEN len= lengths[i];
        if (ChopBlanks)
        {
#if MYSQL_VERSION_ID >= FIELD_CHARSETNR_VERSION
          if (fields[i].charsetnr != 63)
#else
          if (!(fields[i].flags & BINARY_FLAG))
#endif
          while (len && col[len-1] == ' ')
          {	--len; }
        }

        /* Set string value returned from mysql server */
        sv_setpvn(sv, col, len);

        switch (mysql_to_perl_type(fields[i].type)) {
        case MYSQL_TYPE_DOUBLE:
          if (!(fields[i].flags & ZEROFILL_FLAG))
          {
            /* Coerce to dobule and set scalar as NV */
            (void) SvNV(sv);
            SvNOK_only(sv);
          }
          break;

        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
          if (!(fields[i].flags & ZEROFILL_FLAG))
          {
            /* Coerce to integer and set scalar as UV resp. IV */
            if (fields[i].flags & UNSIGNED_FLAG)
            {
              (void) SvUV(sv);
              SvIOK_only_UV(sv);
            }
            else
            {
              (void) SvIV(sv);
              SvIOK_only(sv);
            }
          }
          break;

#if MYSQL_VERSION_ID > NEW_DATATYPE_VERSION
        case MYSQL_TYPE_BIT:
          /* Let it as binary string */
          break;
#endif

        default:
          /* TEXT columns can be returned as MYSQL_TYPE_BLOB, so always check for charset */
          /* see bottom of: http://www.mysql.org/doc/refman/5.0/en/c-api-datatypes.html */
        if ((imp_dbh->enable_utf8 || imp_dbh->enable_utf8mb4) && fields[i].charsetnr != 63)
	  sv_utf8_decode(sv);
          break;
        }
      }
      else
        (void) SvOK_off(sv);  /*  Field is NULL, return undef  */
    }

    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- dbd_st_fetch, %d cols\n", num_fields);
    return av;

#if MYSQL_VERSION_ID  >= SERVER_PREPARE_VERSION
  }
#endif

}

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
/*
  We have to fetch all data from stmt
  There is may be useful for 2 cases:
  1. st_finish when we have undef statement
  2. call st_execute again when we have some unfetched data in stmt
 */

int mysql_st_clean_cursor(SV* sth, imp_sth_t* imp_sth) {

  if (DBIc_ACTIVE(imp_sth) && dbd_describe(sth, imp_sth) &&
      !imp_sth->fetch_done)
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
  dTHX;
  D_imp_xxh(sth);

#if defined (dTHR)
  dTHR;
#endif

#if MYSQL_ASYNC
  D_imp_dbh_from_sth;
  if(imp_dbh->async_query_in_flight) {
    mysql_db_async_result(sth, &imp_sth->result);
  }
#endif

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
  {
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\n--> dbd_st_finish\n");
  }

  if (imp_sth->use_server_side_prepare)
  {
    if (imp_sth && imp_sth->stmt)
    {
      if (!mysql_st_clean_cursor(sth, imp_sth))
      {
        do_error(sth, JW_ERR_SEQUENCE,
                 "Error happened while tried to clean up stmt",NULL);
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
  if (imp_sth && DBIc_ACTIVE(imp_sth))
  {
    /*
      Clean-up previous result set(s) for sth to prevent
      'Commands out of sync' error
    */
    mysql_st_free_result_sets(sth, imp_sth);
  }
  DBIc_ACTIVE_off(imp_sth);
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
  {
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\n<-- dbd_st_finish\n");
  }
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
  dTHX;
  D_imp_xxh(sth);

#if defined (dTHR)
  dTHR;
#endif

  int i;

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
  imp_sth_fbh_t *fbh;
  int n;

  n= DBIc_NUM_PARAMS(imp_sth);
  if (n)
  {
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\tFreeing %d parameters, bind %p fbind %p\n",
          n, imp_sth->bind, imp_sth->fbind);

    free_bind(imp_sth->bind);
    free_fbind(imp_sth->fbind);
  }

  fbh= imp_sth->fbh;
  if (fbh)
  {
    n = DBIc_NUM_FIELDS(imp_sth);
    i = 0;
    while (i < n)
    {
      if (fbh[i].data) Safefree(fbh[i].data);
      ++i;
    }

    free_fbuffer(fbh);
    if (imp_sth->buffer)
      free_bind(imp_sth->buffer);
  }

  if (imp_sth->stmt)
  {
    mysql_stmt_close(imp_sth->stmt);
    imp_sth->stmt= NULL;
  }
#endif


  /* dbd_st_finish has already been called by .xs code if needed.	*/

  /* Free values allocated by dbd_bind_ph */
  if (imp_sth->params)
  {
    free_param(aTHX_ imp_sth->params, DBIc_NUM_PARAMS(imp_sth));
    imp_sth->params= NULL;
  }

  /* Free cached array attributes */
  for (i= 0; i < AV_ATTRIB_LAST; i++)
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
 *  Returns: TRUE for success, FALSE otherwise; do_error will
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
  dTHX;
  STRLEN(kl);
  char *key= SvPV(keysv, kl);
  int retval= FALSE;
  D_imp_xxh(sth);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "\t\t-> dbd_st_STORE_attrib for %p, key %s\n",
                  sth, key);

  if (strEQ(key, "mysql_use_result"))
  {
    imp_sth->use_mysql_use_result= SvTRUE(valuesv);
  }

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "\t\t<- dbd_st_STORE_attrib for %p, result %d\n",
                  sth, retval);

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
  dTHX;
  D_imp_sth(sth);
  AV *av= Nullav;
  MYSQL_FIELD *curField;

  /* Are we asking for a legal value? */
  if (what < 0 ||  what >= AV_ATTRIB_LAST)
    do_error(sth, JW_ERR_NOT_IMPLEMENTED, "Not implemented", NULL);

  /* Return cached value, if possible */
  else if (cacheit  &&  imp_sth->av_attr[what])
    av= imp_sth->av_attr[what];

  /* Does this sth really have a result? */
  else if (!res)
    do_error(sth, JW_ERR_NOT_ACTIVE,
	     "statement contains no result" ,NULL);
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
        sv= newSVpvn(curField->name, strlen(curField->name));
        break;

      case AV_ATTRIB_TABLE:
        sv= newSVpvn(curField->table, strlen(curField->table));
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
        sv= &PL_sv_undef;
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
    return &PL_sv_undef;

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
  dTHX;
  STRLEN(kl);
  char *key= SvPV(keysv, kl);
  SV *retsv= Nullsv;
  D_imp_xxh(sth);

  if (kl < 2)
    return Nullsv;

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "    -> dbd_st_FETCH_attrib for %p, key %s\n",
                  sth, key);

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
            int n;
            char key[100];
            I32 keylen;
            for (n= 0; n < DBIc_NUM_PARAMS(imp_sth); n++)
            {
                keylen= sprintf(key, "%d", n);
                (void)hv_store(pvhv, key,
                         keylen, newSVsv(imp_sth->params[n].value), 0);
            }
        }
        retsv= sv_2mortal(newRV_noinc((SV*)pvhv));
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
        retsv= sv_2mortal(newSViv(PTR2IV(imp_sth->result)));
      break;
    case 13:
      if (strEQ(key, "mysql_is_blob"))
        retsv= ST_FETCH_AV(AV_ATTRIB_IS_BLOB);
      break;
    case 14:
      if (strEQ(key, "mysql_insertid"))
      {
        /* We cannot return an IV, because the insertid is a long.  */
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "INSERT ID %llu\n", imp_sth->insertid);

        return sv_2mortal(my_ulonglong2str(aTHX_ imp_sth->insertid));
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
    case 19:
      if (strEQ(key, "mysql_warning_count"))
        retsv= sv_2mortal(newSViv((IV) imp_sth->warning_count));
      break;
    case 20:
      if (strEQ(key, "mysql_server_prepare"))
#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
        retsv= sv_2mortal(newSViv((IV) imp_sth->use_server_side_prepare));
#else
        retsv= boolSV(0);
#endif
      break;
    case 23:
      if (strEQ(key, "mysql_is_auto_increment"))
        retsv = ST_FETCH_AV(AV_ATTRIB_IS_AUTO_INCREMENT);
      break;
    case 37:
      if (strEQ(key, "mysql_server_prepare_disable_fallback"))
#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
        retsv= sv_2mortal(newSViv((IV) imp_sth->disable_fallback_for_server_prepare));
#else
        retsv= boolSV(0);
#endif
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
 *  Returns: TRUE for success, FALSE otherwise; do_error will
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
    /* quell warnings */
    sth= sth;
    imp_sth=imp_sth;
    field= field;
    offset= offset;
    len= len;
    destrv= destrv;
    destoffset= destoffset;
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

int dbd_bind_ph(SV *sth, imp_sth_t *imp_sth, SV *param, SV *value,
		 IV sql_type, SV *attribs, int is_inout, IV maxlen) {
  dTHX;
  int rc;
  int param_num= SvIV(param);
  int idx= param_num - 1;
  char *err_msg;
  D_imp_xxh(sth);

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
  STRLEN slen;
  char *buffer= NULL;
  int buffer_is_null= 0;
  int buffer_is_unsigned= 0;
  int buffer_length= 0;
  unsigned int buffer_type= 0;
#endif

  D_imp_dbh_from_sth;
  ASYNC_CHECK_RETURN(sth, FALSE);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "   Called: dbd_bind_ph\n");

  attribs= attribs;
  maxlen= maxlen;

  if (param_num <= 0  ||  param_num > DBIc_NUM_PARAMS(imp_sth))
  {
    do_error(sth, JW_ERR_ILLEGAL_PARAM_NUM, "Illegal parameter number", NULL);
    return FALSE;
  }

  /*
     This fixes the bug whereby no warning was issued upon binding a
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
      err_msg = SvPVX(sv_2mortal(newSVpvf(
              "Binding non-numeric field %d, value %s as a numeric!",
              param_num, neatsvpv(value,0))));
      do_error(sth, JW_ERR_ILLEGAL_PARAM_NUM, err_msg, NULL);
    }
  }

  if (is_inout)
  {
    do_error(sth, JW_ERR_NOT_IMPLEMENTED, "Output parameters not implemented", NULL);
    return FALSE;
  }

  rc = bind_param(&imp_sth->params[idx], value, sql_type);

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
  if (imp_sth->use_server_side_prepare)
  {
      switch(sql_type) {
      case SQL_NUMERIC:
      case SQL_INTEGER:
      case SQL_SMALLINT:
      case SQL_TINYINT:
#if IVSIZE >= 8
      case SQL_BIGINT:
          buffer_type= MYSQL_TYPE_LONGLONG;
#else
          buffer_type= MYSQL_TYPE_LONG;
#endif
          break;
      case SQL_DOUBLE:
      case SQL_DECIMAL: 
      case SQL_FLOAT: 
      case SQL_REAL:
          buffer_type= MYSQL_TYPE_DOUBLE;
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
          buffer_type= MYSQL_TYPE_BLOB;
          break;
      default:
          buffer_type= MYSQL_TYPE_STRING;
    }
    buffer_is_null = !(SvOK(imp_sth->params[idx].value) && imp_sth->params[idx].value);
    if (! buffer_is_null) {
      switch(buffer_type) {
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
          /* INT */
          if (!SvIOK(imp_sth->params[idx].value) && DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tTRY TO BIND AN INT NUMBER\n");
          buffer_length = sizeof imp_sth->fbind[idx].numeric_val.lval;
          imp_sth->fbind[idx].numeric_val.lval= SvIV(imp_sth->params[idx].value);
          buffer=(void*)&(imp_sth->fbind[idx].numeric_val.lval);
          if (!SvIOK(imp_sth->params[idx].value))
          {
            if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
              PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                            "   Conversion to INT NUMBER was not successful -> '%s' --> (unsigned) '%"UVuf"' / (signed) '%"IVdf"' <- fallback to STRING\n",
                            SvPV_nolen(imp_sth->params[idx].value), imp_sth->fbind[idx].numeric_val.lval, imp_sth->fbind[idx].numeric_val.lval);
            buffer_type = MYSQL_TYPE_STRING;
            break;
          }
          if (SvIsUV(imp_sth->params[idx].value))
            buffer_is_unsigned= 1;
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "   SCALAR type %"IVdf" ->%"IVdf"<- IS AN INT NUMBER\n",
                          sql_type, *(IV *)buffer);
          break;

        case MYSQL_TYPE_DOUBLE:
          if (!SvNOK(imp_sth->params[idx].value) && DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tTRY TO BIND A FLOAT NUMBER\n");
          buffer_length = sizeof imp_sth->fbind[idx].numeric_val.dval;
          imp_sth->fbind[idx].numeric_val.dval= SvNV(imp_sth->params[idx].value);
          buffer=(char*)&(imp_sth->fbind[idx].numeric_val.dval);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "   SCALAR type %"IVdf" ->%f<- IS A FLOAT NUMBER\n",
                          sql_type, (double)(*buffer));
          break;

        case MYSQL_TYPE_BLOB:
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "   SCALAR type BLOB\n");
          break;

        case MYSQL_TYPE_STRING:
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "   SCALAR type STRING %"IVdf", buffertype=%d\n", sql_type, buffer_type);
          break;

        default:
          croak("Bug in DBD::Mysql file dbdimp.c#dbd_bind_ph: do not know how to handle unknown buffer type.");
      }

      if (buffer_type == MYSQL_TYPE_STRING || buffer_type == MYSQL_TYPE_BLOB)
      {
        buffer= SvPV(imp_sth->params[idx].value, slen);
        buffer_length= slen;
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                        " SCALAR type %"IVdf" ->length %d<- IS A STRING or BLOB\n",
                        sql_type, buffer_length);
      }
    }
    else
    {
      /*case: buffer_is_null != 0*/
      buffer= NULL;
      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
        PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                      "   SCALAR NULL VALUE: buffer type is: %d\n", buffer_type);
    }

    /* Type of column was changed. Force to rebind */
    if (imp_sth->bind[idx].buffer_type != buffer_type || imp_sth->bind[idx].is_unsigned != buffer_is_unsigned) {
      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                        "   FORCE REBIND: buffer type changed from %d to %d, sql-type=%"IVdf"\n",
                        (int) imp_sth->bind[idx].buffer_type, buffer_type, sql_type);
      imp_sth->has_been_bound = 0;
    }

    /* prepare has been called */
    if (imp_sth->has_been_bound)
    {
      imp_sth->stmt->params[idx].buffer= buffer;
      imp_sth->stmt->params[idx].buffer_length= buffer_length;
    }

    imp_sth->bind[idx].buffer_type= buffer_type;
    imp_sth->bind[idx].buffer= buffer;
    imp_sth->bind[idx].buffer_length= buffer_length;
    imp_sth->bind[idx].is_unsigned= buffer_is_unsigned;

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
  dTHX;
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

  if (mysql_errno(imp_dbh->pmysql) != CR_SERVER_GONE_ERROR &&
          mysql_errno(imp_dbh->pmysql) != CR_SERVER_LOST)
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
  save_socket= *(imp_dbh->pmysql);
  memcpy (&save_socket, imp_dbh->pmysql,sizeof(save_socket));
  memset (imp_dbh->pmysql,0,sizeof(*(imp_dbh->pmysql)));

  /* we should disconnect the db handle before reconnecting, this will
   * prevent my_login from thinking it's adopting an active child which
   * would prevent the handle from actually reconnecting
   */
  if (!dbd_db_disconnect(h, imp_dbh) || !my_login(aTHX_ h, imp_dbh))
  {
    do_error(h, mysql_errno(imp_dbh->pmysql), mysql_error(imp_dbh->pmysql),
             mysql_sqlstate(imp_dbh->pmysql));
    memcpy (imp_dbh->pmysql, &save_socket, sizeof(save_socket));
    ++imp_dbh->stats.auto_reconnects_failed;
    return FALSE;
  }

  /*
   *  Tell DBI, that dbh->disconnect should be called for this handle
   */
  DBIc_ACTIVE_on(imp_dbh);

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
        sv= &PL_sv_undef;                         \
    }                                           \
    av_push(row, sv);

#define IV_PUSH(i) sv= newSViv((i)); SvREADONLY_on(sv); av_push(row, sv);

AV *dbd_db_type_info_all(SV *dbh, imp_dbh_t *imp_dbh)
{
  dTHX;
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

  dbh= dbh;
  imp_dbh= imp_dbh;
 
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
      av_push(row, &PL_sv_undef);

    IV_PUSH(t->sql_datatype); /* SQL_DATATYPE*/
    IV_PUSH(t->sql_datetime_sub); /* SQL_DATETIME_SUB*/
    IV_PUSH(t->interval_precision); /* INTERVAL_PRECISION */
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
  dTHX;
  SV *result;

  if (SvGMAGICAL(str))
    mg_get(str);

  if (!SvOK(str))
    result= newSVpvn("NULL", 4);
  else
  {
    char *ptr, *sptr;
    STRLEN len;

    D_imp_dbh(dbh);

    if (type && SvMAGICAL(type))
      mg_get(type);

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
    if (SvUTF8(str)) SvUTF8_on(result);
    sptr= SvPVX(result);

    *sptr++ = '\'';
    sptr+= mysql_real_escape_string(imp_dbh->pmysql, sptr,
                                     ptr, len);
    *sptr++= '\'';
    SvPOK_on(result);
    SvCUR_set(result, sptr - SvPVX(result));
    /* Never hurts NUL terminating a Per string */
    *sptr++= '\0';
  }
  return result;
}

SV *mysql_db_last_insert_id(SV *dbh, imp_dbh_t *imp_dbh,
        SV *catalog, SV *schema, SV *table, SV *field, SV *attr)
{
  dTHX;
  /* all these non-op settings are to stifle OS X compile warnings */
  imp_dbh= imp_dbh;
  dbh= dbh;
  catalog= catalog;
  schema= schema;
  table= table;
  field= field;
  attr= attr;

  ASYNC_CHECK_RETURN(dbh, &PL_sv_undef);
  return sv_2mortal(my_ulonglong2str(aTHX_ mysql_insert_id(imp_dbh->pmysql)));
}

#if MYSQL_ASYNC
int mysql_db_async_result(SV* h, MYSQL_RES** resp)
{
  dTHX;
  D_imp_xxh(h);
  imp_dbh_t* dbh;
  MYSQL* svsock = NULL;
  MYSQL_RES* _res;
  int retval = 0;
  int htype;

  if(! resp) {
      resp = &_res;
  }
  htype = DBIc_TYPE(imp_xxh);


  if(htype == DBIt_DB) {
      D_imp_dbh(h);
      dbh = imp_dbh;
  } else {
      D_imp_sth(h);
      D_imp_dbh_from_sth;
      dbh = imp_dbh;
  }

  if(! dbh->async_query_in_flight) {
      do_error(h, 2000, "Gathering asynchronous results for a synchronous handle", "HY000");
      return -1;
  }
  if(dbh->async_query_in_flight != imp_xxh) {
      do_error(h, 2000, "Gathering async_query_in_flight results for the wrong handle", "HY000");
      return -1;
  }
  dbh->async_query_in_flight = NULL;

  svsock= dbh->pmysql;
  retval= mysql_read_query_result(svsock);
  if(! retval) {
    *resp= mysql_store_result(svsock);

    if (mysql_errno(svsock))
      do_error(h, mysql_errno(svsock), mysql_error(svsock), mysql_sqlstate(svsock));
    if (!*resp)
      retval= mysql_affected_rows(svsock);
    else {
      retval= mysql_num_rows(*resp);
      if(resp == &_res) {
        mysql_free_result(*resp);
        *resp= NULL;
      }
    }
    if(htype == DBIt_ST) {
      D_imp_sth(h);
      D_imp_dbh_from_sth;

      if((my_ulonglong)retval+1 != (my_ulonglong)-1) {
        if(! *resp) {
          imp_sth->insertid= mysql_insert_id(svsock);
#if MYSQL_VERSION_ID >= MULTIPLE_RESULT_SET_VERSION
          if(! mysql_more_results(svsock))
            DBIc_ACTIVE_off(imp_sth);
#endif
        } else {
          DBIc_NUM_FIELDS(imp_sth)= mysql_num_fields(imp_sth->result);
          imp_sth->done_desc= 0;
          imp_sth->fetch_done= 0;
        }
      }
      imp_sth->warning_count = mysql_warning_count(imp_dbh->pmysql);
    }
  } else {
     do_error(h, mysql_errno(svsock), mysql_error(svsock),
              mysql_sqlstate(svsock));
     return -1;
  }
 return retval;
}

int mysql_db_async_ready(SV* h)
{
  dTHX;
  D_imp_xxh(h);
  imp_dbh_t* dbh;
  int htype;

  htype = DBIc_TYPE(imp_xxh);
  
  if(htype == DBIt_DB) {
      D_imp_dbh(h);
      dbh = imp_dbh;
  } else {
      D_imp_sth(h);
      D_imp_dbh_from_sth;
      dbh = imp_dbh;
  }

  if(dbh->async_query_in_flight) {
      if(dbh->async_query_in_flight == imp_xxh && dbh->pmysql->net.fd != -1) {
          struct pollfd fds;
          int retval;

          fds.fd = dbh->pmysql->net.fd;
          fds.events = POLLIN;

          retval = poll(&fds, 1, 0);

          if(retval < 0) {
              do_error(h, errno, strerror(errno), "HY000");
          }
          return retval;
      } else {
          do_error(h, 2000, "Calling mysql_async_ready on the wrong handle", "HY000");
          return -1;
      }
  } else {
      do_error(h, 2000, "Handle is not in asynchronous mode", "HY000");
      return -1;
  }
}
#endif

static int parse_number(char *string, STRLEN len, char **end)
{
    int seen_neg;
    int seen_dec;
    int seen_e;
    int seen_plus;
    int seen_digit;
    char *cp;

    seen_neg= seen_dec= seen_e= seen_plus= seen_digit= 0;

    if (len <= 0) {
        len= strlen(string);
    }

    cp= string;

    /* Skip leading whitespace */
    while (*cp && isspace(*cp))
      cp++;

    for ( ; *cp; cp++)
    {
      if ('-' == *cp)
      {
        if (seen_neg >= 2)
        {
          /*
            third '-'. number can contains two '-'.
            because -1e-10 is valid number */
          break;
        }
        seen_neg += 1;
      }
      else if ('.' == *cp)
      {
        if (seen_dec)
        {
          /* second '.' */
          break;
        }
        seen_dec= 1;
      }
      else if ('e' == *cp)
      {
        if (seen_e)
        {
          /* second 'e' */
          break;
        }
        seen_e= 1;
      }
      else if ('+' == *cp)
      {
        if (seen_plus)
        {
          /* second '+' */
          break;
        }
        seen_plus= 1;
      }
      else if (!isdigit(*cp))
      {
        /* Not sure why this was changed */
        /* seen_digit= 1; */
        break;
      }
    }

    *end= cp;

    /* length 0 -> not a number */
    /* Need to revisit this */
    /*if (len == 0 || cp - string < (int) len || seen_digit == 0) {*/
    if (len == 0 || cp - string < (int) len) {
        return -1;
    }

    return 0;
}
