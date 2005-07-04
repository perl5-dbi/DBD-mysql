/* Hej, Emacs, this is -*- C -*- mode!

   $Id$

   Copyright (c) 2003      Rudolf Lippan
   Copyright (c) 1997-2003 Jochen Wiedmann

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/


#include "dbdimp.h"
#include "constants.h"


DBISTATE_DECLARE;


MODULE = DBD::mysql	PACKAGE = DBD::mysql

INCLUDE: mysql.xsi

MODULE = DBD::mysql	PACKAGE = DBD::mysql

double
constant(name, arg)
    char* name
    char* arg
  CODE:
    RETVAL = mymsql_constant(name, arg);
  OUTPUT:
    RETVAL


MODULE = DBD::mysql	PACKAGE = DBD::mysql::dr

void
_ListDBs(drh, host=NULL, port=NULL, user=NULL, password=NULL)
    SV *        drh
    char *	host
    char *      port
    char *      user
    char *      password
  PPCODE:
{
    MYSQL mysql;
    MYSQL* sock = mysql_dr_connect(drh, &mysql, NULL, host, port, user, password,
				   NULL, NULL);
    if (sock != NULL)
    {
      MYSQL_ROW cur;
      MYSQL_RES* res = mysql_list_dbs(sock, NULL);
      if (!res)
      {
	do_error(drh, mysql_errno(sock), mysql_error(sock));
      }
      else
      {
	EXTEND(sp, mysql_num_rows(res));
	while ((cur = mysql_fetch_row(res)))
        {
	  PUSHs(sv_2mortal((SV*)newSVpv(cur[0], strlen(cur[0]))));
	}
	mysql_free_result(res);
      }
      mysql_close(sock);
    }
}


void
_admin_internal(drh,dbh,command,dbname=NULL,host=NULL,port=NULL,user=NULL,password=NULL)
  SV* drh
  SV* dbh
  char* command
  char* dbname
  char* host
  char* port
  char* user
  char* password
  PPCODE:
{
  MYSQL mysql;
  int retval;
  MYSQL* sock;

  /*
   *  Connect to the database, if required.
 */
  if (SvOK(dbh)) {
    D_imp_dbh(dbh);
    sock = &imp_dbh->mysql;
  }
  else
  {
    sock = mysql_dr_connect(drh, &mysql, NULL, host, port, user,  password, NULL, NULL);
    if (sock == NULL)
    {
      do_error(drh, mysql_errno(&mysql), mysql_error(&mysql));
      XSRETURN_NO;
    }
  }

  if (strEQ(command, "shutdown"))
#if MYSQL_VERSION_ID < 40103
    retval = mysql_shutdown(sock);
#else
    retval = mysql_shutdown(sock, SHUTDOWN_DEFAULT);
#endif
  else if (strEQ(command, "reload"))
    retval = mysql_reload(sock);
  else if (strEQ(command, "createdb"))
  {
#if MYSQL_VERSION_ID < 40000
    retval = mysql_create_db(sock, dbname);
#else
    char* buffer = malloc(strlen(dbname)+50);
    if (buffer == NULL)
    {
      do_error(drh, JW_ERR_MEM, "Out of memory");
      XSRETURN_NO;
    }
    else
    {
      strcpy(buffer, "CREATE DATABASE ");
      strcat(buffer, dbname);
      retval = mysql_real_query(sock, buffer, strlen(buffer));
      free(buffer);
    }
#endif
  }
  else if (strEQ(command, "dropdb"))
  {
#if MYSQL_VERSION_ID < 40000
    retval = mysql_drop_db(sock, dbname);
#else
    char* buffer = malloc(strlen(dbname)+50);
    if (buffer == NULL)
    {
      do_error(drh, JW_ERR_MEM, "Out of memory");
      XSRETURN_NO;
    }
    else
    {
      strcpy(buffer, "DROP DATABASE ");
      strcat(buffer, dbname);
      retval = mysql_real_query(sock, buffer, strlen(buffer));
      free(buffer);
    }
#endif
  }
  else
  {
    croak("Unknown command: %s", command);
  }
  if (retval)
  {
    do_error(SvOK(dbh) ? dbh : drh, mysql_errno(sock),
             mysql_error(sock));
  }

  if (SvOK(dbh))
  {
    mysql_close(sock);
  }
  if (retval)
    XSRETURN_NO;
  else 
    XSRETURN_YES;
}


MODULE = DBD::mysql    PACKAGE = DBD::mysql::db


void
type_info_all(dbh)
  SV* dbh
  PPCODE:
{
  /* 	static AV* types = NULL; */
  /* 	if (!types) { */
  /* 	    D_imp_dbh(dbh); */
  /* 	    if (!(types = dbd_db_type_info_all(dbh, imp_dbh))) { */
  /* 	        croak("Cannot create types array (out of memory?)"); */
  /* 	    } */
  /* 	} */
  /* 	ST(0) = sv_2mortal(newRV_inc((SV*) types)); */
  D_imp_dbh(dbh);
  ST(0) = sv_2mortal(newRV_noinc((SV*) dbd_db_type_info_all(dbh,
                                                            imp_dbh)));
  XSRETURN(1);
}


void
_ListDBs(dbh)
  SV*	dbh
  PPCODE:
  D_imp_dbh(dbh);
  MYSQL_RES* res = mysql_list_dbs(&imp_dbh->mysql, NULL);
  MYSQL_ROW cur;
  if (!res  &&
      (!mysql_db_reconnect(dbh)  ||
       !(res = mysql_list_dbs(&imp_dbh->mysql, NULL))))
{
  do_error(dbh, mysql_errno(&imp_dbh->mysql),
           mysql_error(&imp_dbh->mysql));
}
else
{
  EXTEND(sp, mysql_num_rows(res));
  while ((cur = mysql_fetch_row(res)))
  {
    PUSHs(sv_2mortal((SV*)newSVpv(cur[0], strlen(cur[0]))));
  }
  mysql_free_result(res);
}


void
do(dbh, statement, attr=Nullsv, ...)
  SV *        dbh
  SV *	statement
  SV *        attr
  PROTOTYPE: $$;$@      
  CODE:
{
  D_imp_dbh(dbh);
  struct imp_sth_ph_st* params = NULL;
  int numParams = 0;
  MYSQL_RES* result = NULL;
  int retval;
#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION 
  STRLEN slen;
  MYSQL_STMT      *stmt = NULL;
  MYSQL_BIND      *bind = NULL;
  imp_sth_phb_t   *fbind = NULL;
  int             has_binded;
  char            *str;
  char            *buffer;
  int             col_type = MYSQL_TYPE_STRING;
  int             buffer_is_null = 0;
  int             buffer_length = slen;
  int             buffer_type = 0;
  int             param_type = SQL_VARCHAR;
  int             use_server_side_prepare= 0;
/* Globaly disabled using of server side prepared statement 
   for dbh->do() statements. It is possible to force driver 
   to use server side prepared statement mechanism by adding 
   'mysql_server_prepare' attribute to do() method localy:

   $dbh->do($stmt, {mysql_server_prepare=>1});
*/
#ifdef DBD_ENABLE_GLOBAL_PREPARE_FOR_DO
  use_server_side_prepare = imp_dbh->use_server_side_prepare;
#endif
  if (attr)
  {
    SV **svp;
    DBD_ATTRIBS_CHECK("do", dbh, attr);
    svp = DBD_ATTRIB_GET_SVP(attr, "mysql_server_prepare", 20);

    if (svp)
    {
      use_server_side_prepare = SvTRUE(*svp);
    }
  }

  if (use_server_side_prepare) 
  {
    str = SvPV(statement, slen);

    stmt = mysql_stmt_init(&imp_dbh->mysql);

    if (! mysql_stmt_prepare(stmt, str , strlen(str)))
    {
      /* 
        * 'items' is the number of arguments passed to XSUB, supplied by xsubpp
        * compiler, as listed in manpage for perlxs
      */
      if (items > 3) 
      {
        /*  Handle binding supplied values to placeholders	   */
        /*  Assume user has passed the correct number of parameters  */
        int i;
        numParams = items - 3;
        /*numParams = mysql_stmt_param_count(stmt);*/
        Newz(0, params, sizeof(*params)*numParams, struct imp_sth_ph_st);
        Newz(0, bind, numParams, MYSQL_BIND);
        Newz(0, fbind, numParams, imp_sth_phb_t);

        for (i = 0; i < numParams; i++)
        {
          params[i].value = ST(i+3);

          if ((SvOK(params[i].value) && params[i].value))
          {
            buffer = SvPV(params[i].value, slen);
            buffer_is_null = 0;
            buffer_length = slen;
          }
          else
          {
            buffer = NULL;
            buffer_is_null = 1;
            buffer_length = 0;
          }

          /* if this statement has a result set, field types will be correctly identified. If there 
           * is no result set, such as with an INSERT, fields will not be defined, and all buffer_type
           * will default to MYSQL_TYPE_VAR_STRING */
          col_type = (stmt->fields) ? stmt->fields[i].type : MYSQL_TYPE_STRING;

          switch (col_type) {
          case MYSQL_TYPE_DECIMAL:
            param_type = SQL_DECIMAL;
            buffer_type = MYSQL_TYPE_DOUBLE;
            break;

          case MYSQL_TYPE_DOUBLE:
            param_type = SQL_DOUBLE;
            buffer_type = MYSQL_TYPE_DOUBLE;
            break;

          case MYSQL_TYPE_FLOAT:
            buffer_type = MYSQL_TYPE_DOUBLE;
            param_type = SQL_FLOAT;
            break;

          case MYSQL_TYPE_SHORT:
            buffer_type = MYSQL_TYPE_DOUBLE;
            param_type = SQL_FLOAT;
            break;

          case MYSQL_TYPE_TINY:
            buffer_type = MYSQL_TYPE_DOUBLE;
            param_type = SQL_FLOAT;
            break;

          case MYSQL_TYPE_LONG:
            buffer_type = MYSQL_TYPE_LONG;
            param_type = SQL_BIGINT;
            break;

          case MYSQL_TYPE_INT24:
          case MYSQL_TYPE_YEAR:
            buffer_type = MYSQL_TYPE_LONG;
            param_type = SQL_INTEGER; 
            break;

          case MYSQL_TYPE_LONGLONG:
            /* perl handles long long as double
             * so we'll set this to string */
            buffer_type= MYSQL_TYPE_STRING;
            param_type = SQL_VARCHAR;
            break;

          case MYSQL_TYPE_NEWDATE:
          case MYSQL_TYPE_DATE:
            buffer_type= MYSQL_TYPE_STRING;
            param_type = SQL_DATE;
            break;

          case MYSQL_TYPE_TIME:
            buffer_type= MYSQL_TYPE_STRING;
            param_type = SQL_TIME;
            break;

          case MYSQL_TYPE_TIMESTAMP:
            buffer_type= MYSQL_TYPE_STRING;
            param_type = SQL_TIMESTAMP;
            break;

          case MYSQL_TYPE_VAR_STRING:
          case MYSQL_TYPE_STRING:
          case MYSQL_TYPE_DATETIME:
            buffer_type= MYSQL_TYPE_STRING;
            param_type = SQL_VARCHAR;
            break;

          case MYSQL_TYPE_BLOB:
            buffer_type= MYSQL_TYPE_STRING;
            param_type = SQL_BINARY;
            break;

          default:
            buffer_type= MYSQL_TYPE_STRING;
            param_type = SQL_VARCHAR;
            break;
          }

          bind[i].buffer_type = buffer_type; 
          bind[i].buffer_length= buffer_length; 
          bind[i].buffer = buffer; 
          fbind[i].length = buffer_length;
          fbind[i].is_null= buffer_is_null;
          params[i].type = param_type;
        }
        has_binded=0;
      }
      retval = mysql_st_internal_execute41(dbh, statement, attr,
                                           numParams,
                                           params,
                                           &result,
                                           &imp_dbh->mysql,
                                           0,
                                           stmt,
                                           bind,
                                           &has_binded);
      if (bind)
      {
        Safefree(bind);
      }
      if (fbind)
      {
        Safefree(fbind);
      }
      if(mysql_stmt_close(stmt))
      {
        fprintf(stderr, "\n failed while closing the statement");
        fprintf(stderr, "\n %s", mysql_stmt_error(stmt));
      }
    }
    else
    {
      fprintf(stderr,"DO: Something wrong while try to prepare query %s\n", mysql_error(&imp_dbh->mysql));
      retval=-2;
      mysql_stmt_close(stmt);
      stmt = NULL;
    }
  }
  else
  {
#endif
    if (items > 3) {
      /*  Handle binding supplied values to placeholders	   */
      /*  Assume user has passed the correct number of parameters  */
      int i;
      numParams = items-3;
      Newz(0, params, sizeof(*params)*numParams, struct imp_sth_ph_st);
      for (i = 0;  i < numParams;  i++)
      {
        params[i].value = ST(i+3);
        params[i].type = SQL_VARCHAR;
      }
    }
    retval = mysql_st_internal_execute(dbh, statement, attr, numParams,
                                       params, &result, &imp_dbh->mysql, 0);
#if MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION 
  }
#endif
  if (params)
  {
    Safefree(params);
  }

  if (result) {
    mysql_free_result(result);
  }
  /* remember that dbd_st_execute must return <= -2 for error	*/
  if (retval == 0)		/* ok with no rows affected	*/
    XST_mPV(0, "0E0");	/* (true but zero)		*/
  else if (retval < -1)	/* -1 == unknown number of rows	*/
    XST_mUNDEF(0);		/* <= -2 means error   		*/
  else
    XST_mIV(0, retval);	/* typically 1, rowcount or -1	*/
}


SV*
ping(dbh)
    SV* dbh;
  PROTOTYPE: $
  CODE:
    {
      int retval;
      D_imp_dbh(dbh);
      retval = (mysql_ping(&imp_dbh->mysql) == 0);
      if (!retval) {
	if (mysql_db_reconnect(dbh)) {
	  retval = (mysql_ping(&imp_dbh->mysql) == 0);
	}
      }
      RETVAL = boolSV(retval);
    }
  OUTPUT:
    RETVAL



void
quote(dbh, str, type=NULL)
    SV* dbh
    SV* str
    SV* type
  PROTOTYPE: $$;$
  PPCODE:
    {
        SV* quoted = dbd_db_quote(dbh, str, type);
	ST(0) = quoted ? sv_2mortal(quoted) : str;
	XSRETURN(1);
    }


MODULE = DBD::mysql    PACKAGE = DBD::mysql::st

int
dataseek(sth, pos)
    SV* sth
    int pos
  PROTOTYPE: $$
  CODE:
{
  D_imp_sth(sth);
#if (MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION) 
  if (imp_sth->use_server_side_prepare)
  {
    if (imp_sth->use_mysql_use_result || 1)
    {
      if (imp_sth->result && imp_sth->stmt) 
      {
        mysql_stmt_data_seek(imp_sth->stmt, pos);
        imp_sth->fetch_done=0;
        RETVAL = 1;
      } 
      else 
      {
        RETVAL = 0;
        do_error(sth, JW_ERR_NOT_ACTIVE, "Statement not active");
      }
    }
    else
    {
      RETVAL = 0;
      do_error(sth, JW_ERR_NOT_ACTIVE, "No result set");
    }
  }
  else 
  {
#endif
  if (imp_sth->result) {
    mysql_data_seek(imp_sth->result, pos);
    RETVAL = 1;
  } else {
    RETVAL = 0;
    do_error(sth, JW_ERR_NOT_ACTIVE, "Statement not active");
  }
#if (MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION) 
  }
#endif
}
  OUTPUT:
    RETVAL

void
rows(sth)
    SV* sth
  CODE:
    D_imp_sth(sth);
    char buf[64];
  /* fix to make rows able to handle errors and handle max value from 
     affected rows.
     if mysql_affected_row returns an error, it's value is 18446744073709551614,
     while a (my_ulonglong)-1 is  18446744073709551615, so we have to add 1 to
     imp_sth->row_num to know if there's an error
  */
  if (imp_sth->row_num+1 ==  (my_ulonglong) -1)
    sprintf(buf, "%d", -1);
  else
    sprintf(buf, "%llu", imp_sth->row_num);

  ST(0) = sv_2mortal(newSVpvn(buf, strlen(buf)));



MODULE = DBD::mysql    PACKAGE = DBD::mysql::GetInfo

# This probably should be grabed out of some ODBC types header file
#define SQL_CATALOG_NAME_SEPARATOR 41
#define SQL_CATALOG_TERM 42
#define SQL_DBMS_VER 18
#define SQL_IDENTIFIER_QUOTE_CHAR 29
#define SQL_MAXIMUM_STATEMENT_LENGTH 105
#define SQL_MAXIMUM_TABLES_IN_SELECT 106
#define SQL_MAX_TABLE_NAME_LEN 35
#define SQL_SERVER_NAME 13


#  dbd_mysql_getinfo()
#  Return ODBC get_info() information that must needs be accessed from C
#  This is an undocumented function that should only
#  be used by DBD::mysql::GetInfo.

void
dbd_mysql_get_info(dbh, sql_info_type)
    SV* dbh
    SV* sql_info_type
  CODE:
    D_imp_dbh(dbh);
    IV type = 0;
    SV* retsv=NULL;
    bool using_322=0;


    if (SvOK(sql_info_type))
    	type = SvIV(sql_info_type);
    else
    	croak("get_info called with an invalied parameter");
    
    switch(type) {
    	case SQL_CATALOG_NAME_SEPARATOR:
	    /* (dbc->flag & FLAG_NO_CATALOG) ? WTF is in flag ? */
	    retsv = newSVpv(".",1);
	    break;
	case SQL_CATALOG_TERM:
	    /* (dbc->flag & FLAG_NO_CATALOG) ? WTF is in flag ? */
	    retsv = newSVpv("database",8);
	    break;
	case SQL_DBMS_VER:
	    retsv = newSVpv(
	        imp_dbh->mysql.server_version,
		strlen(imp_dbh->mysql.server_version)
	    );
	    break;
	case SQL_IDENTIFIER_QUOTE_CHAR:
	    /*XXX What about a DB started in ANSI mode? */
	    /* Swiped from MyODBC's get_info.c */
	    using_322=is_prefix(mysql_get_server_info(&imp_dbh->mysql),"3.22");
	    retsv = newSVpv(!using_322 ? "`" : " ", 1);
	    break;
	case SQL_MAXIMUM_STATEMENT_LENGTH:
	    retsv = newSViv(net_buffer_length);
	    break;
	case SQL_MAXIMUM_TABLES_IN_SELECT:
	    /* newSViv((sizeof(int) > 32) ? sizeof(int)-1 : 31 ); in general? */
	    newSViv((sizeof(int) == 64 ) ? 63 : 31 );
	    break;
	case SQL_MAX_TABLE_NAME_LEN:
	    newSViv(NAME_LEN);
	    break;
	case SQL_SERVER_NAME:
	    newSVpv(imp_dbh->mysql.host_info,strlen(imp_dbh->mysql.host_info));
	    break;
    	default:
    		croak("Unknown SQL Info type: %i",dbh);
    }
    ST(0) = sv_2mortal(retsv);

