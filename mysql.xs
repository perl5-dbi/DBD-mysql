/* Hej, Emacs, this is -*- C -*- mode!

   Copyright (c) 2003      Rudolf Lippan
   Copyright (c) 1997-2003 Jochen Wiedmann

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/


#include "dbdimp.h"
#include "constants.h"

#include <errno.h>
#include <string.h>

#define ASYNC_CHECK_XS(h)\
    if(imp_dbh->async_query_in_flight) {\
        do_error(h, 2000, "Calling a synchronous function on an asynchronous handle", "HY000");\
        XSRETURN_UNDEF;\
    }


DBISTATE_DECLARE;


MODULE = DBD::mysql	PACKAGE = DBD::mysql

INCLUDE: mysql.xsi

MODULE = DBD::mysql	PACKAGE = DBD::mysql

double
constant(name, arg)
    char* name
    char* arg
  CODE:
    RETVAL = mysql_constant(name, arg);
  OUTPUT:
    RETVAL

SV*
client_version(pkg)
  CODE:
    RETVAL = my_ulonglong2sv(aTHX_ mysql_get_client_version());
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
    mysql.net.fd = -1;
    MYSQL* sock = mysql_dr_connect(drh, &mysql, NULL, host, port, user, password,
				   NULL, NULL);
    if (sock != NULL)
    {
      MYSQL_ROW cur;
      MYSQL_RES* res = mysql_list_dbs(sock, NULL);
      if (!res)
      {
        do_error(drh, mysql_errno(sock), mysql_error(sock), mysql_sqlstate(sock));
      }
      else
      {
	EXTEND(sp, mysql_num_rows(res));
	while ((cur = mysql_fetch_row(res)))
        {
	  PUSHs(sv_2mortal((SV*)newSVpvn(cur[0], strlen(cur[0]))));
	}
	mysql_free_result(res);
      }
      mysql_close(sock);
    }
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
  ASYNC_CHECK_XS(dbh);
  ST(0) = sv_2mortal(newRV_noinc((SV*) dbd_db_type_info_all(dbh,
                                                            imp_dbh)));
  XSRETURN(1);
}


void
_ListDBs(dbh)
  SV*	dbh
  PPCODE:
  MYSQL_RES* res;
  MYSQL_ROW cur;

  D_imp_dbh(dbh);

  ASYNC_CHECK_XS(dbh);

  res = mysql_list_dbs(imp_dbh->pmysql, NULL);
  if (!res  &&
      (!mysql_db_reconnect(dbh)  ||
       !(res = mysql_list_dbs(imp_dbh->pmysql, NULL))))
{
  do_error(dbh, mysql_errno(imp_dbh->pmysql),
           mysql_error(imp_dbh->pmysql), mysql_sqlstate(imp_dbh->pmysql));
}
else
{
  EXTEND(sp, mysql_num_rows(res));
  while ((cur = mysql_fetch_row(res)))
  {
    PUSHs(sv_2mortal((SV*)newSVpvn(cur[0], strlen(cur[0]))));
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
  int num_params= 0;
  int retval;
  struct imp_sth_ph_st* params= NULL;
  MYSQL_RES* result= NULL;
  SV* async = NULL;
  int next_result_rc;
  STRLEN slen;
  char            *str_ptr, *buffer;
  int             has_binded;
  int             buffer_length;
  int             buffer_type= 0;
  int             use_server_side_prepare= 0;
  int             disable_fallback_for_server_prepare= 0;
  MYSQL_STMT      *stmt= NULL;
  MYSQL_BIND      *bind= NULL;
    ASYNC_CHECK_XS(dbh);
    if ((!DBIc_has(imp_dbh, DBIcf_ACTIVE)) &&
        (!mysql_db_reconnect(dbh)))
      XSRETURN_UNDEF;
    while (mysql_next_result(imp_dbh->pmysql)==0)
    {
      MYSQL_RES* res = mysql_use_result(imp_dbh->pmysql);
      if (res)
        mysql_free_result(res);
      }

  /*
   * Globally enabled using of server side prepared statement
   * for dbh->do() statements. It is possible to force driver
   * to use server side prepared statement mechanism by adding
   * 'mysql_server_prepare' attribute to do() method localy:
   * $dbh->do($stmt, {mysql_server_prepared=>1});
  */

  use_server_side_prepare = imp_dbh->use_server_side_prepare;
  if (attr)
  {
    SV** svp;
    DBD_ATTRIBS_CHECK("do", dbh, attr);
    svp = DBD_ATTRIB_GET_SVP(attr, "mysql_server_prepare", 20);

    use_server_side_prepare = (svp) ?
      SvTRUE(*svp) : imp_dbh->use_server_side_prepare;

    svp = DBD_ATTRIB_GET_SVP(attr, "mysql_server_prepare_disable_fallback", 37);
    disable_fallback_for_server_prepare = (svp) ?
      SvTRUE(*svp) : imp_dbh->disable_fallback_for_server_prepare;

    svp   = DBD_ATTRIB_GET_SVP(attr, "async", 5);
    async = (svp) ? *svp : &PL_sv_no;
  }
  if (DBIc_DBISTATE(imp_dbh)->debug >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_dbh),
                  "mysql.xs do() use_server_side_prepare %d, async %d\n",
                  use_server_side_prepare, SvTRUE(async));

  (void)hv_store((HV*)SvRV(dbh), "Statement", 9, SvREFCNT_inc(statement), 0);

  if(SvTRUE(async)) {
    if (disable_fallback_for_server_prepare)
    {
      do_error(dbh, ER_UNSUPPORTED_PS,
               "Async option not supported with server side prepare", "HY000");
      XSRETURN_UNDEF;
    }
    use_server_side_prepare = FALSE; /* for now */
    imp_dbh->async_query_in_flight = imp_dbh;
  }

  if (use_server_side_prepare)
  {
    str_ptr= SvPV(statement, slen);

    stmt= mysql_stmt_init(imp_dbh->pmysql);

    if ((mysql_stmt_prepare(stmt, str_ptr, strlen(str_ptr)))  &&
        (!mysql_db_reconnect(dbh) ||
         (mysql_stmt_prepare(stmt, str_ptr, strlen(str_ptr)))))
    {
      /*
        For commands that are not supported by server side prepared
        statement mechanism lets try to pass them through regular API
      */
      if (!disable_fallback_for_server_prepare && mysql_stmt_errno(stmt) == ER_UNSUPPORTED_PS)
      {
        use_server_side_prepare= 0;
      }
      else
      {
        do_error(dbh, mysql_stmt_errno(stmt), mysql_stmt_error(stmt)
                 ,mysql_stmt_sqlstate(stmt));
        retval=-2;
      }
      mysql_stmt_close(stmt);
      stmt= NULL;
    }
    else
    {
      /*
        'items' is the number of arguments passed to XSUB, supplied
        by xsubpp compiler, as listed in manpage for perlxs
      */
      if (items > 3)
      {
        /*
          Handle binding supplied values to placeholders assume user has
          passed the correct number of parameters
        */
        int i;
        num_params= items - 3;
        Newz(0, bind, (unsigned int) num_params, MYSQL_BIND);

        for (i = 0; i < num_params; i++)
        {
          int defined= 0;
          SV *param= ST(i+3);

          if (param)
          {
            if (SvMAGICAL(param))
              mg_get(param);
            if (SvOK(param))
              defined= 1;
          }
          if (defined)
          {
            buffer= SvPV(param, slen);
            buffer_length= slen;
            buffer_type= MYSQL_TYPE_STRING;
          }
          else
          {
            buffer= NULL;
            buffer_length= 0;
            buffer_type= MYSQL_TYPE_NULL;
          }

          bind[i].buffer_type = buffer_type;
          bind[i].buffer_length= buffer_length;
          bind[i].buffer= buffer;
        }
        has_binded= 0;
      }
      retval = mysql_st_internal_execute41(dbh,
                                           num_params,
                                           &result,
                                           stmt,
                                           bind,
                                           &has_binded);

      if (bind)
        Safefree(bind);

      /*
         as of 5.7, the end of package was deprecated, breaking
         insertfetch with prepared statements enabled
         no need for this as do() doesn't have a result set since
         it's for DML statements only
      */

      if (retval == -2) /* -2 means error */
      {
        SV *err = DBIc_ERR(imp_dbh);
        if (!disable_fallback_for_server_prepare && SvIV(err) == ER_UNSUPPORTED_PS)
        {
          use_server_side_prepare = 0;
        }
      }
    }
  }

  if (! use_server_side_prepare)
  {
    if (items > 3)
    {
      /*  Handle binding supplied values to placeholders	   */
      /*  Assume user has passed the correct number of parameters  */
      int i;
      num_params= items-3;
      Newz(0, params, sizeof(*params)*num_params, struct imp_sth_ph_st);
      for (i= 0;  i < num_params;  i++)
      {
        params[i].value= ST(i+3);
        params[i].type= SQL_VARCHAR;
      }
    }
    retval = mysql_st_internal_execute(dbh, statement, attr, num_params,
                                       params, &result, imp_dbh->pmysql, 0);
  }
  if (params)
    Safefree(params);

  if (result)
  {
    mysql_free_result(result);
    result= 0;
  }
  if (retval != -2 && !SvTRUE(async)) /* -2 means error */
    {
      /* more results? -1 = no, >0 = error, 0 = yes (keep looping) */
      while ((next_result_rc= mysql_next_result(imp_dbh->pmysql)) == 0)
      {
        result = mysql_use_result(imp_dbh->pmysql);
          if (result)
            mysql_free_result(result);
            result = NULL;
          }
          if (next_result_rc > 0)
          {
            if (DBIc_DBISTATE(imp_dbh)->debug >= 2)
              PerlIO_printf(DBIc_LOGPIO(imp_dbh),
                            "\t<- do() ERROR: %s\n",
                            mysql_error(imp_dbh->pmysql));

              do_error(dbh, mysql_errno(imp_dbh->pmysql),
                       mysql_error(imp_dbh->pmysql),
                       mysql_sqlstate(imp_dbh->pmysql));
              retval= -2;
          }
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
/* MySQL 5.7 below 5.7.18 is affected by Bug #78778.
 * MySQL 5.7.18 and higher (including 8.0.3) is affected by Bug #89139.
 *
 * Once Bug #89139 is fixed we can adjust the upper bound of this check.
 *
 * https://bugs.mysql.com/bug.php?id=78778
 * https://bugs.mysql.com/bug.php?id=89139 */
      unsigned long long insertid;

      D_imp_dbh(dbh);
      ASYNC_CHECK_XS(dbh);
      insertid = mysql_insert_id(imp_dbh->pmysql);
      retval = (mysql_ping(imp_dbh->pmysql) == 0);
      if (!retval) {
	if (mysql_db_reconnect(dbh)) {
	  retval = (mysql_ping(imp_dbh->pmysql) == 0);
	}
      }
      imp_dbh->pmysql->insert_id = insertid;
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
        SV* quoted;

        D_imp_dbh(dbh);
        ASYNC_CHECK_XS(dbh);

        quoted = dbd_db_quote(dbh, str, type);
	ST(0) = quoted ? sv_2mortal(quoted) : str;
	XSRETURN(1);
    }

void mysql_fd(dbh)
    SV* dbh
  PPCODE:
    {
        D_imp_dbh(dbh);
        if(imp_dbh->pmysql->net.fd != -1) {
            XSRETURN_IV(imp_dbh->pmysql->net.fd);
        } else {
            XSRETURN_UNDEF;
        }
    }

void mysql_async_result(dbh)
    SV* dbh
  PPCODE:
    {
        int retval;

        retval = mysql_db_async_result(dbh, NULL);

        if(retval > 0) {
            XSRETURN_IV(retval);
        } else if(retval == 0) {
            XSRETURN_PV("0E0");
        } else {
            XSRETURN_UNDEF;
        }
    }

void mysql_async_ready(dbh)
    SV* dbh
  PPCODE:
    {
        int retval;

        retval = mysql_db_async_ready(dbh);
        if(retval > 0) {
            XSRETURN_YES;
        } else if(retval == 0) {
            XSRETURN_NO;
        } else {
            XSRETURN_UNDEF;
        }
    }

void _async_check(dbh)
    SV* dbh
  PPCODE:
    {
        D_imp_dbh(dbh);
        ASYNC_CHECK_XS(dbh);
        XSRETURN_YES;
    }

MODULE = DBD::mysql    PACKAGE = DBD::mysql::st

int
more_results(sth)
    SV *	sth
    CODE:
{
  D_imp_sth(sth);
  if (dbd_st_more_results(sth, imp_sth))
  {
    RETVAL=1;
  }
  else
  {
    RETVAL=0;
  }
}
    OUTPUT:
      RETVAL

int
dataseek(sth, pos)
    SV* sth
    int pos
  PROTOTYPE: $$
  CODE:
{
  D_imp_sth(sth);
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
        do_error(sth, JW_ERR_NOT_ACTIVE, "Statement not active" ,NULL);
      }
    }
    else
    {
      RETVAL = 0;
      do_error(sth, JW_ERR_NOT_ACTIVE, "No result set" ,NULL);
    }
  }
  else
  {
  if (imp_sth->result) {
    mysql_data_seek(imp_sth->result, pos);
    RETVAL = 1;
  } else {
    RETVAL = 0;
    do_error(sth, JW_ERR_NOT_ACTIVE, "Statement not active" ,NULL);
  }
  }
}
  OUTPUT:
    RETVAL

void
rows(sth)
    SV* sth
  CODE:
    D_imp_sth(sth);
    char buf[64];
    D_imp_dbh_from_sth;
    if(imp_dbh->async_query_in_flight) {
        if(mysql_db_async_result(sth, &imp_sth->result) < 0) {
            XSRETURN_UNDEF;
        }
    }

  /* fix to make rows able to handle errors and handle max value from 
     affected rows.
     if mysql_affected_row returns an error, it's value is 18446744073709551614,
     while a (my_ulonglong)-1 is  18446744073709551615, so we have to add 1 to
     imp_sth->row_num to know if there's an error
  */
  if (imp_sth->row_num+1 ==  (my_ulonglong) -1)
    sprintf(buf, "%d", -1);
  else
    sprintf(buf, "%lu", imp_sth->row_num);

  ST(0) = sv_2mortal(newSVpvn(buf, strlen(buf)));

int mysql_async_result(sth)
    SV* sth
  CODE:
    {
        D_imp_sth(sth);
        int retval;

        retval= mysql_db_async_result(sth, &imp_sth->result);

        if(retval > 0) {
            imp_sth->row_num = retval;
            XSRETURN_IV(retval);
        } else if(retval == 0) {
            imp_sth->row_num = retval;
            XSRETURN_PV("0E0");
        } else {
            XSRETURN_UNDEF;
        }
    }
  OUTPUT:
    RETVAL

void mysql_async_ready(sth)
    SV* sth
  PPCODE:
    {
        int retval;

        retval = mysql_db_async_ready(sth);
        if(retval > 0) {
            XSRETURN_YES;
        } else if(retval == 0) {
            XSRETURN_NO;
        } else {
            XSRETURN_UNDEF;
        }
    }

void _async_check(sth)
    SV* sth
  PPCODE:
    {
        D_imp_sth(sth);
        D_imp_dbh_from_sth;
        ASYNC_CHECK_XS(sth);
        XSRETURN_YES;
    }


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
#define SQL_ASYNC_MODE 10021
#define SQL_MAX_ASYNC_CONCURRENT_STATEMENTS 10022

#define SQL_AM_NONE       0
#define SQL_AM_CONNECTION 1
#define SQL_AM_STATEMENT  2


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

    if (SvMAGICAL(sql_info_type))
        mg_get(sql_info_type);

    if (SvOK(sql_info_type))
    	type = SvIV(sql_info_type);
    else
    	croak("get_info called with an invalied parameter");
    
    switch(type) {
    	case SQL_CATALOG_NAME_SEPARATOR:
	    /* (dbc->flag & FLAG_NO_CATALOG) ? WTF is in flag ? */
	    retsv = newSVpvn(".",1);
	    break;
	case SQL_CATALOG_TERM:
	    /* (dbc->flag & FLAG_NO_CATALOG) ? WTF is in flag ? */
	    retsv = newSVpvn("database",8);
	    break;
	case SQL_DBMS_VER:
	    retsv = newSVpvn(
	        imp_dbh->pmysql->server_version,
		strlen(imp_dbh->pmysql->server_version)
	    );
	    break;
	case SQL_IDENTIFIER_QUOTE_CHAR:
	    retsv = newSVpvn("`", 1);
	    break;
	case SQL_MAXIMUM_STATEMENT_LENGTH:
	    ; /* avoid "a label can only be part of a statement and a declaration is not a statement" */
	    unsigned long buffer_len;
	    mysql_get_option(NULL, MYSQL_OPT_NET_BUFFER_LENGTH, &buffer_len);
	    retsv = newSViv(buffer_len);
	    break;
	case SQL_MAXIMUM_TABLES_IN_SELECT:
	    /* newSViv((sizeof(int) > 32) ? sizeof(int)-1 : 31 ); in general? */
	    retsv= newSViv((sizeof(int) == 64 ) ? 63 : 31 );
	    break;
	case SQL_MAX_TABLE_NAME_LEN:
	    retsv= newSViv(NAME_LEN);
	    break;
	case SQL_SERVER_NAME:
	    retsv= newSVpvn(imp_dbh->pmysql->host_info,strlen(imp_dbh->pmysql->host_info));
	    break;
        case SQL_ASYNC_MODE:
            retsv = newSViv(SQL_AM_STATEMENT);
            break;
        case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS:
            retsv = newSViv(1);
            break;
    	default:
 		croak("Unknown SQL Info type: %i", mysql_errno(imp_dbh->pmysql));
    }
    ST(0) = sv_2mortal(retsv);
