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
_ListDBs(drh, host, port=NULL, user=NULL, password=NULL)
    SV *        drh
    char *	host
    char *      port
    char *      user
    char *      password
  PPCODE:
{
    MYSQL mysql;
    MYSQL* sock = mysql_dr_connect(&mysql, NULL, host, port, user, password,
				   NULL, NULL);
    if (sock != NULL) {
      MYSQL_ROW cur;
      MYSQL_RES* res = mysql_list_dbs(sock, NULL);
      if (!res) {
	do_error(drh, mysql_errno(sock), mysql_error(sock));
      } else {
	EXTEND(sp, mysql_num_rows(res));
	while ((cur = mysql_fetch_row(res))) {
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
	int result;
	MYSQL* sock;

	/*
	 *  Connect to the database, if required.
	 */
	if (SvOK(dbh)) {
	    D_imp_dbh(dbh);
	    sock = &imp_dbh->mysql;
	} else {
	  sock = mysql_dr_connect(&mysql, NULL, host, port, user,
				  password, NULL, NULL);
	  if (sock == NULL) {
	    do_error(drh, mysql_errno(&mysql), mysql_error(&mysql));
	    XSRETURN_NO;
	  }
       }
 
       if (strEQ(command, "shutdown")) {
#if MYSQL_VERSION_ID < 40103
	   result = mysql_shutdown(sock);
#else
	   result = mysql_shutdown(sock, SHUTDOWN_DEFAULT);
#endif
       } else if (strEQ(command, "reload")) {
	   result = mysql_reload(sock);
       } else if (strEQ(command, "createdb")) {
#if MYSQL_VERSION_ID < 40000
	   result = mysql_create_db(sock, dbname);
#else
	   char* buffer = malloc(strlen(dbname)+50);
	   if (buffer == NULL) {
	     do_error(drh, JW_ERR_MEM, "Out of memory");
	     XSRETURN_NO;
	   } else {
	     strcpy(buffer, "CREATE DATABASE ");
	     strcat(buffer, dbname);
	     result = mysql_real_query(sock, buffer, strlen(buffer));
	     free(buffer);
	   }
#endif
       } else if (strEQ(command, "dropdb")) {
#if MYSQL_VERSION_ID < 40000
          result = mysql_drop_db(sock, dbname);
#else
	   char* buffer = malloc(strlen(dbname)+50);
	   if (buffer == NULL) {
	     do_error(drh, JW_ERR_MEM, "Out of memory");
	     XSRETURN_NO;
	   } else {
	     strcpy(buffer, "DROP DATABASE ");
	     strcat(buffer, dbname);
	     result = mysql_real_query(sock, buffer, strlen(buffer));
	     free(buffer);
	   }
#endif
       } else {
	  croak("Unknown command: %s", command);
       }
       if (result) {
	 do_error(SvOK(dbh) ? dbh : drh, mysql_errno(sock),
		  mysql_error(sock));
       }
       if (SvOK(dbh)) {
	   mysql_close(sock);
       }
       if (result) { XSRETURN_NO; } else { XSRETURN_YES; }
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
	 !(res = mysql_list_dbs(&imp_dbh->mysql, NULL)))) {
      do_error(dbh, mysql_errno(&imp_dbh->mysql),
	       mysql_error(&imp_dbh->mysql));
    } else {
      EXTEND(sp, mysql_num_rows(res));
      while ((cur = mysql_fetch_row(res))) {
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
    MYSQL_RES* cda = NULL;
    int retval;

    if (items > 3) {
      /*  Handle binding supplied values to placeholders	     */
      /*  Assume user has passed the correct number of parameters  */
      int i;
      numParams = items-3;
      Newz(0, params, sizeof(*params)*numParams, struct imp_sth_ph_st);
      for (i = 0;  i < numParams;  i++) {
	params[i].value = ST(i+3);
	params[i].type = SQL_VARCHAR;
      }
    }
    retval = mysql_st_internal_execute(dbh, statement, attr, numParams,
				       params, &cda, &imp_dbh->mysql, 0);
    Safefree(params);
    if (cda) {
      mysql_free_result(cda);
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
      int result;
      D_imp_dbh(dbh);
      result = (mysql_ping(&imp_dbh->mysql) == 0);
      if (!result) {
	if (mysql_db_reconnect(dbh)) {
	  result = (mysql_ping(&imp_dbh->mysql) == 0);
	}
      }
      RETVAL = boolSV(result);
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
  if (imp_sth->cda) {
    mysql_data_seek(imp_sth->cda, pos);
    RETVAL = 1;
  } else {
    RETVAL = 0;
    do_error(sth, JW_ERR_NOT_ACTIVE, "Statement not active");
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

  /* fix to make rows able to handle errors and handle max value from 
     affected rows.
     if mysql_affected_row returns an error, it's value is 18446744073709551614,
     while a (my_ulonglong)-1 is  18446744073709551615, so we have to add 1 to
     imp_sth->row_num to know if there's an error
  */
  if (imp_sth->row_num >=  (my_ulonglong) -2)
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





