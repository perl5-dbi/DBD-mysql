/*
 *  DBD::mysql - DBI driver for the mysql database
 *
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
#include "type_info.h"

#if defined(WIN32)  &&  defined(WORD)
    /*  Don't exactly know who's responsible for defining WORD ... :-(  */
#undef WORD
typedef short WORD;
#endif



DBISTATE_DECLARE;


static int CountParam(char *statement)
{
	char *ptr = statement;
	int numParam = 0;
	char c;

	while ((c = *ptr++)) {
		switch (c) {
		case '"':
		case '\'':
			/* Skip string */
			{
				char end_token = c;
				while ((c = *ptr) && c != end_token) {
					if (c == '\\') {
						++ptr;
						if (*ptr) {
							++ptr;
						}
					} else {
						++ptr;
					}
				}
				if (c) {
					++ptr;
				}
				break;
			}

		case '?':
			++numParam;
			break;

		default:
			break;
		}
	}
	return numParam;
}

static imp_sth_ph_t *AllocParam(int numParam)
{
	imp_sth_ph_t *params;

	if (numParam) {
		Newz(908, params, numParam, imp_sth_ph_t);
	} else {
		params = NULL;
	}
	return params;
}

#if MYSQL_VERSION_ID >=40101

static MYSQL_BIND *AllocBind(int numParam)
{
	MYSQL_BIND *bind;

	if (numParam) {
		Newz(908, bind, numParam, MYSQL_BIND);
	} else {
		bind = NULL;
	}
	return bind;
}

static imp_sth_phb_t *AllocFBind(int numParam)
{
	imp_sth_phb_t *fbind;

	if (numParam) {
		Newz(908, fbind, numParam, imp_sth_phb_t);
	} else {
		fbind = NULL;
	}
	return fbind;
}

static MYSQL_BIND *AllocBuffer(int numField)
{
	MYSQL_BIND *buffer;

	if (numField) {
		Newz(908, buffer, numField, MYSQL_BIND);
	} else {
		buffer = NULL;
	}
	return buffer;
}


static imp_sth_fbh_t *AllocFBuffer(int numField)
{
	imp_sth_fbh_t *fbh;

	if (numField) {
		Newz(908, fbh, numField, imp_sth_fbh_t);
	} else {
		fbh = NULL;
	}
	return fbh;
}

static void FreeBind(MYSQL_BIND * bind)
{
	if (bind) {
		Safefree(bind);
	} else {
		fprintf(stderr, "FREE ERROR BIND!");
	}
}

static void FreeBuffer(MYSQL_BIND * buffer)
{
	if (buffer) {
		Safefree(buffer);
	} else {
		fprintf(stderr, "FREE ERROR BUFFER!");
	}
}

static void FreeFBind(imp_sth_phb_t * fbind)
{
	if (fbind) {
		Safefree(fbind);
	} else {
		fprintf(stderr, "FREE ERROR  FBIND!");
	}
}

static void FreeFBuffer(imp_sth_fbh_t * fbh)
{
	if (fbh) {
		Safefree(fbh);
	} else {
		fprintf(stderr, "FREE ERROR FBUFFER!");
	}
}

#endif

static void FreeParam(imp_sth_ph_t * params, int numParam)
{
	if (params) {
		int i;
		for (i = 0; i < numParam; i++) {
			imp_sth_ph_t *ph = params + i;
			if (ph->value) {
				(void) SvREFCNT_dec(ph->value);
				ph->value = NULL;
			}
		}

		Safefree(params);
	}
}


static char *ParseParam(MYSQL * sock,
			char *statement,
			STRLEN * slenPtr,
			imp_sth_ph_t * params, int numParams)
{

	if (numParams == 0) {
		return NULL;
	}

	char *salloc;
	int i, j;
	char *valbuf;
	STRLEN vallen;
	int alen;
	char *ptr;
	imp_sth_ph_t *ph;
	int slen = *slenPtr;
	int limit_flag = 0;

	while (isspace(*statement)) {
		++statement;
		--slen;
	}


	/* Calculate the number of bytes being allocated for the statement */
	alen = slen;

	for (i = 0, ph = params; i < numParams; i++, ph++) {
		if (!ph->value || !SvOK(ph->value)) {
			alen += 3;	/* Erase '?', insert 'NULL' */
		} else {
			//fprintf(stdout, "ph->type %d\n", ph->type);
			/* this will most likely not happen since line 214 */
			/* of mysql.xs hardcodes all types to SQL_VARCHAR */
			if (!ph->type) {
				ph->type = SQL_VARCHAR;
			}

			// fprintf(stdout, "alen %d\n", alen);

			valbuf = SvPV(ph->value, vallen);
			/* shouldn't this be 2 + vallen + 1,not 2 * ? */
			/* 2 is for the each quotes char... right? ''' + vallen + ''' + '\0' */
			alen += 2 * vallen + 1;	/* Erase '?', insert (possibly quoted) * string.  */
		}
	}

	/* Allocate memory */
	New(908, salloc, alen + 1, char);
	ptr = salloc;

	/* Now create the statement string; compare CountParam above */
	i = 0;
	j = 0;

	while (j < slen) {
		/* LIMIT should be the last part of the query, in most cases */
		if (!limit_flag) {
			char *limitcheckptr = &statement[j];
			if (!strncasecmp(limitcheckptr, "limit", 5))
				limit_flag = 1;
		}

		switch (statement[j]) {
		case '\'':
		case '"':
			/* Skip string */
			{
				char endToken = statement[j++];
				*ptr++ = endToken;
				while (j < slen
				       && statement[j] != endToken) {
					if (statement[j] == '\\') {
						*ptr++ = statement[j++];
						if (j < slen) {
							*ptr++ =
							    statement[j++];
						}
					} else {
						*ptr++ = statement[j++];
					}
				}

				if (j < slen) {
					*ptr++ = statement[j++];
				}
			}
			break;

		case '?':
			/* Insert parameter */
			j++;
			if (i >= numParams) {
				break;
			}

			ph = params + i++;
			if (!ph->value || !SvOK(ph->value)) {
				*ptr++ = 'N';
				*ptr++ = 'U';
				*ptr++ = 'L';
				*ptr++ = 'L';
			} else {
				int isNum = FALSE;
				int c;

				valbuf = SvPV(ph->value, vallen);
				if (valbuf) {
					switch (ph->type) {
					case SQL_NUMERIC:
					case SQL_DECIMAL:
					case SQL_INTEGER:
					case SQL_SMALLINT:
					case SQL_FLOAT:
					case SQL_REAL:
					case SQL_DOUBLE:
					case SQL_BIGINT:
					case SQL_TINYINT:
						isNum = TRUE;
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
						isNum = FALSE;
						break;

					default:
						isNum = FALSE;
						break;
					}	/* end of switch(ph->type) */

					/* we're at the end of the query, so any placeholders if */
					/* after a LIMIT clause will be numbers and should not be quoted */
					if (limit_flag == 1) {
						isNum = TRUE;
					}

				//	fprintf(stdout, "isNum %d\n",
				//		isNum);
					if (!isNum) {
						*ptr++ = '\'';
						ptr +=
						    mysql_real_escape_string
						    (sock, ptr, valbuf,
						     vallen);
						*ptr++ = '\'';

					} else {
						while (vallen--) {
							switch ((c =
								 *valbuf++))
							{
							case '\0':
								*ptr++ =
								    '\\';
								*ptr++ =
								    '0';
								break;

							case '\'':
							case '\\':
								*ptr++ =
								    '\\';
								/* No break! */

							default:
								*ptr++ = c;
								break;
							}
						}
					}
				}	/* end of if valbuf */
			}	/* end of if ph->value */

			break;	/* end of case '?' */

			// in case this is a nested LIMIT
		case ')':
			if (limit_flag) {
				limit_flag = 0;
			}
			*ptr++ = statement[j++];
			break;

		default:
			*ptr++ = statement[j++];
			break;

		}		/* end of switch(statement[j]) */

	}			/* end of while (j < slen) */

	*slenPtr = ptr - salloc;
	*ptr++ = '\0';

	return salloc;
}

int BindParam(imp_sth_ph_t * ph, SV * value, IV sql_type)
{
	if (ph->value) {
		(void) SvREFCNT_dec(ph->value);
	}

	ph->value = newSVsv(value);

	if (sql_type) {
		ph->type = sql_type;
	}
	return TRUE;
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

void dbd_init(dbistate_t * dbistate)
{
	DBIS = dbistate;
}


/***************************************************************************
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

void do_error(SV * h, int rc, const char *what)
{
	D_imp_xxh(h);
	STRLEN lna;

	SV *errstr = DBIc_ERRSTR(imp_xxh);
	sv_setiv(DBIc_ERR(imp_xxh), (IV) rc);	/* set err early        */
	sv_setpv(errstr, what);
	DBIh_EVENT2(h, ERROR_event, DBIc_ERR(imp_xxh), errstr);
	if (dbis->debug >= 2)
		PerlIO_printf(DBILOGFP, "%s error %d recorded: %s\n",
			      what, rc, SvPV(errstr, lna));
}

void do_warn(SV * h, int rc, char *what)
{
	D_imp_xxh(h);
	STRLEN lna;

	SV *errstr = DBIc_ERRSTR(imp_xxh);
	sv_setiv(DBIc_ERR(imp_xxh), (IV) rc);	/* set err early        */
	sv_setpv(errstr, what);
	DBIh_EVENT2(h, WARN_event, DBIc_ERR(imp_xxh), errstr);
	if (dbis->debug >= 2)
		PerlIO_printf(DBILOGFP, "%s warning %d recorded: %s\n",
			      what, rc, SvPV(errstr, lna));
	warn("%s", what);
}

#define doquietwarn(s) \
  { \
    SV* sv = perl_get_sv("DBD::mysql::QUIET", FALSE); \
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
 *           char* unixSocket - Name of a UNIX socket being used
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

MYSQL *mysql_dr_connect(MYSQL * sock, char *unixSocket, char *host,
			char *port, char *user, char *password,
			char *dbname, imp_dbh_t * imp_dbh)
{
	int portNr;
	MYSQL *result;
#ifdef MYSQL_NO_CLIENT_FOUND_ROWS
		unsigned int client_flag = 0;
#else
		unsigned int client_flag = CLIENT_FOUND_ROWS;
#endif

	if (host && !*host)
		host = NULL;
	if (port && *port) {
		portNr = atoi(port);
	} else {
		portNr = 0;
	}
	if (user && !*user)
		user = NULL;
	if (password && !*password)
		password = NULL;

	if (dbis->debug >= 2)
		PerlIO_printf(DBILOGFP,
			      "imp_dbh->mysql_dr_connect: host = %s, port = %d,"
			      " uid = %s, pwd = %s\n",
			      host ? host : "NULL", portNr,
			      user ? user : "NULL",
			      password ? password : "NULL");

	mysql_init(sock);

	if (imp_dbh) {
		SV *sv = DBIc_IMP_DATA(imp_dbh);
		imp_dbh->has_transactions = TRUE;
		imp_dbh->auto_reconnect = FALSE;	/* Safer we flip this to TRUE perl side 
							   if we detect a mod_perl env. */

		DBIc_set(imp_dbh, DBIcf_AutoCommit, &sv_yes);
		if (sv && SvROK(sv)) {
			HV *hv = (HV *) SvRV(sv);
			SV **svp;
			STRLEN lna;

			if ((svp =
			     hv_fetch(hv, "mysql_compression", 17,
				      FALSE)) && *svp
			    && SvTRUE(*svp)) {
				if (dbis->debug >= 2)
					PerlIO_printf(DBILOGFP,
						      "imp_dbh->mysql_dr_connect: Enabling"
						      " compression.\n");
				mysql_options(sock,
					      MYSQL_OPT_COMPRESS,
					      NULL);
			}
			if ((svp =
			     hv_fetch(hv, "mysql_connect_timeout",
				      21, FALSE))
			    && *svp && SvTRUE(*svp)) {
				int to = SvIV(*svp);
				if (dbis->debug >= 2)
					PerlIO_printf(DBILOGFP,
						      "imp_dbh->mysql_dr_connect: Setting"
						      " connect timeout (%d).\n",
						      to);
				mysql_options(sock,
					      MYSQL_OPT_CONNECT_TIMEOUT,
					      (const char *) &to);
			}
			if ((svp =
			     hv_fetch(hv,
				      "mysql_read_default_file",
				      23, FALSE)) && *svp
			    && SvTRUE(*svp)) {
				char *df = SvPV(*svp, lna);
				if (dbis->debug >= 2)
					PerlIO_printf(DBILOGFP,
						      "imp_dbh->mysql_dr_connect: Reading"
						      " default file %s.\n",
						      df);
				mysql_options(sock,
					      MYSQL_READ_DEFAULT_FILE,
					      df);
			}
			if ((svp =
			     hv_fetch(hv,
				      "mysql_read_default_group",
				      24, FALSE)) && *svp
			    && SvTRUE(*svp)) {
				char *gr = SvPV(*svp, lna);
				if (dbis->debug >= 2)
					PerlIO_printf(DBILOGFP,
						      "imp_dbh->mysql_dr_connect: Using"
						      " default group %s.\n",
						      gr);
				mysql_options(sock,
					      MYSQL_READ_DEFAULT_GROUP,
					      gr);
			} else {
				mysql_options(sock,
					      MYSQL_READ_DEFAULT_GROUP,
					      "dbd_mysql");
			}
			if ((svp =
			     hv_fetch(hv,
				      "mysql_client_found_rows",
				      23, FALSE)) && *svp) {
				if (SvTRUE(*svp)) {
					client_flag |=
					    CLIENT_FOUND_ROWS;
				} else {
					client_flag &=
					    ~CLIENT_FOUND_ROWS;
				}
			}
#if MYSQL_VERSION_ID >=40101

			if ((svp =
			     hv_fetch(hv, "mysql_server_prepare",
				      20, FALSE)) && *svp) {
				if (SvTRUE(*svp)) {
					client_flag |=
					    CLIENT_PROTOCOL_41;
					imp_dbh->has_protocol41 =
					    TRUE;
				} else {
					client_flag &=
					    ~CLIENT_PROTOCOL_41;
					imp_dbh->has_protocol41 =
					    FALSE;
				}
				if (dbis->debug >= 2)
					PerlIO_printf(DBILOGFP,
						      "imp_dbh->has_protocol41: %d",
						      imp_dbh->
						      has_protocol41);
			}
#endif
#if defined(DBD_MYSQL_WITH_SSL)   && \
    (defined(CLIENT_SSL) || (MYSQL_VERSION_ID >= 40000))
			if ((svp =
			     hv_fetch(hv, "mysql_ssl", 9, FALSE))
			    && *svp) {
				if (SvTRUE(*svp)) {
					char *client_key = NULL;
					char *client_cert = NULL;
					char *ca_file = NULL;
					char *ca_path = NULL;
					char *cipher = NULL;
					STRLEN lna;
					if ((svp =
					     hv_fetch(hv,
						      "mysql_ssl_client_key",
						      20, FALSE))
					    && *svp) {
						client_key =
						    SvPV(*svp,
							 lna);
					}
					if ((svp =
					     hv_fetch(hv,
						      "mysql_ssl_client_cert",
						      21, FALSE))
					    && *svp) {
						client_cert =
						    SvPV(*svp,
							 lna);
					}
					if ((svp =
					     hv_fetch(hv,
						      "mysql_ssl_ca_file",
						      17, FALSE))
					    && *svp) {
						ca_file =
						    SvPV(*svp,
							 lna);
					}
					if ((svp =
					     hv_fetch(hv,
						      "mysql_ssl_ca_path",
						      17, FALSE))
					    && *svp) {
						ca_path =
						    SvPV(*svp,
							 lna);
					}
					if ((svp =
					     hv_fetch(hv,
						      "mysql_ssl_cipher",
						      16, FALSE))
					    && *svp) {
						cipher =
						    SvPV(*svp,
							 lna);
					}
					mysql_ssl_set(sock,
						      client_key,
						      client_cert,
						      ca_file,
						      ca_path,
						      cipher);
					client_flag |= CLIENT_SSL;
				}
			}
#endif
#if (MYSQL_VERSION_ID >= 32349)
				/*
				 * MySQL 3.23.49 disables LOAD DATA LOCAL by default. Use
				 * mysql_local_infile=1 in the DSN to enable it.
				 */
			if ((svp =
			     hv_fetch(hv, "mysql_local_infile", 18,
				      FALSE)) && *svp) {
				unsigned int flag = SvTRUE(*svp);
				if (dbis->debug >= 2)
					PerlIO_printf(DBILOGFP,
						      "imp_dbh->mysql_dr_connect: Using"
						      " local infile %u.\n",
						      flag);
				mysql_options(sock,
					      MYSQL_OPT_LOCAL_INFILE,
					      (const char *)
					      &flag);
			}
#endif
		}
	}
	if (dbis->debug >= 2)
		PerlIO_printf(DBILOGFP,
			      "imp_dbh->mysql_dr_connect: client_flags = %d\n",
			      client_flag);
	result =
	    mysql_real_connect(sock, host, user, password, dbname,
			       portNr, unixSocket, client_flag);
	if (dbis->debug >= 2)
		PerlIO_printf(DBILOGFP,
			      "imp_dbh->mysql_dr_connect: <-");

	/* we turn off Mysql's auto reconnect and handle re-connecting ourselves
	 * so that we can keep track of when this happens.
	 */
	sock->reconnect = 0;
	return result;
}

/***************************************************************************
 *
 * Frontend for mysql_dr_connect
 */
static int _MyLogin(imp_dbh_t * imp_dbh)
{
	SV *sv;
	SV **svp;
	HV *hv;
	char *dbname;
	char *host;
	char *port;
	char *user;
	char *password;
	char *unixSocket = NULL;
	STRLEN len, lna;

	sv = DBIc_IMP_DATA(imp_dbh);
	if (!sv || !SvROK(sv)) {
		return FALSE;
	}
	hv = (HV *) SvRV(sv);
	if (SvTYPE(hv) != SVt_PVHV) {
		return FALSE;
	}
	if ((svp = hv_fetch(hv, "host", 4, FALSE))) {
		host = SvPV(*svp, len);
		if (!len) {
			host = NULL;
		}
	} else {
		host = NULL;
	}
	if ((svp = hv_fetch(hv, "port", 4, FALSE))) {
		port = SvPV(*svp, lna);
	} else {
		port = NULL;
	}
	if ((svp = hv_fetch(hv, "user", 4, FALSE))) {
		user = SvPV(*svp, len);
		if (!len) {
			user = NULL;
		}
	} else {
		user = NULL;
	}
	if ((svp = hv_fetch(hv, "password", 8, FALSE))) {
		password = SvPV(*svp, len);
		if (!len) {
			password = NULL;
		}
	} else {
		password = NULL;
	}
	if ((svp = hv_fetch(hv, "database", 8, FALSE))) {
		dbname = SvPV(*svp, lna);
	} else {
		dbname = NULL;
	}
	if ((svp = hv_fetch(hv, "mysql_socket", 12, FALSE)) &&
	    *svp && SvTRUE(*svp)) {
		unixSocket = SvPV(*svp, lna);
	}

	if (dbis->debug >= 2)
		PerlIO_printf(DBILOGFP,
			      "imp_dbh->MyLogin: dbname = %s, uid = %s, pwd = %s,"
			      "host = %s, port = %s\n",
			      dbname ? dbname : "NULL",
			      user ? user : "NULL",
			      password ? password : "NULL",
			      host ? host : "NULL", port ? port : "NULL");

	return mysql_dr_connect(&imp_dbh->mysql, unixSocket, host, port,
				user, password, dbname,
				imp_dbh) ? TRUE : FALSE;
}


/***************************************************************************
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

int dbd_db_login(SV * dbh, imp_dbh_t * imp_dbh, char *dbname, char *user,
		 char *password)
{
#ifdef dTHR
	dTHR;
#endif

	if (dbis->debug >= 2)
		PerlIO_printf(DBILOGFP,
			      "imp_dbh->connect: dsn = %s, uid = %s, pwd = %s\n",
			      dbname ? dbname : "NULL",
			      user ? user : "NULL",
			      password ? password : "NULL");

	imp_dbh->stats.auto_reconnects_ok = 0;
	imp_dbh->stats.auto_reconnects_failed = 0;

	if (!_MyLogin(imp_dbh)) {
		do_error(dbh, mysql_errno(&imp_dbh->mysql),
			 mysql_error(&imp_dbh->mysql));
		return FALSE;
	}

	/*
	 *  Tell DBI, that dbh->disconnect should be called for this handle
	 */
	DBIc_ACTIVE_on(imp_dbh);

	/*
	 *  Tell DBI, that dbh->destroy should be called for this handle
	 */
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

int dbd_db_commit(SV * dbh, imp_dbh_t * imp_dbh)
{
	if (DBIc_has(imp_dbh, DBIcf_AutoCommit)) {
		do_warn(dbh, TX_ERR_AUTOCOMMIT,
			"Commmit ineffective while AutoCommit is on");
		return TRUE;
	}

	if (imp_dbh->has_transactions) {
#if MYSQL_VERSION_ID >=40101
		if (!imp_dbh->has_protocol41) {
#endif
			if (mysql_real_query(&imp_dbh->mysql, "COMMIT", 6)
			    != 0) {
				do_error(dbh, mysql_errno(&imp_dbh->mysql),
					 mysql_error(&imp_dbh->mysql));
				return FALSE;
			}
#if MYSQL_VERSION_ID >=40101
		} else {
			if (mysql_commit(&imp_dbh->mysql)) {
				do_error(dbh, mysql_errno(&imp_dbh->mysql),
					 mysql_error(&imp_dbh->mysql));
				return FALSE;
			}
		}
#endif
	} else {
		do_warn(dbh, JW_ERR_NOT_IMPLEMENTED,
			"Commmit ineffective while AutoCommit is on");
	}
	return TRUE;
}

int dbd_db_rollback(SV * dbh, imp_dbh_t * imp_dbh)
{
	/* croak, if not in AutoCommit mode */
	if (DBIc_has(imp_dbh, DBIcf_AutoCommit)) {
		do_warn(dbh, TX_ERR_AUTOCOMMIT,
			"Rollback ineffective while AutoCommit is on");
		return FALSE;
	}

	if (imp_dbh->has_transactions) {
#if MYSQL_VERSION_ID >=40101
		if (!imp_dbh->has_protocol41) {
#endif
			if (mysql_real_query
			    (&imp_dbh->mysql, "ROLLBACK", 8) != 0) {
				do_error(dbh, mysql_errno(&imp_dbh->mysql),
					 mysql_error(&imp_dbh->mysql));
				return FALSE;
			}
#if MYSQL_VERSION_ID >=40101
		} else {
			if (mysql_rollback(&imp_dbh->mysql)) {
				do_error(dbh, mysql_errno(&imp_dbh->mysql),
					 mysql_error(&imp_dbh->mysql));
				return FALSE;
			}
		}
#endif
	} else {
		do_error(dbh, JW_ERR_NOT_IMPLEMENTED,
			 "Rollback ineffective while AutoCommit is on");
	}
	return TRUE;
}


/***************************************************************************
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

int dbd_db_disconnect(SV * dbh, imp_dbh_t * imp_dbh)
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
	mysql_close(&imp_dbh->mysql);

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

int dbd_discon_all(SV * drh, imp_drh_t * imp_drh)
{
#if defined(dTHR)
	dTHR;
#endif

	/* The disconnect_all concept is flawed and needs more work */
	if (!dirty && !SvTRUE(perl_get_sv("DBI::PERL_ENDING", 0))) {
		sv_setiv(DBIc_ERR(imp_drh), (IV) 1);
		sv_setpv(DBIc_ERRSTR(imp_drh),
			 (char *) "disconnect_all not implemented");
		DBIh_EVENT2(drh, ERROR_event,
			    DBIc_ERR(imp_drh), DBIc_ERRSTR(imp_drh));
		return FALSE;
	}
	if (perl_destruct_level)
		perl_destruct_level = 0;
	return FALSE;
}


/***************************************************************************
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

void dbd_db_destroy(SV * dbh, imp_dbh_t * imp_dbh)
{

	/*
	 *  Being on the safe side never hurts ...
	 */
	if (DBIc_ACTIVE(imp_dbh)) {
		if (imp_dbh->has_transactions) {
			if (!DBIc_has(imp_dbh, DBIcf_AutoCommit)) {
#if MYSQL_VERSION_ID >=40101
				if (!imp_dbh->has_protocol41) {
#endif
					mysql_real_query(&imp_dbh->mysql,
							 "ROLLBACK", 8);
#if MYSQL_VERSION_ID >=40101
				} else {
					if (mysql_rollback
					    (&imp_dbh->mysql)) {
						do_error(dbh,
							 TX_ERR_ROLLBACK,
							 "ROLLBACK failed");
					}
				}
#endif
			}
		}
		dbd_db_disconnect(dbh, imp_dbh);
	}

	/*
	 *  Tell DBI, that dbh->destroy must no longer be called
	 */
	DBIc_off(imp_dbh, DBIcf_IMPSET);
}


/***************************************************************************
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

int dbd_db_STORE_attrib(SV * dbh, imp_dbh_t * imp_dbh, SV * keysv,
			SV * valuesv)
{
	STRLEN kl;
	char *key = SvPV(keysv, kl);
	SV *cachesv = Nullsv;
	int cacheit = FALSE;
	bool bool_value = SvTRUE(valuesv);

	if (kl == 10 && strEQ(key, "AutoCommit")) {
		if (imp_dbh->has_transactions) {
			int oldval = DBIc_has(imp_dbh, DBIcf_AutoCommit);

			/* if setting AutoCommit on ... */
			if (bool_value) {
				if (!oldval) {
#if MYSQL_VERSION_ID >=40101
					if (!imp_dbh->has_protocol41) {
#endif
						/*  Need to issue a commit before entering AutoCommit  */
						if (mysql_real_query
						    (&imp_dbh->mysql,
						     "COMMIT", 6) != 0) {
							do_error(dbh,
								 TX_ERR_COMMIT,
								 "COMMIT failed");
							return FALSE;
						}
						if (mysql_real_query
						    (&imp_dbh->mysql,
						     "SET AUTOCOMMIT=1",
						     16)
						    != 0) {
							do_error(dbh,
								 TX_ERR_AUTOCOMMIT,
								 "Turning on AutoCommit failed");
							return FALSE;
						}
#if MYSQL_VERSION_ID >=40101
					} else {
						if (mysql_commit
						    (&imp_dbh->mysql)) {
							do_error(dbh,
								 TX_ERR_COMMIT,
								 "COMMIT failed");
							return FALSE;
						}
						if (mysql_autocommit
						    (&imp_dbh->mysql, 1)) {
							do_error(dbh,
								 TX_ERR_AUTOCOMMIT,
								 "Turning on AutoCommit failed");
							return FALSE;
						}
					}
#endif
					DBIc_set(imp_dbh, DBIcf_AutoCommit,
						 bool_value);
				}
			} else {
				if (oldval) {
#if MYSQL_VERSION_ID >=40101
					if (!imp_dbh->has_protocol41) {
#endif
						if (mysql_real_query
						    (&imp_dbh->mysql,
						     "SET AUTOCOMMIT=0",
						     16) != 0) {
							do_error(dbh,
								 TX_ERR_AUTOCOMMIT,
								 "Turning off AutoCommit failed");
							return FALSE;
						}
#if MYSQL_VERSION_ID >=40101
					} else {
						if (mysql_autocommit
						    (&imp_dbh->mysql, 0)) {
							do_error(dbh,
								 TX_ERR_AUTOCOMMIT,
								 "Turning off AutoCommit failed");
							return FALSE;
						}
					}
#endif
					DBIc_set(imp_dbh, DBIcf_AutoCommit,
						 bool_value);
				}
			}
		} else {
			/*
			 *  We do support neither transactions nor "AutoCommit".
			 *  But we stub it. :-)
			 */
			if (!SvTRUE(valuesv)) {
				do_error(dbh, JW_ERR_NOT_IMPLEMENTED,
					 "Transactions not supported by database");
				croak
				    ("Transactions not supported by database");
			}
		}
	} else if (strlen("mysql_auto_reconnect")
		   == kl && strEQ(key, "mysql_auto_reconnect")) {
		/*XXX: Does DBI handle the magic ? */
		imp_dbh->auto_reconnect = bool_value;
		/* imp_dbh->mysql.reconnect=0; */
	} else if (kl == 20 && strEQ(key, "mysql_server_prepare")) {
		imp_dbh->has_protocol41 = SvTRUE(valuesv);
	} else {
		return FALSE;
	}

	if (cacheit)		/* cache value for later DBI 'quick' fetch? */
		hv_store((HV *) SvRV(dbh), key, kl, cachesv, 0);
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


static SV *my_ulonglong2str(my_ulonglong val)
{
	if (val == 0) {
		return newSVpv("0", 1);
	} else {
		char buf[64];
		char *ptr = buf + 63;
		*ptr = '\0';
		while (val > 0) {
			*(--ptr) = ('0' + (val % 10));
			val = val / 10;
		}
		return newSVpv(ptr, (buf + 63) - ptr);
	}
}

SV *dbd_db_FETCH_attrib(SV * dbh, imp_dbh_t * imp_dbh, SV * keysv)
{
	STRLEN kl;
	char *key = SvPV(keysv, kl);
	char *fine_key = NULL;
	SV *result = NULL;

	switch (*key) {
	case 'A':
		if (strEQ(key, "AutoCommit")) {
			if (imp_dbh->has_transactions) {
				return
				    sv_2mortal(boolSV
					       (DBIc_has
						(imp_dbh,
						 DBIcf_AutoCommit)));
			} else {
				return &sv_yes;
			}
		}
		break;
	}
	if (strncmp(key, "mysql_", 6) == 0) {
		fine_key = key;
		key = key + 6;
		kl = kl - 6;
	}

	switch (*key) {
	case 'a':
		if (kl == strlen("auto_reconnect")
		    && strEQ(key, "auto_reconnect"))
			result =
			    sv_2mortal(newSViv(imp_dbh->auto_reconnect));
		break;

	case 'e':
		if (strEQ(key, "errno")) {
			result =
			    sv_2mortal(newSViv
				       ((IV)
					mysql_errno(&imp_dbh->mysql)));
		} else if (strEQ(key, "error")) {
			const char *msg = mysql_error(&imp_dbh->mysql);
			result = sv_2mortal(newSVpv(msg, strlen(msg)));
		} else if (strEQ(key, "errmsg")) {
			/* Obsolete, as of 2.09! */
			const char *msg = mysql_error(&imp_dbh->mysql);
			result = sv_2mortal(newSVpv(msg, strlen(msg)));
		}
		break;

	case 'd':
		if (strEQ(key, "dbd_stats")) {
			HV *hv = newHV();
			hv_store(hv,
				 "auto_reconnects_ok",
				 strlen("auto_reconnects_ok"),
				 newSViv(imp_dbh->stats.
					 auto_reconnects_ok), 0);
			hv_store(hv, "auto_reconnects_failed",
				 strlen("auto_reconnects_failed"),
				 newSViv(imp_dbh->stats.
					 auto_reconnects_failed), 0);

			result = (newRV_noinc((SV *) hv));
		}

	case 'h':
		if (strEQ(key, "hostinfo")) {
			const char *hostinfo =
			    mysql_get_host_info(&imp_dbh->mysql);
			result =
			    hostinfo ?
			    sv_2mortal(newSVpv(hostinfo, strlen(hostinfo)))
			    : &sv_undef;
		}
		break;

	case 'i':
		if (strEQ(key, "info")) {
			const char *info = mysql_info(&imp_dbh->mysql);
			result =
			    info ? sv_2mortal(newSVpv(info, strlen(info)))
			    : &sv_undef;
		} else if (kl == 8 && strEQ(key, "insertid")) {
			/* We cannot return an IV, because the insertid is a long. */
			result =
			    sv_2mortal(my_ulonglong2str
				       (mysql_insert_id(&imp_dbh->mysql)));
		}
		break;

	case 'p':
		if (kl == 9 && strEQ(key, "protoinfo")) {
			result =
			    sv_2mortal(newSViv
				       (mysql_get_proto_info
					(&imp_dbh->mysql)));
		}
		break;

	case 's':
		if (kl == 10 && strEQ(key, "serverinfo")) {
			const char *serverinfo =
			    mysql_get_server_info(&imp_dbh->mysql);
			result =
			    serverinfo ?
			    sv_2mortal(newSVpv
				       (serverinfo,
					strlen(serverinfo))) : &sv_undef;
		} else if (strEQ(key, "sock")) {
			result =
			    sv_2mortal(newSViv((IV) & imp_dbh->mysql));
		} else if (strEQ(key, "sockfd")) {
			result =
			    sv_2mortal(newSViv
				       ((IV) imp_dbh->mysql.net.fd));
		} else if (strEQ(key, "stat")) {
			const char *stats = mysql_stat(&imp_dbh->mysql);
			result = stats ?
			    sv_2mortal(newSVpv(stats, strlen(stats))) :
			    &sv_undef;
		} else if (strEQ(key, "stats")) {
			/* Obsolete, as of 2.09 */
			const char *stats = mysql_stat(&imp_dbh->mysql);
			result = stats ?
			    sv_2mortal(newSVpv(stats, strlen(stats))) :
			    &sv_undef;
		} else if (kl == 14 && strEQ(key, "server_prepare")) {
			result =
			    sv_2mortal(newSViv
				       ((IV) imp_dbh->has_protocol41));
		}
		break;

	case 't':
		if (kl == 9 && strEQ(key, "thread_id")) {
			result =
			    sv_2mortal(newSViv
				       (mysql_thread_id(&imp_dbh->mysql)));
		}
		break;
	}

	if (result == NULL) {
		return Nullsv;
	}
	if (!fine_key) {
		/* Obsolete, as of 2.09 */
	}
	return result;
}


/***************************************************************************
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
dbd_st_prepare(SV * sth,
	       imp_sth_t * imp_sth, char *statement, SV * attribs)
{
	/* Initialize our data */
	int i;
	SV **svp;

	imp_sth->done_desc = 0;
	imp_sth->cda = NULL;
	imp_sth->currow = 0;

#if MYSQL_VERSION_ID >=40101
	D_imp_dbh_from_sth;
	MYSQL_BIND *bind;
	imp_sth_phb_t *fbind;
	imp_sth->fetch_done = 0;
	int col_type;
#endif

	svp = DBD_ATTRIB_GET_SVP(attribs, "mysql_use_result", 16);
	imp_sth->use_mysql_use_result = svp && SvTRUE(*svp);

	if (dbis->debug >= 2) {
		PerlIO_printf(DBILOGFP, "Setting mysql_use_result to %d\n",
			      imp_sth->use_mysql_use_result);
		PerlIO_printf(DBILOGFP,
			      "MYSQL_VERSION_ID %d, imp_sth->has_protocol41 %d\n",
			      MYSQL_VERSION_ID, imp_sth->has_protocol41);
	}
#if MYSQL_VERSION_ID >=40101
	//Set default value for sth from dbh
	imp_sth->has_protocol41 = imp_dbh->has_protocol41;
	svp = DBD_ATTRIB_GET_SVP(attribs, "mysql_server_prepare", 20);
	if (svp) {
		imp_sth->has_protocol41 = SvTRUE(*svp);
	}
	if (imp_sth->has_protocol41) {
		int limit_flag = 0;
		for (i = 0; i < strlen(statement) - 1; i++) {
			char *searchptr = &statement[i];
			// if there is a 'limit' in the statement...
			if (!limit_flag
			    && !strncasecmp(searchptr, "limit ", 6)) {
				limit_flag = 1;
				i += 6;
			}
			if (limit_flag) {
				// ... and place holders after the limit flag is set...
				if (statement[i] == '?') {
					// ... then we do not want to try server side prepare (use emulation)
					imp_sth->has_protocol41 = 0;
					i = strlen(statement) - 1;
				}
			}
		}
	}
#endif

	for (i = 0; i < AV_ATTRIB_LAST; i++) {
		imp_sth->av_attr[i] = Nullav;
	}

	if (imp_sth->has_protocol41 == 0) {
		/* Count the number of parameters, the same way mysql_param_count does for server side prepares */
		DBIc_NUM_PARAMS(imp_sth) = CountParam(statement);
	}
#if MYSQL_VERSION_ID >=40101
	/*
	 *  Perform check for LISTFIELDS command
	 *  and if we met it then mark as uncompatible with new 4.1 protocol 
	 *  i.e. we leave imp_sth->has_protocol41=0 for this stmt 
	 *  and it will be executed later in mysql_st_internal_execute()
	 *  TODO: I think we can replace LISTFIELDS with SHOW COLUMNS [LIKE ...]
	 *        to remove this extension hack
	 */

	/* this is a better way to do this */
	if (!strncasecmp(statement, "listfields ", 11)
	    && imp_sth->has_protocol41) {
		if (dbis->debug >= 2) {
			PerlIO_printf(DBILOGFP,
				      "\"listfields\" Statement: %s\n setting has_protocol41 to 0\n",
				      statement);
		}
		imp_sth->has_protocol41 = 0;
	}

	if (imp_sth->has_protocol41) {
		if (imp_sth->stmt) {
			fprintf(stderr,
				"ERROR: Trying to prepare new stmt while we have already not closed one \n");
		}

		imp_sth->stmt =
		    mysql_prepare(&imp_dbh->mysql, statement,
				  strlen(statement));

		if (imp_sth->stmt) {
			DBIc_NUM_PARAMS(imp_sth) =
			    mysql_param_count(imp_sth->stmt);

			if (DBIc_NUM_PARAMS(imp_sth) > 0) {
				/* Allocate memory for bind variables */
				imp_sth->bind =
				    AllocBind(DBIc_NUM_PARAMS(imp_sth));
				imp_sth->fbind =
				    AllocFBind(DBIc_NUM_PARAMS(imp_sth));
				imp_sth->has_binded = 0;

				/* if this statement has a result set, field types will be correctly identified. If there 
				 * is no result set, such as with an INSERT, fields will not be defined, and all buffer_type
				 * will default to MYSQL_TYPE_STRING */
				col_type =
				    (imp_sth->stmt->fields) ? imp_sth->
				    stmt->fields[i].
				    type : MYSQL_TYPE_STRING;

				if (dbis->debug >= 2) {
					// DEBUG CODE
					char *query =
					    imp_sth->stmt->
					    query ? imp_sth->stmt->
					    query : "NO QUERY";
					unsigned int param_count =
					    imp_sth->stmt->
					    param_count ? imp_sth->stmt->
					    param_count : 0;
					PerlIO_printf(DBILOGFP,
						      "query %s i => %d, col_type => %d param_count => %u\n",
						      query, i, col_type,
						      param_count);
				}

				//Initialize ph variables with  NULL values
				for (bind = imp_sth->bind, fbind =
				     imp_sth->fbind, i = 0;
				     i < DBIc_NUM_PARAMS(imp_sth);
				     i++, bind++, fbind++) {
					switch (col_type) {
					case MYSQL_TYPE_DECIMAL:
					case MYSQL_TYPE_DOUBLE:
					case MYSQL_TYPE_FLOAT:
						if (dbis->debug >= 2) {
							PerlIO_printf
							    (DBILOGFP,
							     "case INT type: i => %d, col_type => %d \n",
							     i, col_type);
						}
						bind->buffer_type =
						    MYSQL_TYPE_DOUBLE;
						bind->buffer = NULL;
						bind->length =
						    &(fbind->length);
						bind->is_null =
						    (char *) &(fbind->
							       is_null);
						fbind->is_null = 1;
						fbind->length = 0;
						break;

					case MYSQL_TYPE_SHORT:
					case MYSQL_TYPE_TINY:
					case MYSQL_TYPE_LONG:
					case MYSQL_TYPE_INT24:
					case MYSQL_TYPE_YEAR:
						if (dbis->debug >= 2) {
							PerlIO_printf
							    (DBILOGFP,
							     "case FLOAT type: i => %d, col_type => %d\n",
							     i, col_type);
						}
						bind->buffer_type =
						    MYSQL_TYPE_LONG;
						bind->buffer = NULL;
						bind->length =
						    &(fbind->length);
						bind->is_null =
						    (char *) &(fbind->
							       is_null);
						fbind->is_null = 1;
						fbind->length = 0;
						break;

					case MYSQL_TYPE_LONGLONG:
						if (dbis->debug >= 2) {
							PerlIO_printf
							    (DBILOGFP,
							     "case LONGLONG i => %d, col_type => %d\n",
							     i, col_type);
						}
						//bind->buffer_type= MYSQL_TYPE_LONGLONG;
						bind->buffer_type =
						    MYSQL_TYPE_STRING;
						bind->buffer = NULL;
						bind->length =
						    &(fbind->length);
						bind->is_null =
						    (char *) &(fbind->
							       is_null);
						fbind->is_null = 1;
						fbind->length = 0;
						break;

					case MYSQL_TYPE_DATE:
					case MYSQL_TYPE_TIME:
					case MYSQL_TYPE_DATETIME:
					case MYSQL_TYPE_NEWDATE:
					case MYSQL_TYPE_VAR_STRING:
					case MYSQL_TYPE_STRING:
					case MYSQL_TYPE_BLOB:
					case MYSQL_TYPE_TIMESTAMP:
						if (dbis->debug >= 2) {
							PerlIO_printf
							    (DBILOGFP,
							     "case STRING i => %d, col_type => %d\n",
							     i, col_type);
						}
						// Create string type here
						bind->buffer_type =
						    MYSQL_TYPE_STRING;
						bind->buffer = NULL;
						//bind->buffer_length= imp_sth->stmt->fields[i].length;
						bind->length =
						    &(fbind->length);
						bind->is_null =
						    (char *) &(fbind->
							       is_null);
						fbind->is_null = 1;
						fbind->length = 0;
						break;

					default:
						if (dbis->debug >= 2) {
							PerlIO_printf
							    (DBILOGFP,
							     "case default i => %d, col_type => %d\n",
							     i, col_type);
						}
						// Create string type here
						bind->buffer_type =
						    MYSQL_TYPE_STRING;
						bind->buffer = NULL;
						//bind->buffer_length= imp_sth->stmt->fields[i].length;
						bind->length =
						    &(fbind->length);
						bind->is_null =
						    (char *) &(fbind->
							       is_null);
						fbind->is_null = 1;
						fbind->length = 0;
						break;
					}
				}
			}
		} else {
			do_error(sth, mysql_errno(&imp_dbh->mysql),
				 mysql_error(&imp_dbh->mysql));
			return 0;
		}
	} else {
		imp_sth->has_protocol41 = 0;
	}
#endif

	/* Allocate memory for parameters */
	imp_sth->params = AllocParam(DBIc_NUM_PARAMS(imp_sth));
	DBIc_IMPSET_on(imp_sth);

	return 1;
}


/***************************************************************************
 *
 *  Name:    mysql_st_internal_execute
 *
 *  Purpose: Internal version for executing a statement, called both from
 *           within the "do" and the "execute" method.
 *
 *  Inputs:  h - object handle, for storing error messages
 *           statement - query being executed
 *           attribs - statement attributes, currently ignored
 *           numParams - number of parameters being bound
 *           params - parameter array
 *           cdaPtr - where to store results, if any
 *           svsock - socket connected to the database
 *
 **************************************************************************/

long mysql_st_internal_execute(SV * h,
			       SV * statement,
			       SV * attribs,
			       int numParams,
			       imp_sth_ph_t * params,
			       MYSQL_RES ** cdaPtr,
			       MYSQL * svsock, int use_mysql_use_result)
{
	STRLEN slen;
	char *sbuf = SvPV(statement, slen);
	char *salloc = ParseParam(svsock, sbuf, &slen, params, numParams);

	if (salloc) {
		sbuf = salloc;
		if (dbis->debug >= 2) {
			PerlIO_printf(DBILOGFP, "Binding parameters: %s\n",
				      sbuf);
		}
	}

	if (*cdaPtr) {
		mysql_free_result(*cdaPtr);
		*cdaPtr = NULL;
	}
	//if (slen >= 10
	// &&  tolower(sbuf[0]) == 'l'
	// &&  tolower(sbuf[1]) == 'i'
	// &&  tolower(sbuf[2]) == 's'
	// &&  tolower(sbuf[3]) == 't') 
	//{
	// if (slen >= 11
	//    &&  tolower(sbuf[4]) == 'f'
	//   &&  tolower(sbuf[5]) == 'i'
	//   &&  tolower(sbuf[6]) == 'e'
	//  &&  tolower(sbuf[7]) == 'l'
	//    &&  tolower(sbuf[8]) == 'd'
	//    &&  tolower(sbuf[9]) == 's'
	//    &&  isspace(sbuf[10])) 
	// did a test comparing the above code vs. using strncasecmp, and found
	// the latter has better performance
	if (slen >= 11 && !strncasecmp(sbuf, "listfields ", 11)) {
		char *table;

		slen -= 10;
		sbuf += 10;
		while (slen && isspace(*sbuf)) {
			--slen;
			++sbuf;
		}

		if (!slen) {
			do_error(h, JW_ERR_QUERY, "Missing table name");
			return -2;
		}
		if (!(table = malloc(slen + 1))) {
			do_error(h, JW_ERR_MEM, "Out of memory");
			return -2;
		}

		strncpy(table, sbuf, slen);
		sbuf = table;

		while (slen && !isspace(*sbuf)) {
			--slen;
			++sbuf;
		}
		*sbuf++ = '\0';

		*cdaPtr = mysql_list_fields(svsock, table, NULL);
		free(table);

		if (!(*cdaPtr)) {
			do_error(h, mysql_errno(svsock),
				 mysql_error(svsock));
			return -2;
		}

		return 0;
	}
	//}

	if ((mysql_real_query(svsock, sbuf, slen)) &&
	    (!mysql_db_reconnect(h) ||
	     (mysql_real_query(svsock, sbuf, slen)))) {
		Safefree(salloc);
		do_error(h, mysql_errno(svsock), mysql_error(svsock));
		return -2;
	}
	Safefree(salloc);

  /** Store the result from the Query */
	*cdaPtr = use_mysql_use_result ?
	    mysql_use_result(svsock) : mysql_store_result(svsock);

	if (mysql_errno(svsock)) {
		do_error(h, mysql_errno(svsock), mysql_error(svsock));
	}

	if (!*cdaPtr) {
		return mysql_affected_rows(svsock);
	} else {
		return mysql_num_rows(*cdaPtr);
	}
}

/***************************************************************************
 *
 *  Name:    mysql_st_internal_execute41
 *
 *  Purpose: Internal version for executing a prepared statement, called both from
 *           within the "do" and the "execute" method.
 *           MYSQL 4.1 API           
 *
 *
 *  Inputs:  h - object handle, for storing error messages
 *           statement - query being executed
 *           attribs - statement attributes, currently ignored
 *           numParams - number of parameters being bound
 *           params - parameter array
 *           cdaPtr - where to store results, if any
 *           svsock - socket connected to the database
 *
 **************************************************************************/

#if MYSQL_VERSION_ID >=40101

long mysql_st_internal_execute41(SV * h,
				 SV * statement,
				 SV * attribs,
				 int numParams,
				 imp_sth_ph_t * params,
				 MYSQL_RES ** cdaPtr,
				 MYSQL * svsock,
				 int use_mysql_use_result,
				 MYSQL_STMT * stmt,
				 MYSQL_BIND * bind, int *has_binded)
{
	STRLEN slen;

	if (dbis->debug >= 2) {
		PerlIO_printf(DBILOGFP,
			      "mysql_st_internal_execute41 bind size %d, numParams %d Executing Statement: %s\n",
			      sizeof(bind), numParams, SvPV(statement,
							    slen));
	}

	if (*cdaPtr)		//do we free metadata info
	{
		mysql_free_result(*cdaPtr);	//free it if not
		*cdaPtr = NULL;
	}

/* 
   If were performed any changes with ph variables 
   we have to rebind them 
*/

	if (numParams > 0 && !(*has_binded)) {
		if (mysql_bind_param(stmt, bind)) {
			fprintf(stderr, "\nparam bind failed\n");
			fprintf(stderr, "\n|%d| |%s|\n",
				mysql_stmt_errno(stmt),
				mysql_stmt_error(stmt));
			do_error(h, mysql_stmt_errno(stmt),
				 mysql_stmt_error(stmt));
			return -2;
		}
		*has_binded = 1;
	}

	if (dbis->debug >= 2) {
		PerlIO_printf(DBILOGFP,
			      "mysql_st_internal_execute41 calling mysql_execute\n");
	}

	if (mysql_execute(stmt)) {
		do_error(h, mysql_stmt_errno(stmt),
			 mysql_stmt_error(stmt));
		return -2;
	}

	if (!(*cdaPtr = mysql_get_metadata(stmt))) {
		if (mysql_stmt_errno(stmt)) {
			do_error(h, mysql_stmt_errno(stmt),
				 mysql_stmt_error(stmt));
			return -2;
		}
	} else {
#if defined IMPLEMENTED_STMT_USE_RESULT
		/* when mysql_stmt_use_result is implemented */
		if (use_mysql_use_result) {
#endif
			if (mysql_stmt_store_result(stmt)) {
				/* Get the total rows affected */
				return (long) mysql_stmt_num_rows(stmt);
			} else {
				do_error(h, mysql_stmt_errno(stmt),
					 mysql_stmt_error(stmt));
			}
#if defined IMPLEMENTED_STMT_USE_RESULT
		}
#endif
	}

	return mysql_stmt_affected_rows(stmt);
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

int dbd_st_execute(SV * sth, imp_sth_t * imp_sth)
{
	D_imp_dbh_from_sth;
	SV **statement;
	int i;
#if defined (dTHR)
	dTHR;
#endif

	if (dbis->debug >= 2) {
		PerlIO_printf(DBILOGFP,
			      "    -> dbd_st_execute for %08lx\n",
			      (u_long) sth);
	}

	if (!SvROK(sth) || SvTYPE(SvRV(sth)) != SVt_PVHV) {
		croak("Expected hash array");
	}

	/* Free cached array attributes */
	for (i = 0; i < AV_ATTRIB_LAST; i++) {
		if (imp_sth->av_attr[i]) {
#ifdef DEBUGGING_MEMORY_LEAK
			PerlIO_printf
			    ("Execute: Decrementing refcnt: old = %d\n",
			     SvREFCNT(imp_sth->av_attr[i]));
#endif
			SvREFCNT_dec(imp_sth->av_attr[i]);
		}
		imp_sth->av_attr[i] = Nullav;
	}

	statement = hv_fetch((HV *) SvRV(sth), "Statement", 9, FALSE);

#if MYSQL_VERSION_ID >=40101

	if (imp_sth->has_protocol41) {
		if (DBIc_ACTIVE(imp_sth)
		    && !(mysql_st_clean_cursor(sth, imp_sth))) {
			//FIXME: Have to add do_error HERE
			return 0;
		}

		imp_sth->row_num = mysql_st_internal_execute41(sth,
							       *statement,
							       NULL,
							       DBIc_NUM_PARAMS
							       (imp_sth),
							       imp_sth->
							       params,
							       &imp_sth->
							       cda,
							       &imp_dbh->
							       mysql,
							       imp_sth->
							       use_mysql_use_result,
							       imp_sth->
							       stmt,
							       imp_sth->
							       bind,
							       &imp_sth->
							       has_binded);
	} else {
#endif
		imp_sth->row_num = mysql_st_internal_execute(sth,
							     *statement,
							     NULL,
							     DBIc_NUM_PARAMS
							     (imp_sth),
							     imp_sth->
							     params,
							     &imp_sth->cda,
							     &imp_dbh->
							     mysql,
							     imp_sth->
							     use_mysql_use_result);

#if MYSQL_VERSION_ID >=40101
	}
#endif

	if (imp_sth->row_num != -2) {
		if (!imp_sth->cda) {
			imp_sth->insertid =
			    mysql_insert_id(&imp_dbh->mysql);
		} else {
      /** Store the result in the current statement handle */
			DBIc_ACTIVE_on(imp_sth);
			DBIc_NUM_FIELDS(imp_sth) =
			    mysql_num_fields(imp_sth->cda);
			imp_sth->done_desc = 0;
			imp_sth->fetch_done = 0;
		}
	}

	if (dbis->debug >= 2) {
		PerlIO_printf(DBILOGFP, "    <- dbd_st_execute %ld rows\n",
			      imp_sth->row_num);
	}

	return imp_sth->row_num;
}


/***************************************************************************
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

int dbd_describe(SV * sth, imp_sth_t * imp_sth)
{

	if (dbis->debug >= 2)
		PerlIO_printf(DBILOGFP, "** dbd_describe() **\n");

#if MYSQL_VERSION_ID >=40101

	if (imp_sth->has_protocol41) {
		int num_fields = DBIc_NUM_FIELDS(imp_sth);
		int i;
		int col_type;
		imp_sth_fbh_t *fbh;
		MYSQL_BIND *bind;
		MYSQL_FIELD *fields;

		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP,
				      "** dbd_describe() num_fields %d**\n",
				      num_fields);

		if (imp_sth->done_desc) {
			return TRUE;
		}

		if (!num_fields || !imp_sth->cda) {
			//no metadata
			do_error(sth, JW_ERR_SEQUENCE,
				 "no metadata information while trying describe result set");
			return 0;
		}

		/* allocate fields buffers  */
		if (!(imp_sth->fbh = AllocFBuffer(num_fields))
		    || !(imp_sth->buffer = AllocBuffer(num_fields))) {
			//Out of memory 
			do_error(sth, JW_ERR_SEQUENCE,
				 "Out of memory in dbd_sescribe()");
			return 0;
		}

		fields = mysql_fetch_fields(imp_sth->cda);

		for (fbh = imp_sth->fbh, bind =
		     (MYSQL_BIND *) imp_sth->buffer, i = 0; i < num_fields;
		     i++, fbh++, bind++) {
			// get the column type 
			col_type =
			    fields ? fields[i].type : MYSQL_TYPE_STRING;
			if (dbis->debug >= 2)
				PerlIO_printf(DBILOGFP, "col type %d\n",
					      col_type);

			switch (col_type) {
			case MYSQL_TYPE_DECIMAL:
			case MYSQL_TYPE_DOUBLE:
			case MYSQL_TYPE_FLOAT:
				bind->buffer_type = MYSQL_TYPE_DOUBLE;
				bind->buffer_length = fields[i].length;
				bind->length = &(fbh->length);
				bind->is_null = &(fbh->is_null);
				Newz(908, fbh->data, fields[i].length,
				     char);
				bind->buffer = (char *) &fbh->ddata;
				break;

			case MYSQL_TYPE_SHORT:
			case MYSQL_TYPE_TINY:
			case MYSQL_TYPE_LONG:
			case MYSQL_TYPE_INT24:
			case MYSQL_TYPE_YEAR:
				bind->buffer_type = MYSQL_TYPE_LONG;
				bind->buffer_length = fields[i].length;
				bind->length = &(fbh->length);
				bind->is_null = &(fbh->is_null);
				Newz(908, fbh->data, fields[i].length,
				     char);
				bind->buffer = (char *) &fbh->ldata;
				break;

			case MYSQL_TYPE_LONGLONG:
				//bind->buffer_type= MYSQL_TYPE_LONGLONG;
				/* perl handles long long as double
				 * so we'll set this to string */
				bind->buffer_type = MYSQL_TYPE_STRING;
				bind->buffer_length = fields[i].length;
				bind->length = &(fbh->length);
				bind->is_null = &(fbh->is_null);
				Newz(908, fbh->data, fields[i].length,
				     char);
				bind->buffer = (char *) fbh->data;
				// must treat as a string for now
				//bind->buffer = (char *) fbh->data;
				break;

			case MYSQL_TYPE_DATE:
			case MYSQL_TYPE_TIME:
			case MYSQL_TYPE_DATETIME:
			case MYSQL_TYPE_NEWDATE:
			case MYSQL_TYPE_VAR_STRING:
			case MYSQL_TYPE_STRING:
			case MYSQL_TYPE_BLOB:
			case MYSQL_TYPE_TIMESTAMP:
				// Create string type here
				bind->buffer_type = MYSQL_TYPE_STRING;
				bind->buffer_length = fields[i].length;
				bind->length = &(fbh->length);
				bind->is_null = &(fbh->is_null);
				Newz(908, fbh->data, fields[i].length,
				     char);
				bind->buffer = (char *) fbh->data;

			default:
				// Create string type here
				bind->buffer_type = MYSQL_TYPE_STRING;
				bind->buffer_length = fields[i].length;
				bind->length = &(fbh->length);
				bind->is_null = &(fbh->is_null);
				Newz(908, fbh->data, fields[i].length,
				     char);
				bind->buffer = (char *) fbh->data;

			}	// end of switch
		}		// end of for

		if (mysql_bind_result(imp_sth->stmt, imp_sth->buffer)) {
			do_error(sth, mysql_stmt_errno(imp_sth->stmt),
				 mysql_stmt_error(imp_sth->stmt));
			return 0;
			//return FALSE;
		}
	}
#endif

	imp_sth->done_desc = 1;
	return TRUE;
}


/***************************************************************************
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

AV *dbd_st_fetch(SV * sth, imp_sth_t * imp_sth)
{
	int num_fields;
	int ChopBlanks;
	int i;
	AV *av;
	MYSQL_ROW cols;
	unsigned long *lengths;

#if MYSQL_VERSION_ID >=40101
	if (imp_sth->has_protocol41) {
		if (!DBIc_ACTIVE(imp_sth)) {
			do_error(sth, JW_ERR_SEQUENCE,
				 "no statement executing\n");
			return Nullav;
		}

		if (imp_sth->fetch_done) {
			do_error(sth, JW_ERR_SEQUENCE,
				 "fetch() but fetch already done");
			return Nullav;
		}

		if (!imp_sth->done_desc) {
			if (!dbd_describe(sth, imp_sth)) {
				do_error(sth, JW_ERR_SEQUENCE,
					 "Error while describe result set.");
				return Nullav;
			}
		}
	}
#endif

	ChopBlanks = DBIc_is(imp_sth, DBIcf_ChopBlanks);

	if (dbis->debug >= 2) {
		PerlIO_printf(DBILOGFP,
			      "    -> dbd_st_fetch for %08lx, chopblanks %d\n",
			      (u_long) sth, ChopBlanks);
	}

	if (!imp_sth->cda) {
		do_error(sth, JW_ERR_SEQUENCE,
			 "fetch() without execute()");
		return Nullav;
	}
#if MYSQL_VERSION_ID >=40101
	int rc;
	imp_sth_fbh_t *fbh;
	MYSQL_BIND *bind;
	if (imp_sth->has_protocol41) {
		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP,
				      "dbd_st_fetch calling mysql_fetch\n");

		if ((rc = mysql_fetch(imp_sth->stmt))) {

			if (rc == 1) {
				do_error(sth,
					 mysql_stmt_errno(imp_sth->stmt),
					 mysql_stmt_error(imp_sth->stmt));
			}

			if (rc == 100) {
				//Update row_num to affected_rows value 
				imp_sth->row_num =
				    (long)
				    mysql_stmt_affected_rows(imp_sth->
							     stmt);
				imp_sth->fetch_done = 1;
			}

			if (!DBIc_COMPAT(imp_sth)) {
				dbd_st_finish(sth, imp_sth);
			}

			return Nullav;
		}

		imp_sth->currow++;

		av = DBIS->get_fbav(imp_sth);
		num_fields = av_len(av) + 1;
		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP,
				      "dbd_st_fetch called mysql_fetch, rc %d num_fields %d\n",
				      rc, num_fields);

		for (bind = imp_sth->buffer,
		     fbh = imp_sth->fbh,
		     i = 0; i < num_fields; i++, fbh++, bind++) {
			SV *sv = AvARRAY(av)[i];	/* Note: we (re)use the SV in the AV     */

			// This is wrong, null is not being set correctly
			// This is not the way to determine length (shit this would break blobs!) 
			if (fbh->is_null) {
				(void) SvOK_off(sv);	/*  Field is NULL, return undef  */
			} else {
				/* This does look a lot like Georg's PHP driver doesn't it?  --Brian */
				/* Credit due to Georg - mysqli_api.c  ;) --PMG */
				switch (bind->buffer_type) {
				case MYSQL_TYPE_DECIMAL:
				case MYSQL_TYPE_DOUBLE:
				case MYSQL_TYPE_FLOAT:
					if (dbis->debug >= 2)
						PerlIO_printf(DBILOGFP,
							      "st_fetch double data %f\n",
							      fbh->ddata);
					sv_setnv(sv, fbh->ddata);
					break;

				case MYSQL_TYPE_SHORT:
				case MYSQL_TYPE_TINY:
				case MYSQL_TYPE_LONG:
				case MYSQL_TYPE_INT24:
				case MYSQL_TYPE_YEAR:
					if (dbis->debug >= 2)
						PerlIO_printf(DBILOGFP,
							      "st_fetch int data %d\n",
							      fbh->ldata);
					sv_setuv(sv, fbh->ldata);
					break;

					/* Create LONG LONG would need a sv_set method for larger 
					   intenger, so we change to a string. 
					   Note to self: contribute sv_setlonglong to perl guts ;) */
				case MYSQL_TYPE_LONGLONG:
					if (dbis->debug >= 2)
						PerlIO_printf(DBILOGFP,
							      "st_fetch long long data (string) %s\n",
							      fbh->data);

					// this can't be used because it doesn't handle numbers large enough
					// sv_setuv(sv, fbh->lldata);
					sv_setpvn(sv, fbh->data,
						  fbh->length);
					break;

				case MYSQL_TYPE_DATE:
				case MYSQL_TYPE_TIME:
				case MYSQL_TYPE_DATETIME:
				case MYSQL_TYPE_NEWDATE:
				case MYSQL_TYPE_VAR_STRING:
				case MYSQL_TYPE_STRING:
				case MYSQL_TYPE_BLOB:
				case MYSQL_TYPE_TIMESTAMP:
					if (dbis->debug >= 2)
						PerlIO_printf(DBILOGFP,
							      "st_fetch string data %s\n",
							      fbh->data);
					sv_setpvn(sv, fbh->data,
						  fbh->length);
					break;

				default:
					if (dbis->debug >= 2)
						PerlIO_printf(DBILOGFP,
							      "st_fetch string data %s\n",
							      fbh->data);
					sv_setpvn(sv, fbh->data,
						  fbh->length);
					break;

				}	// end of switch
			}	// end of else 
		}		// end of for loop 

		if (dbis->debug >= 2) {
			PerlIO_printf(DBILOGFP,
				      "<- dbd_st_fetch, %d cols\n",
				      num_fields);
		}
		return av;

	} else {
#endif

		imp_sth->currow++;

		if (!(cols = mysql_fetch_row(imp_sth->cda))) {
			D_imp_dbh_from_sth;
			if (mysql_errno(&imp_dbh->mysql)) {
				do_error(sth, mysql_errno(&imp_dbh->mysql),
					 mysql_error(&imp_dbh->mysql));
			}
			if (!DBIc_COMPAT(imp_sth)) {
				dbd_st_finish(sth, imp_sth);
			}
			return Nullav;
		}

		lengths = mysql_fetch_lengths(imp_sth->cda);

		av = DBIS->get_fbav(imp_sth);
		num_fields = av_len(av) + 1;

		for (i = 0; i < num_fields; ++i) {
			char *col = cols[i];
			SV *sv = AvARRAY(av)[i];	/* Note: we (re)use the SV in the AV     */

			if (col) {
				STRLEN len = lengths[i];
				if (ChopBlanks) {
					while (len && col[len - 1] == ' ') {
						--len;
					}
				}
				sv_setpvn(sv, col, len);
			} else {
				(void) SvOK_off(sv);	/*  Field is NULL, return undef  */
			}
		}

		if (dbis->debug >= 2) {
			PerlIO_printf(DBILOGFP,
				      "    <- dbd_st_fetch, %d cols\n",
				      num_fields);
		}
		return av;

#if MYSQL_VERSION_ID  >= 40101
	}
#endif

}

#if MYSQL_VERSION_ID >=40101
/* 
   We have to fetch all data from stmt
   There is may be usefull for 2 cases:
   1. st_finish when we have undef statement
   2. call st_execute again when we have some unfetched data in stmt
*/

int mysql_st_clean_cursor(SV * sth, imp_sth_t * imp_sth)
{

	if (DBIc_ACTIVE(imp_sth) && dbd_describe(sth, imp_sth)
	    && !imp_sth->fetch_done) {
		mysql_stmt_free_result(imp_sth->stmt);
	}
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

int dbd_st_finish(SV * sth, imp_sth_t * imp_sth)
{

#if MYSQL_VERSION_ID >=40101
	imp_sth_fbh_t *fbh;
	int i, num_fields;
#endif

#if defined (dTHR)
	dTHR;
#endif

#if MYSQL_VERSION_ID >=40101
	if (imp_sth->has_protocol41) {
		if (imp_sth && imp_sth->stmt) {
			if (!mysql_st_clean_cursor(sth, imp_sth)) {
				do_error(sth, JW_ERR_SEQUENCE,
					 "Error happened while tried to clean up stmt");
				return 0;
			}

			if (imp_sth->fbh) {
				num_fields = DBIc_NUM_FIELDS(imp_sth);

				for (fbh = imp_sth->fbh, i = 0;
				     i < num_fields; i++, fbh++) {
					if (fbh->data) {
						Safefree(fbh->data);
					}
				}
				FreeFBuffer(imp_sth->fbh);
			}
			FreeBuffer(imp_sth->buffer);

			imp_sth->buffer = NULL;
			imp_sth->fbh = NULL;
		}
	}
#endif

	/* Cancel further fetches from this cursor.  */
	/* We don't close the cursor till DESTROY. */
	/* The application may re execute it.  */
	if (imp_sth && imp_sth->cda) {
		mysql_free_result(imp_sth->cda);
		imp_sth->cda = NULL;
	}
	DBIc_ACTIVE_off(imp_sth);
	return 1;
}


/***************************************************************************
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

void dbd_st_destroy(SV * sth, imp_sth_t * imp_sth)
{
	int i;

#if MYSQL_VERSION_ID >=40101
	int num_fields;

	if (imp_sth->has_protocol41) {
		if (imp_sth->stmt) {
			num_fields = DBIc_NUM_FIELDS(imp_sth);

			if (mysql_stmt_close(imp_sth->stmt)) {
				PerlIO_printf(DBILOGFP,
					      "DESTROY: Error %s while close stmt\n",
					      (char *)
					      mysql_stmt_error(imp_sth->
							       stmt));
				do_error(sth,
					 mysql_stmt_errno(imp_sth->stmt),
					 mysql_stmt_error(imp_sth->stmt));
			}

			if (DBIc_NUM_PARAMS(imp_sth) > 0) {
				FreeBind(imp_sth->bind);
				FreeFBind(imp_sth->fbind);
			}

			imp_sth->bind = NULL;
			imp_sth->fbind = NULL;
		}
	}
#endif

	/* dbd_st_finish has already been called by .xs code if needed.       */

	/* Free values allocated by dbd_bind_ph */
	FreeParam(imp_sth->params, DBIc_NUM_PARAMS(imp_sth));
	imp_sth->params = NULL;

	if (imp_sth->params) {
		FreeParam(imp_sth->params, DBIc_NUM_PARAMS(imp_sth));
		imp_sth->params = NULL;
	}

	/* Free cached array attributes */
	for (i = 0; i < AV_ATTRIB_LAST; i++) {
		if (imp_sth->av_attr[i]) {
#ifdef DEBUGGING_MEMORY_LEAK
			PerlIO_printf
			    ("DESTROY: Decrementing refcnt: old = %d\n",
			     SvREFCNT(imp_sth->av_attr[i]));
#endif
			SvREFCNT_dec(imp_sth->av_attr[i]);
		}

		imp_sth->av_attr[i] = Nullav;
	}

	DBIc_IMPSET_off(imp_sth);	/* let DBI know we've done it   */
}


/***************************************************************************
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
dbd_st_STORE_attrib(SV * sth,
		    imp_sth_t * imp_sth, SV * keysv, SV * valuesv)
{
	STRLEN(kl);
	char *key = SvPV(keysv, kl);
	int result = FALSE;

	if (dbis->debug >= 2) {
		PerlIO_printf(DBILOGFP,
			      "-> dbd_st_STORE_attrib for %08lx, key %s\n",
			      (u_long) sth, key);
	}

	if (strEQ(key, "mysql_use_result")) {
		imp_sth->use_mysql_use_result = SvTRUE(valuesv);
	}

	if (dbis->debug >= 2) {
		PerlIO_printf(DBILOGFP,
			      "<- dbd_st_STORE_attrib for %08lx, result %d\n",
			      (u_long) sth, result);
	}

	return result;
}


/***************************************************************************
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

SV *dbd_st_FETCH_internal(SV * sth, int what, MYSQL_RES * res, int cacheit)
{
	D_imp_sth(sth);
	AV *av = Nullav;
	MYSQL_FIELD *curField;

	/* Are we asking for a legal value? */
	if (what < 0 || what >= AV_ATTRIB_LAST) {
		do_error(sth, JW_ERR_NOT_IMPLEMENTED, "Not implemented");

		/* Return cached value, if possible */
	} else if (cacheit && imp_sth->av_attr[what]) {
		av = imp_sth->av_attr[what];

		/* Does this sth really have a result? */
	} else if (!res) {
		do_error(sth, JW_ERR_NOT_ACTIVE,
			 "statement contains no result");

		/* Do the real work. */
	} else {
		av = newAV();
		mysql_field_seek(res, 0);
		while ((curField = mysql_fetch_field(res))) {
			SV *sv;

			switch (what) {
			case AV_ATTRIB_NAME:
				sv = newSVpv(curField->name,
					     strlen(curField->name));
				break;

			case AV_ATTRIB_TABLE:
				sv = newSVpv(curField->table,
					     strlen(curField->table));
				break;

			case AV_ATTRIB_TYPE:
				sv = newSViv((int) curField->type);
				break;

			case AV_ATTRIB_SQL_TYPE:
				sv = newSViv((int)
					     native2sql(curField->type)->
					     data_type);
				break;
			case AV_ATTRIB_IS_PRI_KEY:
				sv = boolSV(IS_PRI_KEY(curField->flags));
				break;

			case AV_ATTRIB_IS_NOT_NULL:
				sv = boolSV(IS_NOT_NULL(curField->flags));
				break;

			case AV_ATTRIB_NULLABLE:
				sv = boolSV(!IS_NOT_NULL(curField->flags));
				break;

			case AV_ATTRIB_LENGTH:
				sv = newSViv((int) curField->length);
				break;

			case AV_ATTRIB_IS_NUM:
				sv = newSViv((int)
					     native2sql(curField->type)->
					     is_num);
				break;

			case AV_ATTRIB_TYPE_NAME:
				sv = newSVpv((char *)
					     native2sql(curField->type)->
					     type_name, 0);
				break;

			case AV_ATTRIB_MAX_LENGTH:
				sv = newSViv((int) curField->max_length);
				break;

			case AV_ATTRIB_IS_AUTO_INCREMENT:
#if defined(AUTO_INCREMENT_FLAG)
				sv = boolSV(IS_AUTO_INCREMENT
					    (curField->flags));
				break;
#else
				croak
				    ("AUTO_INCREMENT_FLAG is not supported on this machine");
#endif

			case AV_ATTRIB_IS_KEY:
				sv = boolSV(IS_KEY(curField->flags));
				break;

			case AV_ATTRIB_IS_BLOB:
				sv = boolSV(IS_BLOB(curField->flags));
				break;

			case AV_ATTRIB_SCALE:
				sv = newSViv((int) curField->decimals);
				break;

			case AV_ATTRIB_PRECISION:
				sv = newSViv((int)
					     (curField->length >
					      curField->
					      max_length) ? curField->
					     length : curField->
					     max_length);
				break;

			default:
				sv = &sv_undef;
				break;
			}
			av_push(av, sv);
		}

		/* Ensure that this value is kept, decremented in
		 *  dbd_st_destroy and dbd_st_execute.  */
		if (cacheit) {
			imp_sth->av_attr[what] = av;
		} else {
			return sv_2mortal(newRV_noinc((SV *) av));
		}
	}

	if (av == Nullav) {
		return &sv_undef;
	}
	return sv_2mortal(newRV_inc((SV *) av));
}


/***************************************************************************
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
    dbd_st_FETCH_internal(sth, (what), imp_sth->cda, TRUE)

SV *dbd_st_FETCH_attrib(SV * sth, imp_sth_t * imp_sth, SV * keysv)
{
	STRLEN(kl);
	char *key = SvPV(keysv, kl);
	SV *retsv = Nullsv;
	if (kl < 2) {
		return Nullsv;
	}

	if (dbis->debug >= 2) {
		PerlIO_printf(DBILOGFP,
			      "    -> dbd_st_FETCH_attrib for %08lx, key %s\n",
			      (u_long) sth, key);
	}

	switch (*key) {
	case 'N':
		if (strEQ(key, "NAME")) {
			retsv = ST_FETCH_AV(AV_ATTRIB_NAME);
		} else if (strEQ(key, "NULLABLE")) {
			retsv = ST_FETCH_AV(AV_ATTRIB_NULLABLE);
		}
		break;
	case 'P':
		if (strEQ(key, "PRECISION")) {
			retsv = ST_FETCH_AV(AV_ATTRIB_PRECISION);
		}
		break;
	case 'S':
		if (strEQ(key, "SCALE")) {
			retsv = ST_FETCH_AV(AV_ATTRIB_SCALE);
		}
		break;
	case 'T':
		if (strEQ(key, "TYPE")) {
			retsv = ST_FETCH_AV(AV_ATTRIB_SQL_TYPE);
		}
		break;
	case 'm':
		switch (kl) {
		case 10:
			if (strEQ(key, "mysql_type")) {
				retsv = ST_FETCH_AV(AV_ATTRIB_TYPE);
			}
			break;
		case 11:
			if (strEQ(key, "mysql_table")) {
				retsv = ST_FETCH_AV(AV_ATTRIB_TABLE);
			}
			break;
		case 12:
			if (strEQ(key, "mysql_is_key")) {
				retsv = ST_FETCH_AV(AV_ATTRIB_IS_KEY);
			} else if (strEQ(key, "mysql_is_num")) {
				retsv = ST_FETCH_AV(AV_ATTRIB_IS_NUM);
			} else if (strEQ(key, "mysql_length")) {
				retsv = ST_FETCH_AV(AV_ATTRIB_LENGTH);
			} else if (strEQ(key, "mysql_result")) {
				retsv =
				    sv_2mortal(newSViv((IV) imp_sth->cda));
			}
			break;
		case 13:
			if (strEQ(key, "mysql_is_blob")) {
				retsv = ST_FETCH_AV(AV_ATTRIB_IS_BLOB);
			}
			break;
		case 14:
			if (strEQ(key, "mysql_insertid")) {
				/* We cannot return an IV, because the insertid is a long.
				 */
				return
				    sv_2mortal(my_ulonglong2str
					       (imp_sth->insertid));
			}
			break;
		case 15:
			if (strEQ(key, "mysql_type_name")) {
				retsv = ST_FETCH_AV(AV_ATTRIB_TYPE_NAME);
			}
			break;
		case 16:
			if (strEQ(key, "mysql_is_pri_key")) {
				retsv = ST_FETCH_AV(AV_ATTRIB_IS_PRI_KEY);
			} else if (strEQ(key, "mysql_max_length")) {
				retsv = ST_FETCH_AV(AV_ATTRIB_MAX_LENGTH);
			} else if (strEQ(key, "mysql_use_result")) {
				retsv =
				    boolSV(imp_sth->use_mysql_use_result);
			}
			break;
		case 20:
			if (strEQ(key, "mysql_server_prepare")) {
#if MYSQL_VERSION_ID >=40101
				retsv =
				    sv_2mortal(newSViv
					       ((IV) imp_sth->
						has_protocol41));
#else
				retsv = boolSV(0);
#endif
			}
			break;
		case 23:
			if (strEQ(key, "mysql_is_auto_increment")) {
				retsv =
				    ST_FETCH_AV
				    (AV_ATTRIB_IS_AUTO_INCREMENT);
			}
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

int dbd_st_blob_read(SV * sth,
		     imp_sth_t * imp_sth,
		     int field,
		     long offset, long len, SV * destrv, long destoffset)
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

int dbd_bind_ph(SV * sth, imp_sth_t * imp_sth, SV * param, SV * value,
		IV sql_type, SV * attribs, int is_inout, IV maxlen)
{
	int rc;
	int paramNum = SvIV(param);
	int idx = paramNum - 1;

#if MYSQL_VERSION_ID >=40101
	STRLEN slen;
#endif

	if (paramNum <= 0 || paramNum > DBIc_NUM_PARAMS(imp_sth)) {
		do_error(sth, JW_ERR_ILLEGAL_PARAM_NUM,
			 "Illegal parameter number");
		return FALSE;
	}

	if (is_inout) {
		do_error(sth, JW_ERR_NOT_IMPLEMENTED,
			 "Output parameters not implemented");
		return FALSE;
	}

	if (dbis->debug >= 2) {
		if (SvOK(value)) {
			char *foof = SvPV(value, slen);
			PerlIO_printf(DBILOGFP,
				      "   type %d value is SCALAR ->%s<- \n",
				      sql_type, foof);
		} else {
			PerlIO_printf(DBILOGFP,
				      "   type %d value is NULL -><- \n",
				      sql_type);
		}
	}
	rc = BindParam(&imp_sth->params[idx], value, sql_type);

#if MYSQL_VERSION_ID >=40101

	if (imp_sth->has_protocol41) {
		if (imp_sth->has_binded == 0)	//first bind
		{
			//SQL_VARCHAR
			imp_sth->bind[idx].buffer_type =
			    MYSQL_TYPE_VAR_STRING;
			imp_sth->bind[idx].length =
			    (ulong *) & (imp_sth->fbind[idx].length);
			imp_sth->bind[idx].is_null =
			    (char *) &(imp_sth->fbind[idx].is_null);

			if (SvOK(imp_sth->params[idx].value)
			    && imp_sth->params[idx].value) {
				if (dbis->debug >= 2)
					PerlIO_printf(DBILOGFP,
						      "(first bind)   SCALAR IS STRING %s\n",
						      imp_sth->bind[idx].
						      buffer);
				imp_sth->bind[idx].buffer =
				    SvPV(imp_sth->params[idx].value, slen);
				imp_sth->bind[idx].buffer_length = slen;	////Should be here max value for this param?
				imp_sth->fbind[idx].is_null = 0;
				imp_sth->fbind[idx].length = slen;
			} else {
				//NULL value 
				if (dbis->debug >= 2)
					PerlIO_printf(DBILOGFP,
						      "(first bind)   SCALAR IS NULL\n");
				imp_sth->bind[idx].buffer =
				    SvPV(imp_sth->params[idx].value, slen);
				imp_sth->fbind[idx].is_null = 1;
				imp_sth->fbind[idx].length = 0;
			}
		} else {
			//rebind ph variable
			//Map the new data direct to stmt handler
			//as this is not first bind 
			if (SvOK(imp_sth->params[idx].value)
			    && imp_sth->params[idx].value) {
				if (dbis->debug >= 2)
					PerlIO_printf(DBILOGFP,
						      "   SCALAR IS STRING %s\n",
						      imp_sth->bind[idx].
						      buffer);
				imp_sth->stmt->params[idx].buffer =
				    SvPV(imp_sth->params[idx].value, slen);
				imp_sth->stmt->params[idx].buffer_length = slen;	//Should be here max value for this param?
				imp_sth->fbind[idx].length = slen;
				imp_sth->fbind[idx].is_null = 0;
			} else {
				//NULL value 
				if (dbis->debug >= 2)
					PerlIO_printf(DBILOGFP,
						      "   SCALAR IS NULL\n");
				imp_sth->stmt->params[idx].buffer = NULL;
				imp_sth->fbind[idx].is_null = 1;
				imp_sth->fbind[idx].length = 0;
			}
		}

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

int mysql_db_reconnect(SV * h)
{
	D_imp_xxh(h);
	imp_dbh_t *imp_dbh;
	MYSQL save_socket;

	if (DBIc_TYPE(imp_xxh) == DBIt_ST) {
		imp_dbh = (imp_dbh_t *) DBIc_PARENT_COM(imp_xxh);
		h = DBIc_PARENT_H(imp_xxh);
	} else {
		imp_dbh = (imp_dbh_t *) imp_xxh;
	}

	if (mysql_errno(&imp_dbh->mysql) != CR_SERVER_GONE_ERROR) {
		/* Other error */
		return FALSE;
	}

	if (!DBIc_has(imp_dbh, DBIcf_AutoCommit)
	    || !imp_dbh->auto_reconnect) {
		/* We never reconnect if AutoCommit is turned off.
		 * Otherwise we might get an inconsistent transaction
		 * state.
		 */
		return FALSE;
	}

	/* _MyLogin will blow away imp_dbh->mysql so we save a copy of
	 * imp_dbh->mysql and put it back where it belongs if the reconnect
	 * fail.  Think server is down & reconnect fails but the application eval{}s
	 * the execute, so next time $dbh->quote() gets called, instant SIGSEGV!
	 */
	save_socket = imp_dbh->mysql;
	memcpy(&save_socket, &imp_dbh->mysql, sizeof(save_socket));
	memset(&imp_dbh->mysql, 0, sizeof(imp_dbh->mysql));

	if (!_MyLogin(imp_dbh)) {
		do_error(h, mysql_errno(&imp_dbh->mysql),
			 mysql_error(&imp_dbh->mysql));
		memcpy(&imp_dbh->mysql, &save_socket, sizeof(save_socket));
		++imp_dbh->stats.auto_reconnects_failed;
		return FALSE;
	} else {
		++imp_dbh->stats.auto_reconnects_ok;
	}
	return TRUE;
}


/***************************************************************************
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

AV *dbd_db_type_info_all(SV * dbh, imp_dbh_t * imp_dbh)
{
	AV* array;
	array =  build_type_info_all();
	return array;
}


SV *dbd_db_quote(SV * dbh, SV * str, SV * type)
{
	SV *result;
	char *ptr;
	char *sptr;
	STRLEN len;


	if (SvGMAGICAL(str))
		mg_get(str);

	if (!SvOK(str)) {
		result = newSVpv("NULL", 4);
	} else {
		D_imp_dbh(dbh);
		if (type && SvOK(type)) {
			int i;
			int tp = SvIV(type);
			const sql_type_info_t *t = sql_type_data(tp);
			if (t && (!t->literal_prefix)) {
					return Nullsv;
			}
		}

		ptr = SvPV(str, len);
		result = newSV(len * 2 + 3);
		sptr = SvPVX(result);

		*sptr++ = '\'';
		sptr += mysql_real_escape_string(&imp_dbh->mysql, sptr,
						 ptr, len);
		*sptr++ = '\'';
		SvPOK_on(result);
		SvCUR_set(result, sptr - SvPVX(result));
		*sptr++ = '\0';	/*  Never hurts NUL terminating a Perl
				 *      string ...
				 */
	}
	return result;
}
