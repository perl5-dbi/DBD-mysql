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
#include "commit.c"		// TODO: Fix & clean up -- Make header file &c.

#if defined(WIN32)  &&  defined(WORD)
    /*  Don't exactly know who's responsible for defining WORD ... :-(  */
#undef WORD
typedef short WORD;
#endif

#define DECODE_KEY(a) ((sizeof(a)-1) == kl && strEQ(a,key))
static bool set_autocommit(SV *dbh, imp_dbh_t *imp_dbh, bool do_auto_commit,
	bool commit_first);


DBISTATE_DECLARE;

#define SQL_GET_TYPE_INFO_num \
	(sizeof(SQL_GET_TYPE_INFO_values)/sizeof(sql_type_info_t))


/***************************************************************************
 *  Name:    dbd_init
 *  Purpose: Called when the driver is installed by DBI
 *  Input:   dbistate - pointer to the DBIS variable, used for some
 *               DBI internal things
 *  Returns: Nothing
 **************************************************************************/

void dbd_init(dbistate_t * dbistate)
{
	DBIS = dbistate;
}


/***************************************************************************
 *  Name:    do_error, do_warn
 *  Purpose: Called to associate an error code and an error message
 *           to some handle
 *  Input:   h - the handle in error condition
 *           rc - the error code
 *           what - the error message
 *  Returns: Nothing
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
 *  Name:    mysql_dr_connect
 *  Purpose: Replacement for mysql_connect
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
	if (port && *port)
		portNr = atoi(port);
	else
		portNr = 0;
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

	/*XXX When does this get called with !imp_dbh ?? */
	if (!imp_dbh)
		goto do_connect;

#ifdef DBD_MYSQL_REAL_PREPARE
	imp_dbh->real_prepare = TRUE;
#else
	imp_dbh->real_prepare = FALSE;
#endif
#if MYSQL_VERSION_ID >=40101
	imp_dbh->has_protocol41 = TRUE;
	client_flag |= CLIENT_PROTOCOL_41;
#else
	imp_dbh->has_protocol41 = FALSE;
	client_flag &= ~CLIENT_PROTOCOL_41;
#endif
	SV *sv = DBIc_IMP_DATA(imp_dbh);
	imp_dbh->auto_reconnect = FALSE;	/* Safer we flip this to TRUE perl side 
						   if we detect a mod_perl env. */

	DBIc_set(imp_dbh, DBIcf_AutoCommit, &sv_yes);
	if (!(sv && SvROK(sv)))
		goto do_connect;

	HV *hv = (HV *) SvRV(sv);
	SV **svp;
	STRLEN lna;

#define OPTION_IF(a) ((svp = hv_fetch(hv,a,sizeof(a), FALSE)) &&\
*svp && SvTRUE(*svp))

	if (OPTION_IF("mysql_compression")) {
		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP,
				      "imp_dbh->mysql_dr_connect: Enabling"
				      " compression.\n");
		mysql_options(sock, MYSQL_OPT_COMPRESS, NULL);
	}
	if (OPTION_IF("myql_connect_timeout")) {
		int to = SvIV(*svp);
		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP,
				      "imp_dbh->mysql_dr_connect: Setting"
				      " connect timeout (%d).\n", to);
		mysql_options(sock, MYSQL_OPT_CONNECT_TIMEOUT,
			      (const char *) &to);
	}
	if (OPTION_IF("mysql_read_default_file")) {
		char *df = SvPV(*svp, lna);
		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP,
				      "imp_dbh->mysql_dr_connect: Reading"
				      " default file %s.\n", df);
		mysql_options(sock, MYSQL_READ_DEFAULT_FILE, df);
	}
	if (OPTION_IF("mysql_read_default_group")) {
		char *gr = SvPV(*svp, lna);
		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP,
				      "imp_dbh->mysql_dr_connect: Using"
				      " default group %s.\n", gr);
		mysql_options(sock, MYSQL_READ_DEFAULT_GROUP, gr);
	} else {
		mysql_options(sock, MYSQL_READ_DEFAULT_GROUP, "dbd_mysql");
	}
	if ((svp = hv_fetch(hv, "mysql_client_found_rows", 23, FALSE))
	    && *svp) {
		if (SvTRUE(*svp)) {
			client_flag |= CLIENT_FOUND_ROWS;
		} else {
			client_flag &= ~CLIENT_FOUND_ROWS;
		}
	}

	if ((svp = hv_fetch(hv, "mysql_protocol41", 16, FALSE)) && *svp) {
		if (SvTRUE(*svp)) {
			imp_dbh->has_protocol41 = TRUE;
			client_flag |= CLIENT_PROTOCOL_41;
		} else {
			imp_dbh->has_protocol41 = FALSE;
			client_flag &= ~CLIENT_PROTOCOL_41;
		}
		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP,
				      "imp_dbh->has_protocol41: %d",
				      imp_dbh->has_protocol41);
	}

	if ((svp = hv_fetch(hv, "mysql_server_prepare", 20, FALSE)) && *svp) {
		if (SvTRUE(*svp)) {
			imp_dbh->real_prepare = TRUE;
		} else {
			imp_dbh->real_prepare = FALSE;
		}
		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP,
				      "imp_dbh->real_prepare: %d",
				      imp_dbh->real_prepare);
	}

#if defined(DBD_MYSQL_WITH_SSL)   && \
    (defined(CLIENT_SSL) || (MYSQL_VERSION_ID >= 40000))

#define DECODE_OPTION(a,b) \
if ((svp = hv_fetch(hv,a, sizeof(a), FALSE)) && *svp) \
	b = SvPV(*svp, lna);
	if (OPTION_IF("mysql_ssl")) {
		char *client_key = NULL;
		char *client_cert = NULL;
		char *ca_file = NULL;
		char *ca_path = NULL;
		char *cipher = NULL;
		STRLEN lna;

		DECODE_OPTION("mysql_ssl_client_key", client_key);
		DECODE_OPTION("mysql_ssl_client_cert", client_cert);
		DECODE_OPTION("mysql_ssl_ca_file", ca_file);
		DECODE_OPTION("mysql_ssl_ca_path", ca_path);
		DECODE_OPTION("mysql_ssl_cipher", cipher);
		mysql_ssl_set(sock, client_key, client_cert, ca_file,
			      ca_path, cipher);
		client_flag |= CLIENT_SSL;
	}
#endif
#if (MYSQL_VERSION_ID >= 32349)
	/*
	 * MySQL 3.23.49 disables LOAD DATA LOCAL by default. Use
	 * mysql_local_infile=1 in the DSN to enable it.
	 */
	if ((svp = hv_fetch(hv, "mysql_local_infile", 18, FALSE)) && *svp) {
		unsigned int flag = SvTRUE(*svp);
		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP,
				      "imp_dbh->mysql_dr_connect: Using"
				      " local infile %u.\n", flag);
		mysql_options(sock, MYSQL_OPT_LOCAL_INFILE,
			      (const char *) &flag);
	}
#endif

      do_connect:
	if (dbis->debug >= 2)
		PerlIO_printf(DBILOGFP,
			      "imp_dbh->mysql_dr_connect: client_flags = %d\n",
			      client_flag);
	result =
	    mysql_real_connect(sock, host, user, password, dbname,
			       portNr, unixSocket, client_flag);
	if (dbis->debug >= 2)
		PerlIO_printf(DBILOGFP, "imp_dbh->mysql_dr_connect: <-");

	/* we turn off Mysql's auto reconnect and handle re-connecting ourselves
	 * so that we can keep track of when this happens.
	 */
	sock->reconnect = 0;
	return result;
}

/***************************************************************************
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
		if (!len)
			host = NULL;
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
		if (!len)
			user = NULL;
	} else {
		user = NULL;
	}
	if ((svp = hv_fetch(hv, "password", 8, FALSE))) {
		password = SvPV(*svp, len);
		if (!len)
			password = NULL;
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
 *  Name:    dbd_db_login
 *  Purpose: Called for connecting to a database and logging in.
 *  Input:   dbh - database handle being initialized
 *           imp_dbh - drivers private database handle data
 *           dbname - the database we want to log into; may be like
 *               "dbname:host" or "dbname:host:port"
 *           user - user name to connect as
 *           password - passwort to connect with
 *  Returns: TRUE for success, FALSE otherwise; do_error has already
 *           been called in the latter case
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
 *  Name:    dbd_db_commit
 *           dbd_db_rollback
 *  Purpose: You guess what they should do. mSQL doesn't support
 *           transactions, so we stub commit to return OK
 *           and rollback to return ERROR in any case.
 *  Input:   dbh - database handle being commited or rolled back
 *           imp_dbh - drivers private database handle data
 *  Returns: TRUE for success, FALSE otherwise; do_error has already
 *           been called in the latter case
 **************************************************************************/

int dbd_db_commit(SV * dbh, imp_dbh_t * imp_dbh)
{
	if (DBIc_has(imp_dbh, DBIcf_AutoCommit)) {
		do_warn(dbh, TX_ERR_AUTOCOMMIT,
			"Commmit ineffective while AutoCommit is on");
		return TRUE;
	}

	if (!imp_dbh->has_protocol41) {
		if (mysql_real_query(&imp_dbh->mysql,"COMMIT",6) != 0) {
			do_error(dbh, mysql_errno(&imp_dbh->mysql),
				 mysql_error(&imp_dbh->mysql));
			return FALSE;
		}
	} else {
#if MYSQL_VERSION_ID >=40101
		if (mysql_commit(&imp_dbh->mysql)) {
			do_error(dbh, mysql_errno(&imp_dbh->mysql),
				 mysql_error(&imp_dbh->mysql));
			return FALSE;
		}
#else
		die("DBD::mysql Bug");
#endif
	}
	if (imp_dbh->begun_work) {
		imp_dbh->begun_work = FALSE;
		set_autocommit(dbh, imp_dbh, FALSE, FALSE);
	}
	return TRUE;
}

int dbd_db_rollback(SV * dbh, imp_dbh_t * imp_dbh)
{
	int ret = 1;
	/* croak, if not in AutoCommit mode */
	if (DBIc_has(imp_dbh, DBIcf_AutoCommit)) {
		do_warn(dbh, TX_ERR_AUTOCOMMIT,
			"Rollback ineffective while AutoCommit is on");
		return FALSE;
	}

	if (imp_dbh->has_protocol41) {
#if MYSQL_VERSION_ID >=40101
		ret = mysql_rollback(&imp_dbh->mysql);
#else
		die("DBD::mysql Bug");
#endif
	} else
		ret = mysql_real_query(&imp_dbh->mysql, "ROLLBACK", 8);

	if (ret) {
		do_error(dbh, mysql_errno(&imp_dbh->mysql),
			 mysql_error(&imp_dbh->mysql));
		return FALSE;
	}
	if (imp_dbh->begun_work) {
		imp_dbh->begun_work = 0;
		set_autocommit(dbh, imp_dbh, FALSE, FALSE);
	}	
}


/***************************************************************************
 *  Name:    dbd_db_disconnect
 *  Purpose: Disconnect a database handle from its database
 *  Input:   dbh - database handle being disconnected
 *           imp_dbh - drivers private database handle data
 *  Returns: TRUE for success, FALSE otherwise; do_error has already
 *           been called in the latter case
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
 *  Name:    dbd_discon_all
 *  Purpose: Disconnect all database handles at shutdown time
 *  Input:   dbh - database handle being disconnected
 *           imp_dbh - drivers private database handle data
 *  Returns: TRUE for success, FALSE otherwise; do_error has already
 *           been called in the latter case
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
 *  Name:    dbd_db_destroy
 *  Purpose: Our part of the dbh destructor
 *  Input:   dbh - database handle being destroyed
 *           imp_dbh - drivers private database handle data
 *  Returns: Nothing
 **************************************************************************/

void dbd_db_destroy(SV * dbh, imp_dbh_t * imp_dbh)
{

	/*
	 *  Being on the safe side never hurts ...
	 */
	if (DBIc_ACTIVE(imp_dbh)) {
		mysql_db_rollback(dbh, imp_dbh);
		dbd_db_disconnect(dbh, imp_dbh);
	}

	/*
	 *  Tell DBI, that dbh->destroy must no longer be called
	 */
	DBIc_off(imp_dbh, DBIcf_IMPSET);
}


static bool set_autocommit(SV *dbh, imp_dbh_t *imp_dbh, bool do_auto_commit,
	bool commit_first)
{
	int oldval = DBIc_has(imp_dbh, DBIcf_AutoCommit);

	/* if setting AutoCommit on ... */
	if (!oldval && do_auto_commit) {
		/*  Need to issue a commit before entering AutoCommit */
		if (imp_dbh->begun_work)
			croak("begun work bug autocommit was tunred off "
			   "without commit/rollback");

		if (commit_first && dbd_mysql_commit(imp_dbh) != 0) {
			do_error(dbh, TX_ERR_COMMIT, "COMMIT failed");
			return FALSE;
		}
		if (dbd_mysql_autocommit_on(imp_dbh)) {
			do_error(dbh, TX_ERR_AUTOCOMMIT,
				 "Turning on AutoCommit failed");
			return FALSE;
		}
		DBIc_set(imp_dbh, DBIcf_AutoCommit, TRUE);
	} else if (!do_auto_commit && oldval) {
		if (dbd_mysql_autocommit_off(imp_dbh)) {
			do_error(dbh, TX_ERR_AUTOCOMMIT,
				 "Turning off AutoCommit failed");
			return FALSE;
		}
		DBIc_set(imp_dbh, DBIcf_AutoCommit, FALSE);
	}
}


/***************************************************************************
 *  Name:    dbd_db_STORE_attrib
 *  Purpose: Function for storing dbh attributes; we currently support
 *           just nothing. :-)
 *  Input:   dbh - database handle being modified
 *           imp_dbh - drivers private database handle data
 *           keysv - the attribute name
 *           valuesv - the attribute value
 *  Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

int dbd_db_STORE_attrib(SV * dbh, imp_dbh_t * imp_dbh, SV * keysv,
			SV * valuesv)
{
	STRLEN kl;
	char *key = SvPV(keysv, kl);
	SV *cachesv = Nullsv;
	bool bool_value = SvTRUE(valuesv);

	if (DECODE_KEY("AutoCommit")) {
		/* TODO Set return value */
		set_autocommit(dbh, imp_dbh, bool_value, TRUE);
	} else if (DECODE_KEY("BegunWork")) {
		imp_dbh->begun_work = 1;
	} else if (DECODE_KEY("mysql_auto_reconnect")) {
		/*XXX: Does DBI handle the magic ? */
		imp_dbh->auto_reconnect = bool_value;
	} else if (DECODE_KEY("mysql_server_prepare")) {
		imp_dbh->real_prepare = SvTRUE(valuesv);
	} else {
		do_error(dbh, TX_ERR_AUTOCOMMIT, "Unknown Key");
		return FALSE;
	}

	return TRUE;
}


/***************************************************************************
 *  Name:    dbd_db_FETCH_attrib
 *  Purpose: Function for fetching dbh attributes
 *  Input:   dbh - database handle being queried
 *           imp_dbh - drivers private database handle data
 *           keysv - the attribute name
 *  Returns: An SV*, if sucessfull; NULL otherwise
 *  Notes:   Do not forget to call sv_2mortal in the former case!
 **************************************************************************/


static SV *my_ulonglong2str(my_ulonglong val)
{
	if (0 == val) {
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


	if (DECODE_KEY("AutoCommit"))
		return sv_2mortal(boolSV(DBIc_has (imp_dbh, DBIcf_AutoCommit)));

	if (0 == strncmp(key, "mysql_", 6)) {
		fine_key = key;
		key = key + 6;
		kl = kl - 6;
	}

	if (DECODE_KEY("auto_reconnect")) {
		result = sv_2mortal(newSViv(imp_dbh->auto_reconnect));
	} else if (DECODE_KEY("errno")) {
		result = sv_2mortal(newSViv((IV)
					    mysql_errno(&imp_dbh->mysql)));
	} else if (DECODE_KEY("error")) {
		const char *msg = mysql_error(&imp_dbh->mysql);
		result = sv_2mortal(newSVpv(msg, strlen(msg)));
	} else if (DECODE_KEY("errmsg")) {
		/* Obsolete, as of 2.09! */
		const char *msg = mysql_error(&imp_dbh->mysql);
		result = sv_2mortal(newSVpv(msg, strlen(msg)));
	} else if (DECODE_KEY("dbd_stats")) {
		HV *hv = newHV();
		hv_store(hv, "auto_reconnects_ok",
			 sizeof("auto_reconnects_ok"),
			 newSViv(imp_dbh->stats.auto_reconnects_ok), 0);
		hv_store(hv, "auto_reconnects_failed",
			 sizeof("auto_reconnects_failed"),
			 newSViv(imp_dbh->stats.auto_reconnects_failed),
			 0);

		result = (newRV_noinc((SV *) hv));

	} else if (DECODE_KEY("hostinfo")) {
		const char *hostinfo =
		    mysql_get_host_info(&imp_dbh->mysql);
		result = hostinfo ?
		    sv_2mortal(newSVpv(hostinfo, strlen(hostinfo)))
		    : &sv_undef;

	} else if (DECODE_KEY("info")) {
		const char *info = mysql_info(&imp_dbh->mysql);
		result = info ? sv_2mortal(newSVpv(info, strlen(info)))
		    : &sv_undef;
	} else if (DECODE_KEY("insertid")) {
		/* We cannot return an IV, because the insertid is a long. */
		result = sv_2mortal(my_ulonglong2str
				    (mysql_insert_id(&imp_dbh->mysql)));
	} else if (DECODE_KEY("protoinfo")) {
		result = sv_2mortal(newSViv
				    (mysql_get_proto_info
				     (&imp_dbh->mysql)));
	} else if (DECODE_KEY("serverinfo")) {
		const char *serverinfo =
		    mysql_get_server_info(&imp_dbh->mysql);
		result =
		    serverinfo ?
		    sv_2mortal(newSVpv(serverinfo, strlen(serverinfo)))
		    : &sv_undef;

	} else if (DECODE_KEY("sock")) {
		result = sv_2mortal(newSViv((IV) & imp_dbh->mysql));

	} else if (DECODE_KEY("sockfd")) {
		result = sv_2mortal(newSViv((IV) imp_dbh->mysql.net.fd));
	} else if (DECODE_KEY("stat")) {
		const char *stats = mysql_stat(&imp_dbh->mysql);
		result = stats ?
		    sv_2mortal(newSVpv(stats, strlen(stats))) : &sv_undef;
	} else if (DECODE_KEY("stats")) {
		/* Obsolete, as of 2.09 */
		const char *stats = mysql_stat(&imp_dbh->mysql);
		result = stats ?
		    sv_2mortal(newSVpv(stats, strlen(stats))) : &sv_undef;
	} else if (DECODE_KEY("server_prepare")) {
		result = sv_2mortal(newSViv((IV) imp_dbh->real_prepare));
	} else if (DECODE_KEY("protocol41")) {
		result = sv_2mortal(newSViv((IV) imp_dbh->real_prepare));
	} else if (DECODE_KEY("thread_id")) {
		result =
		    sv_2mortal(newSViv(mysql_thread_id(&imp_dbh->mysql)));
	}

	if (NULL == result) {
		return Nullsv;
	}
	if (!fine_key) {
		/* Obsolete, as of 2.09 */
	}
	return result;
}


/***************************************************************************
 *  Name:    dbd_st_prepare
 *  Purpose: Called for preparing an SQL statement; our part of the
 *           statement handle constructor
 *  Input:   sth - statement handle being initialized
 *           imp_sth - drivers private statement handle data
 *           statement - pointer to string with SQL statement
 *           attribs - statement attributes, currently not in use
 *  Returns: TRUE for success, FALSE otherwise; do_error will
 *           be called in the latter case
 **************************************************************************/

int
dbd_st_prepare(SV * sth, imp_sth_t * imp_sth, char *statement, SV * attribs)
{
	/* Initialize our data */
	int i;
	SV **svp;
	D_imp_dbh_from_sth;

	unsigned int phc;
	STRLEN stmt_len;

	/* clear imp_sth, but don't clear first struct dbih_stc_t */
	memset((((char *)imp_sth)+sizeof(dbih_stc_t)), 0,
	    sizeof(imp_sth_t)-sizeof(dbih_stc_t));

	prescan_stmt(statement, &stmt_len, &phc);
	stmt_len += calc_ph_space(phc) + 1;
	Newc(908, imp_sth->statement, stmt_len, char, char);

	/* +1 so we can use a 1 based idx (placeholders start from 1) */
	if (phc) {
		Newc(0, imp_sth->place_holders, phc + 1, phs_t **, phs_t *);
		Newz(908, imp_sth->bind, phc+1, MYSQL_BIND);
	}

	/* rewrite and set up placeholders (imp_sth->phc, imp_sth->params &c.*/
	rewrite_placeholders(imp_sth, statement);
	assert(strlen(imp_sth->statement) + 1 <= stmt_len);

	svp = DBD_ATTRIB_GET_SVP(attribs, "mysql_use_result", 16);
	imp_sth->use_mysql_use_result = svp && SvTRUE(*svp);


	imp_sth->real_prepare = imp_dbh->real_prepare;
	if (svp = DBD_ATTRIB_GET_SVP(attribs, "mysql_server_prepare", 20))
		imp_sth->real_prepare = SvTRUE(*svp);

	if (!imp_dbh->has_protocol41 && imp_sth->real_prepare) {
		warn("Cannot use server prepare when not using Protocol 41");
		imp_sth->real_prepare = FALSE;
	}
	if (dbis->debug >= 2) {
		PerlIO_printf(DBILOGFP, "Setting mysql_use_result to %d\n",
			      imp_sth->use_mysql_use_result);
		PerlIO_printf(DBILOGFP,
			      "MYSQL_VERSION_ID %d, imp_sth->real_prepare %d\n",
			      MYSQL_VERSION_ID, imp_sth->real_prepare);
	}


	/* Bail out if we are don't need to do a real prepare as the
	   rest of this function only applies to real prepared statements */
	if (!imp_sth->real_prepare || has_limit_clause(statement) || 
	    has_list_fields(statement)) {

		imp_sth->real_prepare = FALSE;
		DBIc_IMPSET_on(imp_sth);	/* it lives */
		return 1;
	}
#if MYSQL_VERSION_ID >=40101
	/*TODO remove strlen(statement) as we should already know this*/
	imp_sth->stmt = mysql_prepare(&imp_dbh->mysql, statement,
				      strlen(statement));
#else
	croak("DBD::mysql BUG\n");
#endif
	if (!imp_sth->stmt) {
		do_error(sth, mysql_errno(&imp_dbh->mysql),
			 mysql_error(&imp_dbh->mysql));
		return 0;
	}

	assert(DBIc_NUM_PARAMS(imp_sth) == mysql_param_count(imp_sth->stmt));

	DBIc_IMPSET_on(imp_sth);

	if (DBIc_NUM_PARAMS(imp_sth) < 0)
		return 1;

	imp_sth->metadata = mysql_get_metadata(imp_sth->stmt);

	return 1;
}




/***************************************************************************
 *  Name:    mysql_st_internal_execute
 *  Purpose: Internal version for executing a statement, called both from
 *           within the "do" and the "execute" method.
 *  Inputs:  h - object handle, for storing error messages
 *           statement - query being executed
 *           attribs - statement attributes, currently ignored
 *           numParams - number of parameters being bound
 *           params - parameter array
 *           cdaPtr - where to store results, if any
 *           svsock - socket connected to the database
 **************************************************************************/

int mysql_st_internal_execute(SV * sth, imp_sth_t * imp_sth)
{
	STRLEN slen;
	// char *sbuf = SvPV(statement, slen); 
	// char *salloc = ParseParam(svsock, sbuf, &slen, params, numParams);
	char *statement;
	int ret = -2;
	int max_len = 0;
	D_imp_dbh_from_sth;
	STRLEN stmt_len;
	char *sbuf;


	if (!imp_sth->statement) {
		/* not prepared */
		return 0;
	}


	slen = max_len = strlen(imp_sth->statement);
	sbuf = imp_sth->statement;

	if (slen >= 11 && !strncasecmp(sbuf, "listfields ", 11)) {
		char *table;

		slen -= 10;
		sbuf += 10;
		while (slen && isspace(*sbuf)) {
			--slen;
			++sbuf;
		}

		if (!slen) {
			do_error(sth, JW_ERR_QUERY, "Missing table name");
			return -2;
		}
		if (!(table = malloc(slen + 1))) {
			do_error(sth, JW_ERR_MEM, "Out of memory");
			return -2;
		}

		strncpy(table, sbuf, slen);
		sbuf = table;

		while (slen && !isspace(*sbuf)) {
			--slen;
			++sbuf;
		}
		*sbuf++ = '\0';

		imp_sth->cda =
		    mysql_list_fields(&imp_dbh->mysql, table, NULL);
		free(table);

		if (!imp_sth->cda) {
			do_error(sth, mysql_errno(&imp_dbh->mysql),
				 mysql_error(&imp_dbh->mysql));
			return -2;
		}

		return 0;
	}
	if ((int) DBIc_NUM_PARAMS(imp_sth) > 0) {
		/* How much do we need to malloc to hold resultant string */
		HV *hv = imp_sth->all_params_hv;
		SV *sv;
		char *key;
		I32 retlen;
		hv_iterinit(hv);
		while ((sv = hv_iternextsv(hv, &key, &retlen)) != NULL) {
			if (sv != &sv_undef) {
				phs_t *phs_tpl =
				    (phs_t *) (void *) SvPVX(sv);
				if (!phs_tpl->is_bound) {
					do_error(sth, -2,
						 "Execute called with unbound placeholder");
					return -2;
				}
				max_len += phs_tpl->quoted_len * phs_tpl->count;
			}
		}
		Newc(0, statement, max_len, char, char);

		/* scan statement for placeholders and replace with values */
		if ((ret = rewrite_execute_stmt(sth, imp_sth, statement)) < 0)
			return ret;
	} else {
		statement = imp_sth->statement;
	}
	assert(strlen(statement) + 1 <= max_len);

	/*TODO:  Clean up and free old result */


	if ((mysql_real_query (&imp_dbh->mysql, statement, strlen(statement)))
	    && (!mysql_db_reconnect(sth) || (mysql_real_query
		(&imp_dbh->mysql, statement, strlen(statement))))) {

		if ((int) DBIc_NUM_PARAMS(imp_sth) > 0)
			Safefree(statement);

		do_error(sth, mysql_errno(&imp_dbh->mysql),
			 mysql_error(&imp_dbh->mysql));
		return -2;
	}
	if ((int) DBIc_NUM_PARAMS(imp_sth) > 0)
		Safefree(statement);

	/* Store the result from the Query */
	imp_sth->cda = imp_sth->use_mysql_use_result ?
	    mysql_use_result(&imp_dbh->mysql) :
	    mysql_store_result(&imp_dbh->mysql);

	if (mysql_errno(&imp_dbh->mysql)) {
		do_error(sth, mysql_errno(&imp_dbh->mysql),
			 mysql_error(&imp_dbh->mysql));
	}

	if (!imp_sth->cda) {
		return mysql_affected_rows(&imp_dbh->mysql);
	} else {
		/*TODO: mysql_affect_rowsi */
		return mysql_num_rows(imp_sth->cda);
	}

}

/***************************************************************************
 *  Name:    mysql_st_internal_execute41
 *  Purpose: Internal version for executing a prepared statement, called both from
 *           within the "do" and the "execute" method.
 *           MYSQL 4.1 API           
 *  Inputs:  h - object handle, for storing error messages
 *           statement - query being executed
 *           attribs - statement attributes, currently ignored
 *           numParams - number of parameters being bound
 *           params - parameter array
 *           cdaPtr - where to store results, if any
 *           svsock - socket connected to the database
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
				 MYSQL_BIND * bind)
{
	STRLEN slen;

	if (dbis->debug >= 2) {
		PerlIO_printf(DBILOGFP,
			      "mysql_st_internal_execute41 bind size %d, numParams %d Executing Statement: %s\n",
			      sizeof(bind), numParams, SvPV(statement,
							    slen));
	}

	if (numParams > 0 ) {
		if (mysql_bind_param(stmt, bind)) {
			fprintf(stderr, "\nparam bind failed\n");
			fprintf(stderr, "\n|%d| |%s|\n",
				mysql_stmt_errno(stmt),
				mysql_stmt_error(stmt));
			do_error(h, mysql_stmt_errno(stmt),
				 mysql_stmt_error(stmt));
			return -2;
		}
	}

	if (dbis->debug >= 2) {
		PerlIO_printf(DBILOGFP,
			      "mysql_st_internal_execute41 calling "
			      "mysql_execute\n");
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
 *  Name:    dbd_st_execute
 *  Purpose: Called for preparing an SQL statement; our part of the
 *           statement handle constructor
 *  Input:   sth - statement handle being initialized
 *           imp_sth - drivers private statement handle data
 *  Returns: TRUE for success, FALSE otherwise; do_error will
 *           be called in the latter case
 **************************************************************************/

int dbd_st_execute(SV * sth, imp_sth_t * imp_sth)
{
	D_imp_dbh_from_sth;
	SV **statement;
	int i;
	int ret;

#if defined (dTHR)
	dTHR;
#endif
	char *stmt;

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

	if (imp_sth->real_prepare) {
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
							       bind);
	} else {
		imp_sth->row_num = mysql_st_internal_execute(sth, imp_sth);
	}

	if (imp_sth->row_num != -2) {
		if (!imp_sth->cda) {
			imp_sth->insertid = mysql_insert_id(&imp_dbh->mysql);
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
 *  Name:    dbd_describe
 *  Purpose: Called from within the fetch method to describe the result
 *  Input:   sth - statement handle being initialized
 *           imp_sth - our part of the statement handle, there's no
 *               need for supplying both; Tim just doesn't remove it
 *  Returns: TRUE for success, FALSE otherwise; do_error will
 *           be called in the latter case
 **************************************************************************/

int dbd_describe(SV * sth, imp_sth_t * imp_sth)
{
	int num_fields = DBIc_NUM_FIELDS(imp_sth);
	int i;
	int col_type;
	imp_sth_fbh_t *fbh;
	MYSQL_BIND *bind;
	MYSQL_FIELD *fields;


	if (dbis->debug >= 2)
		PerlIO_printf(DBILOGFP, "** dbd_describe() **\n");

	if (imp_sth->done_desc) 
		return TRUE;

	if (!imp_sth->real_prepare) {
		imp_sth->done_desc = 1;
		return TRUE;
	}


	if (dbis->debug >= 2)
		PerlIO_printf(DBILOGFP, "** dbd_describe() num_fields %d**\n",
			num_fields);


	if (!num_fields || !imp_sth->cda) {
		//no metadata
		/*XXX Should this be an error in describe?  user tries to 
		  fetch on an insert probably should not thow some weird
		  message about not having metadata and -- error during
		  describe*/
		do_error(sth, JW_ERR_SEQUENCE,
			"no metadata information while trying describe"
			" result set");
			return 0;
	}

	/* allocate fields buffers  */
	if (num_fields) {
	 	Newz(908, imp_sth->fbh, num_fields, imp_sth_fbh_t);
		Newz(908,imp_sth-> buffer, num_fields, MYSQL_BIND);
	}

	fields = mysql_fetch_fields(imp_sth->cda);

	for (fbh = imp_sth->fbh, bind =
	     (MYSQL_BIND *) imp_sth->buffer, i = 0; i < num_fields;
	     i++, fbh++, bind++) {
		// get the column type 
		col_type =
		    fields ? fields[i].type : MYSQL_TYPE_STRING;
		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP, "col type %d\n", col_type);

		switch (col_type) {
		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_DOUBLE:
		case MYSQL_TYPE_FLOAT:
			bind->buffer_type = MYSQL_TYPE_DOUBLE;
			bind->buffer_length = fields[i].length;
			bind->length = &(fbh->length);
			bind->is_null = &(fbh->is_null);
			Newz(908, fbh->data, fields[i].length, char);
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
			Newz(908, fbh->data, fields[i].length, char);
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
			Newz(908, fbh->data, fields[i].length, char);
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
			Newz(908, fbh->data, fields[i].length, char);
			bind->buffer = (char *) fbh->data;

		default:
			// Create string type here
			bind->buffer_type = MYSQL_TYPE_STRING;
			bind->buffer_length = fields[i].length;
			bind->length = &(fbh->length);
			bind->is_null = &(fbh->is_null);
			Newz(908, fbh->data, fields[i].length, char);
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


/***************************************************************************
 *  Name:    dbd_st_fetch
 *  Purpose: Called for fetching a result row
 *  Input:   sth - statement handle being initialized
 *           imp_sth - drivers private statement handle data
 *  Returns: array of columns; the array is allocated by DBI via
 *           DBIS->get_fbav(imp_sth), even the values of the array
 *           are prepared, we just need to modify them appropriately
 **************************************************************************/

AV *dbd_st_fetch(SV * sth, imp_sth_t * imp_sth)
{
	int num_fields;
	int ChopBlanks;
	int i;
	AV *av;
	MYSQL_ROW cols;
	unsigned long *lengths;

	if (imp_sth->real_prepare) {
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
	if (imp_sth->real_prepare) {
		if (dbis->debug >= 2)
			PerlIO_printf(DBILOGFP,
				      "dbd_st_fetch calling mysql_fetch\n");

		if ((rc = mysql_fetch(imp_sth->stmt))) {

			if (1 == rc) {
				do_error(sth,
					 mysql_stmt_errno(imp_sth->stmt),
					 mysql_stmt_error(imp_sth->stmt));
			}

			if (100 == rc) {
				//Update row_num to affected_rows value 
				imp_sth->row_num = (long)
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
					while (len && ' ' == col[len - 1]) {
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
 *  Name:    dbd_st_finish
 *  Purpose: Called for freeing a mysql result
 *  Input:   sth - statement handle being finished
 *           imp_sth - drivers private statement handle data
 *  Returns: TRUE for success, FALSE otherwise; do_error() will
 *           be called in the latter case
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
	if (imp_sth->real_prepare) {
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
				if (imp_sth->fbh)
					Safefree(imp_sth->fbh);
			}
			if(imp_sth->buffer)
				Safefree(imp_sth->buffer);

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
		/* TODO: free more metadata &c.*/
	}
	DBIc_ACTIVE_off(imp_sth);
	return 1;
}


/***************************************************************************
 *  Name:    dbd_st_destroy
 *  Purpose: Our part of the statement handles destructor
 *  Input:   sth - statement handle being destroyed
 *           imp_sth - drivers private statement handle data
 *  Returns: Nothing
 **************************************************************************/

void dbd_st_destroy(SV * sth, imp_sth_t * imp_sth)
{
	int i;

	int num_fields;

	if (imp_sth->real_prepare) {
		if (imp_sth->stmt) {
			num_fields = DBIc_NUM_FIELDS(imp_sth);

#if MYSQL_VERSION_ID >=40101
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
#endif

		}
	}

	if (DBIc_NUM_PARAMS(imp_sth) > 0) {
		Safefree(imp_sth->params);
		Safefree(imp_sth->bind);
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
 *  Name:    dbd_st_STORE_attrib
 *  Purpose: Modifies a statement handles attributes; we currently
 *           support just nothing
 *  Input:   sth - statement handle being destroyed
 *           imp_sth - drivers private statement handle data
 *           keysv - attribute name
 *           valuesv - attribute value
 *  Returns: TRUE for success, FALSE otrherwise; do_error will
 *           be called in the latter case
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
 *  Name:    dbd_st_FETCH_internal
 *  Purpose: Retrieves a statement handles array attributes; we use
 *           a separate function, because creating the array
 *           attributes shares much code and it aids in supporting
 *           enhanced features like caching.
 *  Input:   sth - statement handle; may even be a database handle,
 *               in which case this will be used for storing error
 *               messages only. This is only valid, if cacheit (the
 *               last argument) is set to TRUE.
 *           what - internal attribute number
 *           res - pointer to a DBMS result
 *           cacheit - TRUE, if results may be cached in the sth.
 *  Returns: RV pointing to result array in case of success, NULL
 *           otherwise; do_error has already been called in the latter
 *           case.
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

	if (Nullav == av) {
		return &sv_undef;
	}
	return sv_2mortal(newRV_inc((SV *) av));
}


/***************************************************************************
 *  Name:    dbd_st_FETCH_attrib
 *  Purpose: Retrieves a statement handles attributes
 *  Input:   sth - statement handle being destroyed
 *           imp_sth - drivers private statement handle data
 *           keysv - attribute name
 *  Returns: NULL for an unknown attribute, "undef" for error,
 *           attribute value otherwise.
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

	if (DECODE_KEY("ParamValues")) {
		HV *returnHV;
		SV *valueSV;
		SV *sv;
		I32 keylen;
		returnHV = newHV();

		while ((sv = hv_iternextsv(imp_sth->all_params_hv, &key,
			&keylen)) != NULL) {

			phs_t *phs = (phs_t *) (void *) SvPVX(sv);
			if (phs->quoted)
				valueSV =
				    newSVpv(phs->quoted, phs->quoted_len);
			else
				valueSV = &sv_undef;

			hv_store(returnHV, key, keylen, valueSV, 0);

		}
		return sv_2mortal(newRV((SV *) returnHV));
	}


	if (DECODE_KEY("NAME")) {
		retsv = ST_FETCH_AV(AV_ATTRIB_NAME);
	} else if (DECODE_KEY("NULLABLE")) {
		retsv = ST_FETCH_AV(AV_ATTRIB_NULLABLE);
	} else if (DECODE_KEY("PRECISION")) {
		retsv = ST_FETCH_AV(AV_ATTRIB_PRECISION);
	} else if (DECODE_KEY("SCALE")) {
		retsv = ST_FETCH_AV(AV_ATTRIB_SCALE);
	} else if (DECODE_KEY("TYPE")) {
		retsv = ST_FETCH_AV(AV_ATTRIB_SQL_TYPE);
	} else if (DECODE_KEY("mysql_type")) {
		retsv = ST_FETCH_AV(AV_ATTRIB_TYPE);
	} else if (DECODE_KEY("mysql_table")) {
		retsv = ST_FETCH_AV(AV_ATTRIB_TABLE);
	} else if (DECODE_KEY("mysql_is_key")) {
		retsv = ST_FETCH_AV(AV_ATTRIB_IS_KEY);
	} else if (DECODE_KEY("mysql_is_num")) {
		retsv = ST_FETCH_AV(AV_ATTRIB_IS_NUM);
	} else if (DECODE_KEY("mysql_length")) {
		retsv = ST_FETCH_AV(AV_ATTRIB_LENGTH);
	} else if (DECODE_KEY("mysql_result")) {
		retsv = sv_2mortal(newSViv((IV) imp_sth->cda));
	} else if (DECODE_KEY("mysql_is_blob")) {
		retsv = ST_FETCH_AV(AV_ATTRIB_IS_BLOB);
	} else if (DECODE_KEY("mysql_insertid")) {
		/* We cannot return an IV, because the insertid is a long.  */
		return sv_2mortal(my_ulonglong2str(imp_sth->insertid));
	} else if (DECODE_KEY("mysql_type_name")) {
		retsv = ST_FETCH_AV(AV_ATTRIB_TYPE_NAME);
	} else if (DECODE_KEY("mysql_is_pri_key")) {
		retsv = ST_FETCH_AV(AV_ATTRIB_IS_PRI_KEY);
	} else if (DECODE_KEY("mysql_max_length")) {
		retsv = ST_FETCH_AV(AV_ATTRIB_MAX_LENGTH);
	} else if (DECODE_KEY("mysql_use_result")) {
		retsv = boolSV(imp_sth->use_mysql_use_result);
	} else if (DECODE_KEY("mysql_server_prepare")) {
		retsv = sv_2mortal(newSViv((IV) imp_sth->real_prepare));
	} else if (DECODE_KEY("mysql_is_auto_increment")) {
		retsv = ST_FETCH_AV(AV_ATTRIB_IS_AUTO_INCREMENT);
	}

	return retsv;
}


/***************************************************************************
 *  Name:    dbd_st_blob_read
 *  Purpose: Used for blob reads if the statement handles "LongTruncOk"
 *           attribute (currently not supported by DBD::mysql)
 *  Input:   SV* - statement handle from which a blob will be fetched
 *           imp_sth - drivers private statement handle data
 *           field - field number of the blob (note, that a row may
 *               contain more than one blob)
 *           offset - the offset of the field, where to start reading
 *           len - maximum number of bytes to read
 *           destrv - RV* that tells us where to store
 *           destoffset - destination offset
 *  Returns: TRUE for success, FALSE otrherwise; do_error will
 *           be called in the latter case
 **************************************************************************/

int dbd_st_blob_read(SV * sth,
		     imp_sth_t * imp_sth,
		     int field,
		     long offset, long len, SV * destrv, long destoffset)
{
	return FALSE;
}



static phs_t * retrieve_placeholder (imp_sth_t *imp_sth, SV *ph_namesv)
{
	STRLEN name_len;
	char *name = Nullch;
	char namebuf[30];
	phs_t *phs;
	SV **phs_svp;

	if (SvGMAGICAL(ph_namesv))
		mg_get(ph_namesv);
	if (SvNIOKp(ph_namesv) || (name && isDIGIT(name[0]))) {
		snprintf(namebuf,sizeof(namebuf),"$%d", (int) SvIV(ph_namesv));
		name = namebuf;
		name_len = strlen(name);
	} else if (SvOKp(ph_namesv))
		name = SvPV(ph_namesv, name_len);
	assert(name != Nullch);

	/* get the place holder */
	phs_svp = hv_fetch(imp_sth->all_params_hv, name, name_len, 0);
	if (NULL == phs_svp) {
		croak("Can't bind unknown placeholder '%s' (%s)",
		      name, neatsvpv(ph_namesv, 0));
	}
	phs = (phs_t *) (void *) SvPVX(*phs_svp);
	return phs;
}


/***************************************************************************
 *  Name:    dbd_bind_ph
 *  Purpose: Binds a statement value to a parameter
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
 *  Returns: TRUE for success, FALSE otherwise
 **************************************************************************/

int dbd_bind_ph(SV * sth, imp_sth_t * imp_sth, SV * ph_namesv, SV * newvalue,
		IV sql_type, SV * attribs, int is_inout, IV maxlen)
{
	STRLEN slen;
	SV **svp;
	phs_t *phs;
	sql_type_info_t *sql_type_info;
	D_imp_dbh_from_sth;

	if (is_inout)
		croak("bind_inout not supported by this driver");

	phs = retrieve_placeholder(imp_sth, ph_namesv);

	if (phs->is_bound && sql_type != 0 && phs->ftype != sql_type) {
		croak("Can't change TYPE of param: %s from %d to %d after"
		      " initial bind", phs->name, phs->ftype, sql_type);
	} else {
		phs->ftype = sql_type ? sql_type : SQL_VARCHAR;
	}

	if (imp_sth->real_prepare && phs->quoted)
		Safefree(phs->quoted);

	if (!SvOK(newvalue)) {
		if (imp_sth->real_prepare)
			phs->bind->buffer = SvPV(newvalue, slen);
		else {
			New(908, phs->quoted, sizeof("NULL"), char);
			strcpy(phs->quoted, "NULL");
			phs->quoted_len = strlen(phs->quoted);
		}
	} else {
		sql_type_info_t *type_info;
		if (!(type_info = native2sql(FIELD_TYPE_VAR_STRING)))
			croak("Default field type is bad. DBD::mysql bug");

		if (imp_sth->real_prepare) {
			phs->bind->buffer = SvPV(newvalue, slen);
			/* Should be here max value for this param? */
			phs->bind->buffer_length = slen;
		} else
			phs->quoted = type_info->quote(imp_dbh->mysql, 
			    newvalue, &phs->quoted_len);

	}
	phs->bind->buffer_type = MYSQL_TYPE_VAR_STRING;
	phs->is_bound = 1;
	return 1;
}


/***************************************************************************
 *  Name:    mysql_db_reconnect
 *  Purpose: If the server has disconnected, try to reconnect.
 *  Input:   h - database or statement handle
 *  Returns: TRUE for success, FALSE otherwise
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

	/* Other error that we don't handle */
	if (mysql_errno(&imp_dbh->mysql) != CR_SERVER_GONE_ERROR)
		return FALSE;

	/* We never reconnect if AutoCommit is turned off.  Otherwise we might
	   get an inconsistent transaction state.  */
	if (!DBIc_has(imp_dbh, DBIcf_AutoCommit) || !imp_dbh->auto_reconnect)
		return FALSE;

	/* _MyLogin will blow away imp_dbh->mysql so we save a copy of
	   imp_dbh->mysql and put it back where it belongs if the reconnect
	   fail.  Think server is down & reconnect fails but the application 
	   eval{}s the execute, so next time $dbh->quote() gets called, instant
	   SIGSEGV!  */
	save_socket = imp_dbh->mysql;
	memcpy(&save_socket, &imp_dbh->mysql, sizeof(save_socket));
	memset(&imp_dbh->mysql, 0, sizeof(imp_dbh->mysql));

	if (!_MyLogin(imp_dbh)) {
		do_error(h, mysql_errno(&imp_dbh->mysql),
			 mysql_error(&imp_dbh->mysql));
		memcpy(&imp_dbh->mysql, &save_socket, sizeof(save_socket));
		++imp_dbh->stats.auto_reconnects_failed;
		return FALSE;
	}

	mysql_close(&save_socket); /* TODO: testthis */
	++imp_dbh->stats.auto_reconnects_ok;
	return TRUE;
}


/***************************************************************************
 *  Name:    dbd_db_type_info_all
 *  Purpose: Implements $dbh->type_info_all
 *  Input:   dbh - database handle
 *           imp_sth - drivers private database handle data
 *  Returns: RV to AV of types
 **************************************************************************/

AV *dbd_db_type_info_all(SV * dbh, imp_dbh_t * imp_dbh)
{
	AV *array;
	array = build_type_info_all();
	return array;
}


SV *dbd_db_quote(SV * dbh, SV * str, SV * type_sv)
{
	D_imp_dbh(dbh);
	SV *result;
	char *quoted;
	STRLEN len;
	IV type;
	const sql_type_info_t *type_info;

	if (SvGMAGICAL(str))
		mg_get(str);

	if (!SvOK(str))
		return result = newSVpv("NULL", 4);


	if (type_sv && SvOK(type_sv))
		type = SvIV(type_sv);
	else
		type = SQL_VARCHAR;

	type_info = sql_type_data(type);

	if (!type_info)
		croak("Unknown type %d", type);	/*TODO: Default to varchar ?? */


	quoted = type_info->quote(imp_dbh->mysql, str, &len);
	result = newSVpvn(quoted, len);	/*XXX can we do this w/o the copy? */

	return result;
}
