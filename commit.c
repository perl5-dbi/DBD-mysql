#include "dbdimp.h"



#define ON 1
#define OFF 0

int dbd_mysql_set_autocommit(imp_dbh_t *imp_dbh, bool autocommit)
{
	char *query;
#if MYSQL_VERSION_ID >40101
	if (!imp_dbh->has_protocol_41)
		return mysql_autocommit(&imp_dbh, autocommit);
#endif
	query = (autocommit) ? "SET AUTOCOMMIT=1" : "SET AUTOCOMMIT=0";
	return mysql_real_query(&imp_dbh->mysql, query, strlen(query));
}


int dbd_mysql_autocommit_on(imp_dbh_t *imp_dbh)
{
	return dbd_mysql_set_autocommit(imp_dbh, ON);
}


int dbd_mysql_autocommit_off(imp_dbh_t *imp_dbh)
{
	return dbd_mysql_set_autocommit(imp_dbh, OFF);
}




int dbd_mysql_commit(imp_dbh_t *imp_dbh)
{

#if MYSQL_VERSION_ID >40101
	if (!imp_dbh->has_protocol_41)
		return mysql_commit(&imp_dbh);
#endif
	return	mysql_real_query(&imp_dbh->mysql, 
		"COMMIT", sizeof("COMMIT")
	);
}

