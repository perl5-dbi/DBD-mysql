#ifndef DBDQUOTEH
#define DBDQUOTEH
char * null_quote();
char * quote_varchar();
char * quote_char();
char * quote_sql_binary();
char * quote_bytea();
char * quote_bool() ;
char * quote_integer() ;
void dequote_char();
void dequote_varchar();
void dequote_bytea();
void dequote_sql_binary();
void dequote_bool();
void null_dequote();
#endif /*DBDQUOTEH*/
