#include "mysql.h"
#include "type_info.h"
#include <assert.h>

char *
null_quote(mysql, string_sv, retlen)
	MYSQL mysql;
	SV *string_sv;
	size_t *retlen;
{
	STRLEN len;
	char *result;
	char *string;
	string = SvPV(string_sv, len);

	Newc(0,result,len+1,char, char);
	strncpy(result,string, len);
	*retlen = len;
	return result;
}


char *
quote_varchar(mysql, string_sv, retlen)
	MYSQL mysql;
	SV *string_sv;
	size_t *retlen;
{
	STRLEN len;
	size_t	outlen;
	char *result;
	char *string;


	string = SvPV(string_sv, len);
	Newc(0,result,len*2+3,char, char);
	outlen = mysql_real_escape_string(&mysql, result+1, string, len);

	/* TODO: remalloc outlen */
	*result = '\'';
	outlen++;
	*(result+outlen)='\'';
	outlen++;
	*(result+outlen)='\0';
	*retlen = outlen;
	return result;
}

char *
quote_char(mysql, string_sv, retlen)
	MYSQL mysql;
	SV *string_sv;
	size_t *retlen;
{
	STRLEN len;
	size_t	outlen;
	char *string;
	char *result;
	
	string = SvPV(string_sv, len);
	Newc(0,result,len*2+3,char, char);
	outlen = mysql_real_escape_string(&mysql, result+1, string, len);

	/* TODO: remalloc outlen */
	*result = '\'';
	outlen++;
	*(result+outlen)='\'';
	outlen++;
	*(result+outlen)='\0';
	*retlen = outlen;
	return result;
}




char *
quote_sql_binary(mysql, string, len, retlen)
	void *string;
	size_t	len;
	size_t	*retlen;
{
	char *result;
	char *dest;
	int max_len = 0, i;

	/* We are going to retun a quote_bytea() for backwards compat but
	   we warn first */
	return result;
}



char *
quote_bool(mysql, value, len, retlen) 
	void *value;
	size_t	len;
	size_t	*retlen;
{
	char *result;
	long int int_value;
	size_t	max_len=6;

	if (isDIGIT(*(char*)value)) {
 		/* For now -- will go away when quote* take SVs */
		int_value = atoi(value);
	} else {
		int_value = 42; /* Not true, not false. Just is */
	}
	Newc(0,result,max_len,char,char);

	if (0 == int_value)
		strcpy(result,"FALSE");
	else if (1 == int_value)
		strcpy(result,"TRUE");
	else
		croak("Error: Bool must be either 1 or 0");

	*retlen = strlen(result);
	assert(*retlen+1 <= max_len);

	return result;
}



char *
quote_integer(mysql, value, len, retlen) 
	void *value;
	size_t	len;
	size_t	*retlen;
{
	char *result;
	size_t	max_len=6;

	Newc(0,result,max_len,char,char);

	if (*((int*)value) == 0)
		strcpy(result,"FALSE");
	if (*((int*)value) == 1)
		strcpy(result,"TRUE");

	*retlen = strlen(result);
	assert(*retlen+1 <= max_len);

	return result;
}



void
dequote_char(string, retlen)
	char *string;
	int *retlen;
{
	/* TODO: chop_blanks if requested */
	*retlen = strlen(string);
}


void
dequote_varchar (string, retlen)
	char *string;
	int *retlen;
{
	*retlen = strlen(string);
}



void
dequote_bool (string, retlen)
	char *string;
	int *retlen;
{
	switch(*string){
		case 'f': *string = '0'; break;
		case 't': *string = '1'; break;
		default:
			croak("I do not know how to deal with %c as a bool", *string);
	}
	*retlen = 1;
}



void
null_dequote (mysql, string, retlen)
	void *string;
	size_t *retlen;
{
	*retlen = strlen(string);
}

