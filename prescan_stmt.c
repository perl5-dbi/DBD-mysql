#include "dbdimp.h"
/*******************
 * pre_scan_stmt()
 * returns the length of the statement and 
 * an estimate of how many place holders it contains.
 */

void
prescan_stmt (stmt, stmt_len, place_holder_count)
	const char *stmt;
	STRLEN *stmt_len;
	unsigned int *place_holder_count;
{
	char ch;
	int length = 0;
	int phc = 0;

	while ((ch = *stmt)) {
        	if (':' == ch || '?' == ch || '$' == ch)
			++phc;
		++length;
		++stmt;
	}
	
	*stmt_len = length;
	*place_holder_count = phc;
}



/*******************
 * clc_ph_space()
 * givin a place_holder count,  retuns the 
 * string space needed to hold them.
 */

size_t
calc_ph_space (place_holder_count)
	int place_holder_count;
{
	int divisor = 10,i;
	int  digits = 2; /* 2: 1 for " " 1 for "$" eg: ' $1' */
	size_t	total_length = 0 ;

	for (i=1; i<=place_holder_count; ++i) {
		if (i%divisor == 0) {	/* this could be made more eff. */
			/* //PerlIO_printf(DBILOGFP, "    \tDigits:%i\n", digits); */
			divisor *=10;
			++digits;
		}
		total_length += digits;
	}
	return  total_length;
}



/*******************
 * is_dml()
 * givin a statement/fragment makes a guess as to whether 
 * it be a DML statement
 */

int
is_dml (stmt)
	const char *stmt;
{
	char token[7];

	/* skip any leading whitespace */
	while (*stmt && (isSPACE(*stmt) || '\n' == *stmt)  )
		++stmt;

	/* must be the first non-whitespace token */
	/* TODO: Check size of stmt */
	strncpy (token, stmt, 6);
	
	token[6] = '\0';
	/* // PerlIO_printf(DBILOGFP, "token: stmt: %s\n", token); */
	
	/* XXX: UPDATE & INSERT are broken. The (varchar) hack does not work
	   as they actually look at the field type. Until I get a fix for this
	   we don't prepare them
	  */
	if (0/*   !strcasecmp(token, "SELECT")
	    || !strcasecmp(token, "DELETE") */
	    /*|| !strcasecmp(token, "UPDATE")
	    || !strcasecmp(token, "INSERT")*/ )
	{
		/* //PerlIO_printf(DBILOGFP, "Is DML\n"); */
		return 1;
	}
	/* // PerlIO_printf(DBILOGFP, "Is not DML\n"); */
	return 0;
}




/*******************
 * is_tx_stmt()
 * decides if a statement is a tx type statement
 */

int
is_tx_stmt (stmt)
	const char *stmt;
{
	char token[10];

	/* skip any leading whitespace */
	while (*stmt && (isSPACE(*stmt) || '\n' == *stmt)  )
		++stmt;

	/* must be the first non-whitespace token */
	/* TODO: Check size of stmt */
	strncpy (token, stmt, 8);
	
	token[9] = '\0';
	/* // PerlIO_printf(DBILOGFP, "token: stmt: %s\n", token); */
	
	if (   !strncasecmp(token, "END",     4)
  	    || !strncasecmp(token, "BEGIN",   5)
	    || !strncasecmp(token, "ABORT",   5) 
	    || !strncasecmp(token, "COMMIT",  6)
	    || !strncasecmp(token, "ROLLBACK",8) )
	{
		/* //PerlIO_printf(DBILOGFP, "Is DML\n"); */
		return 1;
	}
	/* // PerlIO_printf(DBILOGFP, "Is not DML\n"); */
	return 0;
}




/*******************
 * scan_placeholders()
 * old preparse. this one takes a statement and sets up
 *  the place holder SV*
 */

void
rewrite_placeholders (imp_sth, statement)
	imp_sth_t *imp_sth;
	char *statement;

{
	phs_t phs_tpl;
	phs_t *phs;
	SV *phs_sv;
	SV **hv;
	char name_buff[20]; /* XXX XXX XXX XXX */
	char *src, *dest, *style = "\0", *laststyle = Nullch;
	int ch, namelen;
	int in_comment=0, in_literal=0;
	unsigned int phc =0;
	char *ph_name_start;

	memset(&phs_tpl, 0, sizeof(phs_tpl));


	src = statement;
	dest = imp_sth->statement;

	/* // PerlIO_printf(DBILOGFP, "HERE: stmt: %s\n", src); */
	while ((ch = *src++)) {
		if (in_comment) {
			/* SQL-style and C++-style */
			if ((in_comment == '-' || in_comment == '/') && 
			     '\n' == ch)
			{
				in_comment = '\0';

			} else if (in_comment == '*' && '*' == ch && 
			    '/' == *src) /* C style */
			{
				/* *dest++ = ch; */
 				/* avoids asterisk-slash-asterisk issues */
				ch = *src++;
				in_comment = '\0';
			}
			/* *dest++ = ch; */
			continue;
		}

		if (in_literal) {
			/* check if literal ends but keep quotes in literal */
			if (ch == in_literal) {
				int back_slashes=0;
				char *str;
				str = src-2;
				while (*(str-back_slashes) == '\\')
					++back_slashes;

				/* odd number of '\'s ? */
				if (!(back_slashes & 1)) 
					in_literal = 0;
			}
			*dest++ = ch;
			continue;
        	}

		/* Look for comments: SQL-style or C++-style or C-style */
		if (('-' == ch && '-' == *src) ||
		    ('/' == ch && '/' == *src) ||
		    ('/' == ch && '*' == *src))
		{
			in_comment = *src;
			/* We know *src & the next char are to be copied, so do 
			 it. In the case of C-style comments, it happens to
			 help us avoid slash-asterisk-slash oddities. */
			/* *dest++ = ch; */
			continue;
		}


		/* collapse whitespace */
		if ('\n' == ch) {
			*(src-1) = ' ';
			ch = ' ';
		}
		if (isSPACE(ch) && src-2 > statement && 
		    isSPACE(*(src-2))  ) 
		{ 
			continue;
		}

		/* check if no placeholders */
		if (':' != ch && '?' != ch && '$' != ch) {
			if ('\'' == ch || '"' == ch)
				in_literal = ch;
			else if ('[' == ch)  /* ignore arrays ex. foo[1:3] */
				in_literal = ']'; 
				
			*dest++ = ch;
			continue;
		}

		/* cast */
		if (':' == ch && ':'== *src) {
			*dest++ = ch;
			*dest++ = *src++;
			continue;
		}

		if (ch != '?' && !isALNUM(*src))
			continue;


		/* sprintf(dest," $%d", ++phc);*/
		snprintf(name_buff, sizeof(name_buff), "$%d", ++phc);
		sprintf(dest, "?");

		namelen = strlen(dest);
		dest += namelen;

		ph_name_start = src-1;
		if ('?' == ch) {		/* X/Open standard	    */
			ph_name_start = name_buff;
			namelen = strlen(name_buff);
			style = "?";
		} else if (isDIGIT(*src)) {	/* '(:/$)1'	*/
			namelen = 1;
			while(isDIGIT(*src)) {
				++namelen;
				++src;
			}
			style = ":1";
		} else if (isALNUM(*src)) {	/* ':foo'	*/
			namelen = 1;
			while(isALNUM(*src)){	/* includes '_'	*/
				++namelen;
				++src;
			}
			style = ":foo";
		}

		if (laststyle && style != laststyle) {
			croak("Can't mix placeholder styles (%s/%s)",
			    style,laststyle);
		}
		laststyle = style;


		if (imp_sth->all_params_hv == NULL) {
			imp_sth->all_params_hv = newHV();
		}

		/* fprintf(stderr, "phs name start:%s len: %i Index:%i\n", 
		    ph_name_start,namelen, phc); */
		
		hv =hv_fetch(imp_sth->all_params_hv,ph_name_start,namelen,0);

		if (NULL == hv) {
			phs_sv = newSV(sizeof(phs_tpl)+namelen+1);
			Zero(SvPVX(phs_sv), sizeof(phs_tpl)+namelen+1, char);
			hv_store( imp_sth->all_params_hv,
			    ph_name_start,namelen,phs_sv,0);

			 memcpy( ((phs_t*)SvPVX(phs_sv))->name,
			    ph_name_start,namelen);
			*(((phs_t*)SvPVX(phs_sv))->name+namelen+1)='\0';
		} else {
			phs_sv = *hv;
		}
		phs = (phs_t *)SvPVX(phs_sv);
		phs->count++; /* Number with this name */
		imp_sth->place_holders[phc] = phs;
		phs->bind = &imp_sth->bind[phc];
	}

	if (phc) {
		DBIc_NUM_PARAMS(imp_sth) = phc;
		/* if (dbis->debug >= 2) {
			PerlIO_printf(DBILOGFP, 
			"    dbd_preparse scanned %d"
			" placeholders\n", (int)DBIc_NUM_PARAMS(imp_sth));
		} */
	}
	*dest = '\0';
	imp_sth->phc = phc;
}




/*******************
 * build_preamble()
 * sticks the SQL needed to prepare/execute a statement
 * at the head of the statement. 
 * type: is one of PREPARE or EXECUTE
 */

void
build_preamble (statement, type, place_holder_count, prep_stmt_id)
	char *statement;
	/* const char *type; */
	int type;
	int place_holder_count;
	int prep_stmt_id;
{
	int i;
	char *keyword;

	if (1 == type)
		keyword = "PREPARE";
	else if (2 == type)
		keyword = "EXECUTE";
	else
		croak("error");


	 sprintf(statement, 
	    "%s \"DBD::ChurlPg::cached_query %i\"", keyword,  prep_stmt_id);

		/* //PerlIO_printf(DBILOGFP, "statement: %s\n", statement); */

        if (!place_holder_count) {
		statement += strlen(statement);
		if (1 == type) 
	 		memcpy(statement, " AS ",4);
		else if (2 == type)
			*statement = '\0'; /* chop off sql statement */
		else
			croak("error");
		return;
	}

	strcat(statement, " (");
	statement += strlen(statement);

	for (i =1; i <= place_holder_count; ++i) {
		if (type == 1)
			sprintf(statement, "varchar");
		if (type == 2)
			sprintf(statement, "$%i", i);

		if (place_holder_count != i)
			strcat(statement, ", ");

		statement += strlen(statement);
	}

	if (1 == type)
		memcpy(statement, ") AS ", 5); /*finish off */
	else if (2 == type)
		memcpy(statement, ")\0 ", 2); /*finish off */
	else
		croak("error");
}



/*******************
 * rewrite_execute_stmt()
 * rewrites the execute statement to include the
 * quoted parameters for the placeholders
 * 
 */

int
rewrite_execute_stmt(sth, imp_sth, output)
	SV* sth;
	imp_sth_t *imp_sth;
	char *output;
{
	const char *src, *statement;
	char *dest;
	char *end;
	char ch;
	phs_t *phs;
	unsigned long ph = 0;
	bool in_literal = 0;
	
	src = statement = imp_sth->statement;
	dest = output;
	while ((ch = *src++)) {
		if (in_literal) {
			/* check if literal ends but keep quotes in literal */
			if (ch == in_literal) {
				int back_slashes=0;
				const char *str;
				str = src-2;
				while (*(str-back_slashes) == '\\')
					++back_slashes;
				/* Odd number of '\'s ? */
				if (!(back_slashes & 1)) 
					in_literal = 0;
			}
			*dest++ = ch;
			continue;
		}

		/* check if no placeholders */
		/* if (('$' != ch) || !isDIGIT(*src)) {
			if ('\'' == ch || '"' == ch) {
				in_literal = ch;
			}
			*dest++ = ch;
			continue;
		} */
		if ('?' != ch) {
			if ('\'' == ch || '"' == ch) {
				in_literal = ch;
			}
			*dest++ = ch;
			continue;
		}

		/* ph = strtol(src, &end, 10); */
		/* src = end;*/
		ph++; /* we are a place holder so get phs */

		assert(ph <= imp_sth->phc);
		phs = imp_sth->place_holders[ph];
		if (!phs)
			croak("DBD::Pg Bug -- Invalid Placeholder");

		memcpy(dest, phs->quoted, phs->quoted_len);
		dest += phs->quoted_len;
	}
	*dest = '\0';
	
	imp_sth->stmt_len = dest-output;
	return 0;
}

int has_limit_clause(char *statement)
{
	/* This is just a quick ugly test for LIMIT statements use FBM?  */

	while (*(statement++))
		if (!strncasecmp(statement,"limit ",6) && '?' == *(statement+6))
			return 1;

	return 0;
}

int has_list_fields(char *statement)
{
        /*
         *  Perform check for LISTFIELDS command
         *  and if we met it then mark as uncompatible with new 4.1 protocol 
         *  i.e. we leave imp_sth->has_protocol41=0 for this stmt 
         *  and it will be executed later in mysql_st_internal_execute()
         *  TODO: I think we can replace LISTFIELDS with SHOW COLUMNS [LIKE ...]
         *        to remove this extension hack
         */

	if (!strncasecmp(statement, "listfields", 10))
		return 1;

	return 0;
}
