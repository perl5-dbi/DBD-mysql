#!/usr/local/bin/perl
#
#   $Id$
#
#   This is a skeleton test. For writing new tests, take this file
#   and modify/extend it.
#


#
#   Make -w happy
#
use vars qw($test_dsn $test_user $test_password $dbdriver $mdriver
	    $verbose $state);
use vars qw($COL_NULLABLE $COL_KEY);
$test_dsn = '';
$test_user = '';
$test_password = '';


#
#   Include lib.pl
#
use DBI;
use strict;
$dbdriver = "";
{   my $file;
    foreach $file ("lib.pl", "t/lib.pl", "DBD-mysql/t/lib.pl") {
	do $file; if ($@) { print STDERR "Error while executing lib.pl: $@\n";
			    exit 10;
			}
	if ($dbdriver ne '') {
	    last;
	}
    }
}

my $test_db = '';
my $test_hostname = $ENV{DBI_HOST} || 'localhost';
$| = 1;

if ($test_dsn =~ /^DBI\:[^\:]+\:/) {
    $test_db = $';
    if ($test_db =~ /:/) {
	$test_db = $`;
	$test_hostname = $';
    }
}

#
#   Main loop; leave this untouched, put tests after creating
#   the new table.
#
while (Testing()) {
    #
    #   Connect to the database
    my($dbh, $sth, $test_table, $query);
    $test_table = '';  # Avoid warnings for undefined variables.
    Test($state or ($dbh = DBI->connect($test_dsn, $test_user,
					$test_password)))
	or ErrMsg("Cannot connect: $DBI::errstr.\n");

    #
    #   Verify whether constants work
    #
    if ($mdriver eq 'mysql') {
	my ($val);
	Test($state  or  (($val = &DBD::mysql::FIELD_TYPE_STRING()) == 254))
	    or ErrMsg("Wrong value for FIELD_TYPE_STRING:"
		      . " Expected 254, got $val\n");
	Test($state  or  (($val = &DBD::mysql::FIELD_TYPE_SHORT()) == 2))
	    or ErrMsg("Wrong value for FIELD_TYPE_SHORT:"
		      . " Expected 2, got $val\n");
    } elsif ($mdriver eq 'mSQL') {
	my ($val);
	Test($state  or  (($val = &DBD::mSQL::CHAR_TYPE()) == 2))
	    or ErrMsg("Wrong value for CHAR_TYPE: Expected 2, got $val\n");
	Test($state  or  (($val = &DBD::mSQL::INT_TYPE()) == 1))
	    or ErrMsg("Wrong value for INT_TYPE: Expected 1, got $val\n");
    }

    #
    #   Find a possible new table name
    #
    Test($state or $test_table = FindNewTable($dbh)) or !$verbose
	or ErrMsg("Cannot get table name: $dbh->errstr.\n");

    #
    #   Create a new table; EDIT THIS!
    #
    Test($state or ($query = TableDefinition($test_table,
				     ["id",   "INTEGER",  4, $COL_NULLABLE],
				     ["name", "CHAR",    64, $COL_NULLABLE]),
		    $dbh->do($query)))
	or ErrMsg("Cannot create table: query $query error $dbh->errstr.\n");

    #
    #   and here's the right place for inserting new tests:
    #
    Test($state or $dbh->quote('tast1'))
	or ErrMsgF("quote('tast1') returned %s.\n", $dbh->quote('tast1'));

    ### ...and disconnect
    Test($state or $dbh->disconnect)
	or ErrMsg("\$dbh->disconnect() failed!\n",
		  "Make sure your server is still functioning",
		  "correctly, and check to make\n",
		  "sure your network isn\'t malfunctioning in the",
		  "case of the server running on a remote machine.\n");

    ### Now, re-connect again so that we can do some more complicated stuff..
    Test($state or ($dbh = DBI->connect($test_dsn, $test_user,
					$test_password)))
	or ErrMsg("reconnect failed: $DBI::errstr\n");

    ### List all the tables in the selected database........
    ### This test for mSQL and mysql only.
    if ($mdriver eq 'mysql'  or $mdriver eq 'mSQL' or $mdriver eq 'mSQL1') {
	Test($state or $dbh->tables())
	    or ErrMsgF("tables() failed: $dbh->errstr.\n"
		       . "This could be due to the fact you have no tables,"
		       . " but I hope not. You\n"
		       . "could try running '%s -h %s %s' and see if it\n"
		       . "reports any information about your database,"
		       . " or errors.\n",
		       ($mdriver eq 'mysql') ? "mysqlshow" : "relshow",
		       $test_hostname, $test_db);
    }

    Test($state or $dbh->do("DROP TABLE $test_table"))
	or ErrMsg("Dropping table failed: $dbh->errstr.\n");
    Test($state or ($query = TableDefinition($test_table,
				     ["id",   "INTEGER",  4, $COL_NULLABLE],
				     ["name", "CHAR",    64, $COL_NULLABLE]),
		    $dbh->do($query)))
        or ErrMsg("create failed, query $query, error $dbh->errstr.\n");

    ### Get some meta-data for the table we've just created...
    if ($mdriver eq 'mysql' or $mdriver eq 'mSQL1' or $mdriver eq 'mSQL') {
	my $ref;
	Test($state or ($ref = $dbh->prepare("LISTFIELDS $test_table")))
	    or ErrMsg("listfields failed: $dbh->errstr.\n");
	Test($state or $ref->execute);
    }

    ### Insert a row into the test table.......
    Test($state or ($dbh->do("INSERT INTO $test_table VALUES(1,"
			     . " 'Alligator Descartes')")))
         or ErrMsg("INSERT failed: $dbh->errstr.\n");

    ### ...and delete it........
    Test($state or $dbh->do("DELETE FROM $test_table WHERE id = 1"))
         or ErrMsg("Cannot delete row: $dbh->errstr.\n");
    Test($state or ($sth = $dbh->prepare("SELECT * FROM $test_table"
                                         . " WHERE id = 1")))
         or ErrMsg("Cannot select: $dbh->errstr.\n");

    # This should fail with error message: We "forgot" execute.
    {
	my $pe = $sth->{'PrintError'};
	$sth->{'PrintError'} = 0;
	Test($state or !$sth->fetchrow)
	    or ErrMsg("Missing error report from fetchrow.\n");
	$sth->{'PrintError'} = $pe;
    }

    Test($state or $sth->execute)
         or ErrMsg("execute SELECT failed: $dbh->errstr.\n");

    # This should fail without error message: No rows returned.
    my(@row, $ref);
    {
        local($^W) = 0;
        Test($state or !defined($ref = $sth->fetch))
	    or ErrMsgF("Unexpected row returned by fetchrow: $ref\n".
		       scalar(@row));
    }

    # Now try a "finish"
    Test($state or $sth->finish)
	or ErrMsg("sth->finish failed: $sth->errstr.\n");

    # Call destructors:
    Test($state or (undef $sth || 1));

    ### This section should exercise the sth->func( '_NumRows' ) private
    ###  method by preparing a statement, then finding the number of rows
    ###  within it. Prior to execution, this should fail. After execution,
    ###  the number of rows affected by the statement will be returned.
    Test($state or ($dbh->do($query = "INSERT INTO $test_table VALUES"
			               . " (1, 'Alligator Descartes' )")))
	or ErrMsgF("INSERT failed: query $query, error %s.\n", $dbh->errstr);
    Test($state or ($sth = $dbh->prepare($query = "SELECT * FROM $test_table"
					          . " WHERE id = 1")))
	or ErrMsgF("prepare failed: query $query, error %s.\n", $dbh->errstr);
    if ($dbdriver eq 'mysql'  ||  $dbdriver eq 'mSQL'  ||
	$dbdriver eq 'mSQL1') {
	Test($state or defined($sth->rows))
	    or ErrMsg("sth->rows returning result before 'execute'.\n");
    }

    Test($state or $sth->execute)
	or ErrMsgF("execute failed: query $query, error %s.\n", $sth->errstr);
    Test($state  or  ($sth->rows == 1)  or  ($sth->rows == -1))
	or ErrMsgF("sth->rows returned wrong result %s after 'execute'.\n",
		   $sth->rows);
    Test($state or $sth->finish)
	or ErrMsgF("finish failed: %s.\n", $sth->errstr);
    Test($state or (undef $sth or 1));

    ### Test whether or not a field containing a NULL is returned correctly
    ### as undef, or something much more bizarre
    $query = "INSERT INTO $test_table VALUES ( NULL, 'NULL-valued id' )";
    Test($state or $dbh->do($query))
	or ErrMsgF("INSERT failed: query $query, error %s.\n", $dbh->errstr);
    $query = "SELECT id FROM $test_table WHERE " . IsNull("id");
    Test($state or ($sth = $dbh->prepare($query)))
	or ErrMsgF("Cannot prepare, query = $query, error %s.\n",
		   $dbh->errstr);
    Test($state or $sth->execute)
	or ErrMsgF("Cannot execute, query = $query, error %s.\n",
		   $dbh->errstr);
    my $rv;
    Test($state or defined($rv = $sth->fetch))
	or ErrMsgF("fetch failed, error %s.\n", $dbh->errstr);
    Test($state or !defined($$rv[0]))
	or ErrMsgF("Expected NULL value, got %s.\n", $$rv[0]);
    Test($state or $sth->finish)
	or ErrMsgF("finish failed: %s.\n", $sth->errstr);
    Test($state or undef $sth or 1);

    ### Delete the test row from the table
    $query = "DELETE FROM $test_table WHERE id = NULL AND"
        . " name = 'NULL-valued id'";
    Test($state or ($rv = $dbh->do($query)))
        or ErrMsg("DELETE failed: query $query, error %s.\n", $dbh->errstr);

    ### Test whether or not a char field containing a blank is returned
    ###  correctly as blank, or something much more bizarre
    $query = "INSERT INTO $test_table VALUES (2, '')";
    Test($state or $dbh->do($query))
        or ErrMsg("INSERT failed: query $query, error %s.\n", $dbh->errstr);
    $query = "SELECT name FROM $test_table WHERE id = 2 AND name = ''";
    Test($state or ($sth = $dbh->prepare($query)))
        or ErrMsg("prepare failed: query $query, error %s.\n", $dbh->errstr);
    Test($state or $sth->execute)
        or ErrMsg("execute failed: query $query, error %s.\n", $dbh->errstr);
    $rv = undef;
    Test($state or defined($ref = $sth->fetch))
        or ErrMsgF("fetchrow failed: query $query, error %s.\n", $sth->errstr);
    Test($state or (defined($$ref[0])  &&  ($$ref[0] eq '')))
        or ErrMsgF("blank value returned as %s.\n", $$ref[0]);
    Test($state or $sth->finish)
	or ErrMsg("finish failed: $sth->errmsg.\n");
    Test($state or undef $sth or 1);

    ### Delete the test row from the table
    $query = "DELETE FROM $test_table WHERE id = 2 AND name = ''";
    Test($state or $dbh->do($query))
	or ErrMsg("DELETE failed: query $query, error $dbh->errstr.\n");

    ### Test the new funky routines to list the fields applicable to a SELECT
    ### statement, and not necessarily just those in a table...
    $query = "SELECT * FROM $test_table";
    Test($state or ($sth = $dbh->prepare($query)))
	or ErrMsg("prepare failed: query $query, error $dbh->errstr.\n");
    Test($state or $sth->execute)
	or ErrMsg("execute failed: query $query, error $dbh->errstr.\n");
    Test($state or $sth->execute)
	or ErrMsg("re-execute failed: query $query, error $dbh->errstr.\n");
    Test($state or (@row = $sth->fetchrow))
	or ErrMsg("Query returned no result, query $query,",
		  " error $sth->errstr.\n");
    Test($state or $sth->finish)
	or ErrMsg("finish failed: $sth->errmsg.\n");
    Test($state or undef $sth or 1);

    ### Insert some more data into the test table.........
    $query = "INSERT INTO $test_table VALUES( 2, 'Gary Shea' )";
    Test($state or $dbh->do($query))
        or ErrMsg("INSERT failed: query $query, error $dbh->errstr.\n");
    $query = "UPDATE $test_table SET id = 3 WHERE name = 'Gary Shea'";
    Test($state or ($sth = $dbh->prepare($query)))
        or ErrMsg("prepare failed: query $query, error $sth->errmsg.\n");
    # This should fail: We "forgot" execute.
    if ($mdriver eq 'mysql'  ||  $mdriver eq 'mSQL'  ||
	$mdriver eq 'mSQL1') {
        Test($state or !defined($sth->{'NAME'}))
            or ErrMsg("Expected error without execute, got $ref.\n");
    }
    Test($state or undef $sth or 1);

    ### Drop the test table out of our database to clean up.........
    $query = "DROP TABLE $test_table";
    Test($state or $dbh->do($query))
        or ErrMsg("DROP failed: query $query, error $dbh->errstr.\n");

    # Verify whether we can set mysql_use_result via $dbh->prepare
    if ($dbdriver eq 'mysql') {
	Test($state or
	     ($sth = $dbh->prepare("SELECT 1", {'mysql_use_result' => 1})));
	Test($state or $sth->{'mysql_use_result'})
	    or printf("mysql_use_result not set\n");
	Test($state or
	     ($sth = $dbh->prepare("SELECT 1", {'mysql_use_result' => 0})));
	Test($state or !$sth->{'mysql_use_result'})
	    or printf("mysql_use_result is set\n");
	Test($state or
	     ($sth = $dbh->prepare("SELECT 1")));
	Test($state or !$sth->{'mysql_use_result'})
	    or printf("mysql_use_result is set\n");
    }

    Test($state or $dbh->disconnect)
        or ErrMsg("disconnect failed: $dbh->errstr.\n");

    #
    #   Try mysql's insertid feature
    #
    if ($dbdriver eq 'mysql') {
	my ($sth, $table);
	Test($state or ($dbh = DBI->connect($test_dsn, $test_user,
					    $test_password)))
            or ErrMsgF("connect failed: %s.\n", $DBI::errstr);
	Test($state or ($table = FindNewTable($dbh)));
	Test($state or $dbh->do("CREATE TABLE $table ("
				. " id integer AUTO_INCREMENT PRIMARY KEY,"
				. " country char(30) NOT NULL)"))
	    or printf("Error while executing query: %s\n", $dbh->errstr);
	Test($state or
	     ($sth = $dbh->prepare("INSERT INTO $table VALUES (NULL, 'a')")))
	    or printf("Error while preparing query: %s\n", $dbh->errstr);
	Test($state or $sth->execute)
	    or printf("Error while executing query: %s\n", $sth->errstr);
	Test($state or $sth->finish)
	    or printf("Error while finishing query: %s\n", $sth->errstr);
	Test($state or
	     ($sth = $dbh->prepare("INSERT INTO $table VALUES (NULL, 'b')")))
	    or printf("Error while preparing query: %s\n", $dbh->errstr);
	Test($state or $sth->execute)
	    or printf("Error while executing query: %s\n", $sth->errstr);
	Test($state or $sth->{'mysql_insertid'} =~ /\d+/)
	    or printf("insertid generated incorrect result: %s\n",
		      $sth->insertid);
	Test($state or $sth->finish)
	    or printf("Error while finishing query: %s\n", $sth->errstr);
	Test($state or $dbh->do("DROP TABLE $table"));
	Test($state or $dbh->disconnect)
	    or ErrMsg("disconnect failed: $dbh->errstr.\n");
    }

    #
    # Try mysql_client_found_rows
    #
    if ($dbdriver eq 'mysql') {
	my $table;
	Test($state or
	     ($dbh = DBI->connect("$test_dsn;mysql_client_found_rows=0",
				  $test_user, $test_password)))
	    or ErrMsgF("connect failed: %s.\n", $DBI::errstr);
	Test($state or ($table = FindNewTable($dbh)));
	Test($state or $dbh->do("DROP TABLE IF EXISTS $table"))
	    or printf("Error while dropping table: %s\n", $dbh->errstr());
	Test($state or $dbh->do("CREATE TABLE $table ("
				. " object_id integer,"
				. " object_title VARCHAR(64))"))
	    or printf("Error while creating table: %s\n", $dbh->errstr());
	Test($state or $dbh->do("INSERT INTO $table VALUES (?, ?)", undef,
				162, ""))
	    or printf("Failed to insert: %s\n", $dbh->errstr());
	my $rc;
	my $query = "UPDATE $table SET object_title = ? WHERE object_id = 162"
	    unless $state;
	Test($state or
	     ($rc = $dbh->do($query, undef, "test")))
	    or printf("Failed to update: %s\n", $dbh->errstr());
	Test($state or $rc == 1)
	    or printf("Expected 1st Update to return 1 without"
		      . " mysql_client_found_rows, got $rc\n");
	Test($state or
	     ($rc = $dbh->do($query, undef, "test")))
	    or printf("Failed to update: %s\n", $dbh->errstr());
	Test($state or $rc == 0)
	    or printf("Expected 2nd Update to return 0 without"
		      . " mysql_client_found_rows, got $rc\n");
	Test($state or $dbh->disconnect())
	    or printf("Error while disconnecting: %s\n", $dbh->errstr());

	Test($state or
	     ($dbh = DBI->connect("$test_dsn;mysql_client_found_rows=1",
				  $test_user, $test_password)))
            or ErrMsgF("reconnect failed: %s.\n", $DBI::errstr);
	Test($state or
	     ($rc = $dbh->do($query, undef, "")))
	    or printf("Failed to update: %s\n", $dbh->errstr());
	Test($state or ($rc = $dbh->do($query, undef, "test")))
	    or printf("Failed to reupdate: %s\n", $dbh->errstr());
	Test($state or $rc == 1)
	    or printf("Expected 1st Update to return 1 with"
		      . " mysql_client_found_rows, got $rc\n");
	Test($state or ($rc = $dbh->do($query, undef, "test")))
	    or printf("Failed to reupdate: %s\n", $dbh->errstr());
	Test($state or $rc == 1)
	    or printf("Expected 2nd Update to return 1 with"
		      . " mysql_client_found_rows, got $rc\n");
	Test($state or $dbh->disconnect())
	    or printf("Error while disconnecting: %s\n", $dbh->errstr());
    }
}
