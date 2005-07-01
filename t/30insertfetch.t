#!/usr/local/bin/perl
#
#   $Id$
#
#   This is a simple insert/fetch test.
#
$^W = 1;

#
#   Make -w happy
#
$test_dsn = '';
$test_user = '';
$test_password = '';


#
#   Include lib.pl
#
use DBI;
$mdriver = "";
foreach $file ("lib.pl", "t/lib.pl", "DBD-mysql/t/lib.pl") {
    do $file; if ($@) { print STDERR "Error while executing lib.pl: $@\n";
			   exit 10;
		      }
    if ($mdriver ne '') {
	last;
    }
}

sub ServerError() {
    print STDERR ("Cannot connect: ", $DBI::errstr, "\n",
	"\tEither your server is not up and running or you have no\n",
	"\tpermissions for acessing the DSN $test_dsn.\n",
	"\tThis test requires a running server and write permissions.\n",
	"\tPlease make sure your server is running and you have\n",
	"\tpermissions, then retry.\n");
    exit 10;
}

#
#   Main loop; leave this untouched, put tests after creating
#   the new table.
#
while (Testing()) {
    #
    #   Connect to the database
    Test($state or $dbh = DBI->connect($test_dsn, $test_user, $test_password))
	or ServerError();
    #DBI->trace(2);

    #
    #   Find a possible new table name
    #
    Test($state or $table = FindNewTable($dbh))
	   or DbiError($dbh->err, $dbh->errstr);

    #
    #   Create a new table; EDIT THIS!
    #
    Test($state or ($def = TableDefinition($table,
					  ["id",   "INTEGER",  4, 0],
					  ["name", "CHAR",    64, 0]),
		    $dbh->do($def)))
	   or DbiError($dbh->err, $dbh->errstr);


    #
    #   Insert a row into the test table.......
    #
    Test($state or $dbh->do("INSERT INTO $table"
			    . " VALUES(1, 'Alligator Descartes')"))
	   or DbiError($dbh->err, $dbh->errstr);

    #
    #   ...and delete it........
    #
    Test($state or $dbh->do("DELETE FROM $table WHERE id = 1"))
	   or DbiError($dbh->err, $dbh->errstr);

    #
    #   Now, try SELECT'ing the row out. This should fail.
    #
    Test($state or $cursor = $dbh->prepare("SELECT * FROM $table"
					   . " WHERE id = 1"))
	   or DbiError($dbh->err, $dbh->errstr);

    Test($state or $cursor->execute)
	   or DbiError($cursor->err, $cursor->errstr);

    my ($row, $errstr);
    Test($state or (!defined($row = $cursor->fetchrow_arrayref)  &&
		    (!defined($errstr = $cursor->errstr) ||
		     $cursor->errstr eq '')))
	or DbiError($cursor->err, $cursor->errstr);

    Test($state or $cursor->finish, "\$sth->finish failed")
	   or DbiError($cursor->err, $cursor->errstr);

    Test($state or undef $cursor || 1);


    #
    #   Finally drop the test table.
    #
    Test($state or $dbh->do("DROP TABLE $table"))
	   or DbiError($dbh->err, $dbh->errstr);

}

