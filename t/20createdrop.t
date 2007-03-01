#!/usr/local/bin/perl
#
#   $Id$
#
#   This is a skeleton test. For writing new tests, take this file
#   and modify/extend it.
#

use strict;
use vars qw($test_dsn $test_user $test_password $mdriver $dbdriver);
use DBI ();

#
#   Include lib.pl
#
$mdriver = "";
my $file;
foreach $file ("lib.pl", "t/lib.pl") {
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
#   Main loop; leave this untouched, put tests into the loop
#
use vars qw($state);
while (Testing()) {
    #
    #   Connect to the database
    my $dbh;
    Test($state or $dbh = DBI->connect($test_dsn, $test_user, $test_password))
	or ServerError();

    #
    #   Find a possible new table name
    #
    my $table;
    Test($state or $table = FindNewTable($dbh))
	   or DbiError($dbh->err, $dbh->errstr);

    #
    #   Create a new table
    #
    my $def;
    if (!$state) {
	($def = TableDefinition($table,
				["id",   "INTEGER",  4, 0],
				["name", "CHAR",    64, 0]));
	print "Creating table:\n$def\n";
    }
    Test($state or $dbh->do($def))
	or DbiError($dbh->err, $dbh->errstr);


    #
    #   ... and drop it.
    #
    Test($state or $dbh->do("DROP TABLE $table"))
	   or DbiError($dbh->err, $dbh->errstr);

    #
    #   ... check do() sets $dbh->{Statement}
    #
    Test($state or $dbh->{Statement} eq "DROP TABLE $table")
	   or DbiError(1, "do() didn't set Statement attribute");

    #
    #   Finally disconnect.
    #
    Test($state or $dbh->disconnect())
	   or DbiError($dbh->err, $dbh->errstr);
}
