#!/usr/local/bin/perl
#
#   $Id$
#
#   This is a test for statement attributes being present appropriately.
#


#
#   Make -w happy
#
$test_dsn = '';
$test_user = '';
$test_password = '';
$COL_KEY = '';


#
#   Include lib.pl
#
use DBI;
use vars qw($verbose);

$mdriver = "";
foreach $file ("lib.pl", "t/lib.pl") {
    do $file; if ($@) { print STDERR "Error while executing lib.pl: $@\n";
			   exit 10;
		      }
    if ($mdriver ne '') {
	last;
    }
}


@table_def = (
	      ["id",   "INTEGER",  4, $COL_KEY],
	      ["name", "CHAR",    64, $COL_NULLABLE]
	     );

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

    #
    #   Find a possible new table name
    #
    Test($state or $table = FindNewTable($dbh))
	   or DbiError($dbh->err, $dbh->errstr);

    #
    #   Create a new table
    #
    Test($state or ($def = TableDefinition($table, @table_def),
		    $dbh->do($def)))
	   or DbiError($dbh->err, $dbh->errstr);

    Test($state or $dbh->table_info(undef,undef,$table));
    Test($state or $dbh->column_info(undef,undef,$table,'%'));

    Test($state or $cursor = $dbh->prepare("SELECT * FROM $table"))
	   or DbiError($dbh->err, $dbh->errstr);

    Test($state or $cursor->execute)
	   or DbiError($cursor->err, $cursor->errstr);

    my $res;
    Test($state or (($res = $cursor->{'NUM_OF_FIELDS'}) == @table_def))
	   or DbiError($cursor->err, $cursor->errstr);
    if (!$state && $verbose) {
	printf("Number of fields: %s\n", defined($res) ? $res : "undef");
    }

    Test($state or ($ref = $cursor->{'NAME'})  &&  @$ref == @table_def
	            &&  (lc $$ref[0]) eq $table_def[0][0]
		    &&  (lc $$ref[1]) eq $table_def[1][0])
	   or DbiError($cursor->err, $cursor->errstr);
    if (!$state && $verbose) {
	print "Names:\n";
	for ($i = 0;  $i < @$ref;  $i++) {
	    print "    ", $$ref[$i], "\n";
	}
    }

    Test($state or ($ref = $cursor->{'NULLABLE'})  &&  @$ref == @table_def
		    &&  !($$ref[0] xor ($table_def[0][3] & $COL_NULLABLE))
		    &&  !($$ref[1] xor ($table_def[1][3] & $COL_NULLABLE)))
	   or DbiError($cursor->err, $cursor->errstr);
    if (!$state && $verbose) {
	print "Nullable:\n";
	for ($i = 0;  $i < @$ref;  $i++) {
	    print "    ", ($$ref[$i] & $COL_NULLABLE) ? "yes" : "no", "\n";
	}
    }

    Test($state or (($ref = $cursor->{TYPE})  &&  (@$ref == @table_def)
		    &&  ($ref->[0] eq DBI::SQL_INTEGER())
		    &&  ($ref->[1] eq DBI::SQL_VARCHAR()  ||
			 $ref->[1] eq DBI::SQL_CHAR())))
	or printf("Expected types %d and %d, got %s and %s\n",
		  &DBI::SQL_INTEGER(), &DBI::SQL_VARCHAR(),
		  defined($ref->[0]) ? $ref->[0] : "undef",
		  defined($ref->[1]) ? $ref->[1] : "undef");

    Test($state or undef $cursor  ||  1);


    #
    #  Drop the test table
    #
    Test($state or ($cursor = $dbh->prepare("DROP TABLE $table")))
	or DbiError($dbh->err, $dbh->errstr);
    Test($state or $cursor->execute)
	or DbiError($cursor->err, $cursor->errstr);

    #  NUM_OF_FIELDS should be zero (Non-Select)
    Test($state or (! defined $cursor->{'NUM_OF_FIELDS'} ||
          $cursor->{'NUM_OF_FIELDS'} == 0))
	or !$verbose or printf("NUM_OF_FIELDS is %s, not zero.\n",
			       $cursor->{'NUM_OF_FIELDS'});
    Test($state or (undef $cursor) or 1);

    #
    #  Test different flavours of quote. Need to work around a bug in
    #  DBI 1.02 ...
    #
    my $quoted;
    if (!$state) {
	$quoted = eval { $dbh->quote(0, DBI::SQL_INTEGER()) };
    }
    Test($state or $@  or  $quoted eq 0);
    if (!$state) {
	$quoted = eval { $dbh->quote('abc', DBI::SQL_VARCHAR()) };
    }
    Test($state or $@ or $quoted eq q{'abc'});
}
