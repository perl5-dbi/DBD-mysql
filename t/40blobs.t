#!/usr/local/bin/perl
#
#   $Id$
#
#   This is a test for correct handling of BLOBS; namely $dbh->quote
#   is expected to work correctly.
#


#
#   Make -w happy
#
$test_dsn = '';
$test_user = '';
$test_password = '';


#
#   Include lib.pl
#
require DBI;
$mdriver = "";
foreach $file ("lib.pl", "t/lib.pl") {
    do $file; if ($@) { print STDERR "Error while executing lib.pl: $@\n";
			   exit 10;
		      }
    if ($mdriver ne '') {
	last;
    }
}
if ($dbdriver eq 'mSQL'  ||  $dbdriver eq 'mSQL1') {
    print "1..0\n";
    exit 0;
}

sub ServerError() {
    my $err = $DBI::errstr; # Hate -w ...
    print STDERR ("Cannot connect: ", $DBI::errstr, "\n",
	"\tEither your server is not up and running or you have no\n",
	"\tpermissions for acessing the DSN $test_dsn.\n",
	"\tThis test requires a running server and write permissions.\n",
	"\tPlease make sure your server is running and you have\n",
	"\tpermissions, then retry.\n");
    exit 10;
}


sub ShowBlob($) {
    my ($blob) = @_;
    for($i = 0;  $i < 8;  $i++) {
	if (defined($blob)  &&  length($blob) > $i) {
	    $b = substr($blob, $i*32);
	} else {
	    $b = "";
	}
	printf("%08lx %s\n", $i*32, unpack("H64", $b));
    }
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
	   or DbiError($dbh->error, $dbh->errstr);

    my($def);
    foreach $size (128) {
	#
	#   Create a new table
	#
	if (!$state) {
	    $def = TableDefinition($table,
				   ["id",   "INTEGER",      4, 0],
				   ["name", "BLOB",     $size, 0]);
	    print "Creating table:\n$def\n";
	}
	Test($state or $dbh->do($def))
	    or DbiError($dbh->err, $dbh->errstr);


	#
	#  Create a blob
	#
	my ($blob, $qblob) = "";
	if (!$state) {
	    my $b = "";
	    for ($j = 0;  $j < 256;  $j++) {
		$b .= chr($j);
	    }
	    for ($i = 0;  $i < $size;  $i++) {
		$blob .= $b;
	    }
	    if ($mdriver eq 'pNET') {
		# Quote manually, no remote quote
		$qblob = eval "DBD::" . $dbdriver . "::db->quote(\$blob)";
	    } else {
		$qblob = $dbh->quote($blob);
	    }
	}

	#
	#   Insert a row into the test table.......
	#
	my($query);
	if (!$state) {
	    $query = "INSERT INTO $table VALUES(1, $qblob)";
	    if ($ENV{'SHOW_BLOBS'}  &&  open(OUT, ">" . $ENV{'SHOW_BLOBS'})) {
		print OUT $query;
		close(OUT);
	    }
	}
        Test($state or $dbh->do($query))
	    or DbiError($dbh->err, $dbh->errstr);

	#
	#   Now, try SELECT'ing the row out.
	#
	Test($state or $sth = $dbh->prepare("SELECT * FROM $table"
					       . " WHERE id = 1"))
	       or DbiError($dbh->err, $dbh->errstr);

	Test($state or $sth->execute)
	       or DbiError($dbh->err, $dbh->errstr);

	Test($state or (defined($row = $sth->fetchrow_arrayref)))
	    or DbiError($sth->err, $sth->errstr);

	Test($state or (@$row == 2  &&  $$row[0] == 1  &&  $$row[1] eq $blob))
	    or (ShowBlob($blob),
		ShowBlob(defined($$row[1]) ? $$row[1] : ""));

	Test($state or $sth->finish)
	    or DbiError($sth->err, $sth->errstr);

	Test($state or undef $sth || 1)
	    or DbiError($sth->err, $sth->errstr);

	#
	#   Finally drop the test table.
	#
	Test($state or $dbh->do("DROP TABLE $table"))
	    or DbiError($dbh->err, $dbh->errstr);
    }
}
