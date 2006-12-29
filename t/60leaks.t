#!/usr/local/bin/perl
#
#   $Id$
#
#   This is a skeleton test. For writing new tests, take this file
#   and modify/extend it.
#

my $COUNT_CONNECT = 4000;   # Number of connect/disconnect iterations
my $COUNT_PREPARE = 10000;  # Number of prepare/execute/finish iterations


my $haveStorable;

if (!$ENV{SLOW_TESTS}) {
    print "1..0 # Skip \$ENV{SLOW_TESTS} is not set\n";
    exit 0;
}
eval { require Proc::ProcessTable; };
if ($@) {
    print "1..0 # Skip Proc::ProcessTable not installed \n";
    exit 0;
}
eval { require Storable };
$haveStorable = $@ ? 0 : 1;

sub size {
    my($p, $pt);
    $pt = Proc::ProcessTable->new('cache_ttys' => $haveStorable);
    foreach $p (@{$pt->table()}) {
	if ($p->pid() == $$) {
	    return $p->size();
	}
    }
    die "Cannot find my own process?!?\n";
    exit 0;
}

#
#   Make -w happy
#
$test_dsn = $test_user = $test_password = '';


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
if ($mdriver eq 'whatever') {
    print "1..0\n";
    exit 0;
}


#
#   Main loop; leave this untouched, put tests after creating
#   the new table.
#
while (Testing()) {
    #
    #   Connect to the database
    Test($state or ($dbh = DBI->connect($test_dsn, $test_user,
					$test_password)),
	 undef,
	 "Attempting to connect.\n")
	   or ErrMsgF("Cannot connect: Error %s.\n\n"
		      . "Make sure, your database server is up and running.\n"
		      . "Check that '$test_dsn' references a valid database"
		      . " name.\nDBI error message: $DBI::errstr");

    #
    #   Find a possible new table name
    #
    Test($state or $table = FindNewTable($dbh))
	   or ErrMsgF("Cannot determine a legal table name: Error %s.\n",
		      $dbh->errstr);

    #
    #   Create a new table; EDIT THIS!
    #
    Test($state or ($def = TableDefinition($table,
					   ["id",   "INTEGER",  4, 0],
					   ["name", "CHAR",    64, 0]),
		    $dbh->do($def)))
	   or ErrMsgF("Cannot create table: Error %s.\n",
		      $dbh->errstr);


    my($size, $prevSize, $ok, $notOk, $dbh2, $msg);
    if (!$state) {
	print "Testing memory leaks in connect/disconnect\n";
	$msg = "Possible memory leak in connect/disconnect detected";

	$ok = 0;
	$notOk = 0;
	for (my $i = 0;  $i < $COUNT_CONNECT;  $i++) {
	    if (!($dbh2 = DBI->connect($test_dsn, $test_user,
				       $test_password))) {
		$ok = 0;
		$msg = "Cannot connect: $DBI::errstr\n";
		last;
	    }
	    $dbh2->disconnect();
	    undef $dbh2;
	    if ($i % 100  ==  99) {
		$size = size();
		if (defined($prevSize)  &&  $size == $prevSize) {
		    ++$ok;
		} else {
		    ++$notOk;
		}
		$prevSize = $size;
	    }
	}
    }
    Test($state or ($ok > $notOk))
	or print "$msg\n";


    if (!$state) {
	print "Testing memory leaks in prepare/execute/finish\n";
	$msg = "Possible memory leak in prepare/execute/finish detected";

	$ok = 0;
	$notOk = 0;
	undef $prevSize;
	for (my $i = 0;  $i < $COUNT_PREPARE;  $i++) {
	    my $sth = $dbh->prepare("SELECT * FROM $table");
	    $sth->execute();
	    $sth->finish();
	    undef $sth;

	    if ($i % 100  ==  99) {
		$size = size();
		if (defined($prevSize)  &&  $size == $prevSize) {
		    ++$ok;
		} else {
		    ++$notOk;
		}
		$prevSize = $size;
	    }
	}
    }
    Test($state or ($ok > $notOk))
	or print "$msg\n";


    if (!$state) {
	print "Testing memory leaks in fetchrow_arrayref\n";
	$msg = "Possible memory leak in fetchrow_arrayref detected";

	# Insert some records into the test table
	my $row;
	foreach $row ([1, 'Jochen Wiedmann'],
		      [2, 'Andreas König'],
		      [3, 'Tim Bunce'],
		      [4, 'Alligator Descartes'],
		      [5, 'Jonathan Leffler']) {
	    $dbh->do(sprintf("INSERT INTO $table VALUES (%d, %s)",
			     $row->[0], $dbh->quote($row->[1])));
	}

	$ok = 0;
	$notOk = 0;
	undef $prevSize;
	for (my $i = 0;  $i < $COUNT_PREPARE;  $i++) {
	    {
		my $sth = $dbh->prepare("SELECT * FROM $table");
		$sth->execute();
		my $row;
		while ($row = $sth->fetchrow_arrayref()) {
		}
		$sth->finish();
	    }

	    if ($i % 100  ==  99) {
		$size = size();
		if (defined($prevSize)  &&  $size == $prevSize) {
		    ++$ok;
		} else {
		    ++$notOk;
		}
		$prevSize = $size;
	    }
	}
    }
    Test($state or ($ok > $notOk))
	or print "$msg\n";


    if (!$state) {
	print "Testing memory leaks in fetchrow_hashref\n";
	$msg = "Possible memory leak in fetchrow_hashref detected";

	$ok = 0;
	$notOk = 0;
	undef $prevSize;
	for (my $i = 0;  $i < $COUNT_PREPARE;  $i++) {
	    {
		my $sth = $dbh->prepare("SELECT * FROM $table");
		$sth->execute();
		my $row;
		while ($row = $sth->fetchrow_hashref()) {
		}
		$sth->finish();
	    }

	    if ($i % 100  ==  99) {
		$size = size();
		if (defined($prevSize)  &&  $size == $prevSize) {
		    ++$ok;
		} else {
		    ++$notOk;
		}
		$prevSize = $size;
	    }
	}
    }
    Test($state or ($ok > $notOk))
	or print "$msg\n";


    #
    #   Finally drop the test table.
    #
    Test($state or $dbh->do("DROP TABLE $table"))
	   or ErrMsgF("Cannot DROP test table $table: %s.\n",
		      $dbh->errstr);
    Test($state or $dbh->disconnect);
}
