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
use Carp qw(croak);
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
    my $testInsertVals = {};
    #
    #   Connect to the database
    Test($state or $dbh = DBI->connect($test_dsn, $test_user, $test_password))
	or ServerError();

  #Test($state or $dbh->trace("3", "/tmp/trace.log")) or DbiError($dbh->err, $dbh->errstr);
    #
    #   Find a possible new table name
    #
    #Test($state or $table = FindNewTable($dbh))
	  # or DbiError($dbh->err, $dbh->errstr);
    $table = 'prepare_test';

    Test($state or ($dbh->do("DROP TABLE IF EXISTS $table")) or DbiError($dbh->err, $dbh->errstr));
    #
    #   Create a new table; EDIT THIS!
    #
    Test($state or ($def = TableDefinition($table,
					  ["id",   "INTEGER",  4, 0],
					  ["name", "CHAR",    64, 0]),
		    $dbh->do($def)))
	   or DbiError($dbh->err, $dbh->errstr);


    print "PERL testing prepare of insert statement:\n";
    Test($state or $sth = $dbh->prepare("INSERT INTO $table VALUES (?,?)"))
	    or DbiError($dbh->err, $dbh->errstr);

    print "PERL testing insertion of values from previous prepare of insert statement:\n";
    my $rows = 0;
    for (my $i = 0 ; $i < 100; $i++) { 
        my @chars = grep !/[0O1Iil]/, 0..9, 'A'..'Z', 'a'..'z';
        my $random_chars = join '', map { $chars[rand @chars] } 0 .. 16;
        # save these values for later testing
        $testInsertVals->{$i} = $random_chars;
        Test(
          $state or 
          $rows = $sth->execute($i, $random_chars)
        ) or 
        DbiError($dbh->err, $dbh->errstr);
        
    }
    print "PERL rows : " . $rows . "\n"; 

    print "PERL testing prepare of select statement with INT and VARCHAR placeholders:\n";
    Test($state or $sth = $dbh->prepare("SELECT * FROM $table WHERE id = ? AND name = ?"))
	    or DbiError($dbh->err, $dbh->errstr);

    for my $id (keys %$testVals) {
      Test($state or $sth->execute($id, $testVals->{$id}))
        or DbiError($sth->err, $sth->errstr);
    }
 
    print "PERL testing prepare of select statement with LIMIT placeholders:\n";
    Test($state or $sth = $dbh->prepare("SELECT * FROM $table LIMIT ?, ?"))
	    or DbiError($dbh->err, $dbh->errstr);

    print "PERL testing exec of bind vars for LIMIT\n";
    Test($state or $sth->execute(20, 50))
	   or DbiError($sth->err, $sth->errstr);

    my ($row, $errstr, $array_ref);
    Test(
      $state or 
      (defined($array_ref = $sth->fetchall_arrayref)  &&
      (!defined($errstr = $sth->errstr) || $sth->errstr eq '')))
	  or DbiError($sth->err, $sth->errstr);

    Test ($state or @$array_ref == 50) or print "results not equaling 50\n";
    
    Test($state or $sth->finish, "\$sth->finish failed")
      or DbiError($sth->err, $sth->errstr);

    Test($state or undef $sth || 1);


    #
    #   Finally drop the test table.
    #
    Test($state or $dbh->do("DROP TABLE $table")) or DbiError($dbh->err, $dbh->errstr);

    Test($state or $dbh->disconnect)  or DBiError($dbh->err, $dbh-errstr);

}
