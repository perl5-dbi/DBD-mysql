#!/usr/local/bin/perl
#
#   $Id: 40blobs.t 1103 2003-03-18 02:53:28Z rlippan $
#
#   This is a test for correct handling of BLOBS; namely $dbh->quote
#   is expected to work correctly.
#

#
# Thank you to Brad Choate for finding the bug that resulted in this test,
# which he kindly sent code that this test uses!
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
my $update_blob;
foreach $file ("lib.pl", "t/lib.pl") {
    do $file; if ($@) { print STDERR "Error while executing lib.pl: $@\n";
			   exit 10;
		      }
    if ($mdriver ne '') {
	last;
    }
}

my @chars = grep !/[0O1Iil]/, 0..9, 'A'..'Z', 'a'..'z';
my $blob1= join '', map { $chars[rand @chars] } 0 .. 10000;
$blob2 = '"' x 10000;

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

# 
# create a new table
#
  Test($state or
      $dbh->do("CREATE TABLE $table (id int(4), name text)"))
    or DbiError($dbh->err, $dbh->errstr);

  my($def);

#
#   Insert a row into the test table.......
#
  my($query, $sth);
  if (!$state) {
    $query = "INSERT INTO $table VALUES(?, ?)";
  }
  Test($state or $sth= $dbh->prepare($query))
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute(1, $blob1))
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->finish)
    or DbiError($sth->err, $sth->errstr);

  Test($state or undef $sth || 1)
    or DbiError($sth->err, $sth->errstr);

#
#   Now, try SELECTing the row out.
#
  Test($state or $sth=
      $dbh->prepare("SELECT * FROM $table WHERE id = 1"))
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute)
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or (defined($row = $sth->fetchrow_arrayref)))
    or DbiError($sth->err, $sth->errstr);

  Test($state or (@$row == 2 && $$row[0] == 1 && $$row[1] eq $blob1))
    or (ShowBlob($blob1),
        ShowBlob(defined($$row[1]) ? $$row[1] : ""));

  Test($state or $sth->finish)
    or DbiError($sth->err, $sth->errstr);

  Test($state or undef $sth || 1)
    or DbiError($sth->err, $sth->errstr);

  Test($state or $sth=
      $dbh->prepare("UPDATE $table SET name = ? WHERE id = 1"))
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute($blob2))
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->finish)
    or DbiError($sth->err, $sth->errstr);

  Test($state or undef $sth || 1)
    or DbiError($sth->err, $sth->errstr);

  Test($state or $sth=
      $dbh->prepare("SELECT * FROM $table WHERE id = 1"))
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute)
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or (defined($row = $sth->fetchrow_arrayref)))
    or DbiError($sth->err, $sth->errstr);

  Test($state or (@$row == 2  &&  $$row[0] == 1  &&  $$row[1] eq $blob2))
    or (ShowBlob($blob2),
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
