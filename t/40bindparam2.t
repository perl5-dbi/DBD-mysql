#!/usr/local/bin/perl
#
#   $Id: 40bindparam.t 6304 2006-05-17 21:23:10Z capttofu $ 
#
#   This is a skeleton test. For writing new tests, take this file
#   and modify/extend it.
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
use DBI ();
use vars qw($COL_NULLABLE $rows);
$mdriver = "";
foreach $file ("lib.pl", "t/lib.pl") {
    do $file; if ($@) { print STDERR "Error while executing lib.pl: $@\n";
			   exit 10;
		      }
    if ($mdriver ne '') {
	last;
    }
}
if ($mdriver eq 'pNET') {
    print "1..0\n";
    exit 0;
}

sub ServerError() {
    my $err = $DBI::errstr;  # Hate -w ...
    print STDERR ("Cannot connect: ", $DBI::errstr, "\n",
	"\tEither your server is not up and running or you have no\n",
	"\tpermissions for acessing the DSN $test_dsn.\n",
	"\tThis test requires a running server and write permissions.\n",
	"\tPlease make sure your server is running and you have\n",
	"\tpermissions, then retry.\n");
    exit 10;
}

if (!defined(&SQL_VARCHAR)) {
    eval "sub SQL_VARCHAR { 12 }";
}
if (!defined(&SQL_INTEGER)) {
    eval "sub SQL_INTEGER { 4 }";
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

    Test($state or $table = FindNewTable($dbh))
	   or DbiError($dbh->err, $dbh->errstr);

    #
    #   Create a new table; EDIT THIS!
    #
  Test($state or 
    ($dbh->do("CREATE TABLE $table (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, num INT)")))
      or DbiError($dbh->err, $dbh->errstr);

  Test($state or ($dbh->do("INSERT INTO $table VALUES(NULL, 1)")))
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or ($rows= $dbh->selectall_arrayref("SELECT * FROM $table")))
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or ($rows->[0][1] == 1)) 
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or
    ($sth = $dbh->prepare("UPDATE $table SET num = ? WHERE id = ?")))
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or ($sth->bind_param(2, 1, SQL_INTEGER())))
    or DbiError($dbh->err, $dbh->errstr);
  
  Test($state or ($sth->execute()))
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or
    ($rows = $dbh->selectall_arrayref("SELECT * FROM $table")))
    or DbiError($dbh->err, $dbh->errstr);

  #
  # in this case, it should be NULL
  #
  Test($state or (! defined $rows->[0][1]))
    or DbiError($dbh->err, $dbh->errstr);

  #
  #   Finally drop the test table.
  #
  Test($state or $dbh->do("DROP TABLE $table"))
    or DbiError($dbh->err, $dbh->errstr);

  # 
  # disconnect
  #
  Test($state or ($dbh->disconnect()))
    or DbiError($dbh->err, $dbh->errstr);

}
