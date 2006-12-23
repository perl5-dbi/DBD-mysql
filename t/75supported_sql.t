#!/usr/bin/perl

use strict;
use vars qw($state $test_dsn $test_user $test_password $mdriver $dbdriver);
use DBI;
use Carp qw(croak);
use Data::Dumper;
$^W =1;

use DBI;
$mdriver = "";
my ($row, $vers, $test_procs);
foreach my $file ("lib.pl", "t/lib.pl", "DBD-mysql/t/lib.pl") {
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

while(Testing())
{
  my ($table, $errstr);
  Test($state or my $dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1})) or ServerError() ;

  # don't want this during make test!
  Test($state or (!$dbh->trace("3", "/tmp/trace.log"))) or
    DbiError($dbh->err, $dbh->errstr);

  #
  #   Create a new table; EDIT THIS!
  #
  Test($state or $table = FindNewTable($dbh)) or
    DbiError($dbh->err, $dbh->errstr); 

  Test($state or $dbh->do("DROP TABLE IF EXISTS $table")) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or my $sth= $dbh->prepare("
CREATE TABLE $table (
  id INT,
  name VARCHAR(32)
  )")) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or ($sth->execute())) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or
    $sth= $dbh->prepare("SHOW TABLES LIKE '$table'"))
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute())
	   or DbiError($dbh->err, $dbh->errstr);
  Test(
    $state or 
    (defined($row= $sth->fetchrow_arrayref)  &&
    (!defined($errstr= $sth->errstr) || $sth->errstr eq '')))
         or DbiError($sth->err, $sth->errstr);

  Test ($state or ($row->[0] eq "$table")) 
      or print "results not equal to '$table' \n";

  Test($state or
    $sth= $dbh->prepare("DROP TABLE $table")) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute()) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or
    $sth=$dbh->prepare("CREATE TABLE $table (a int)")) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute()) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or
    $sth= $dbh->prepare("ALTER TABLE $table ADD COLUMN b varchar(31)")) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute()) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or
    $sth = $dbh->prepare("DROP TABLE $table")) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute()) or 
    DbiError($dbh->err, $dbh->errstr);
}
