#!/usr/bin/perl

use strict;
use vars qw($test_dsn $test_user $test_password $mdriver $state);
use DBI;
use Carp qw(croak);
use Data::Dumper;

$^W =1;


use DBI;
$mdriver = "";
my ($row, $sth, $dbh);
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
  my ($table, $def, $rows, $errstr, $ret_ref);
  Test($state or $dbh =
    DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1})) or ServerError() ;

  #Test($state or (!$dbh->trace("3", "/tmp/trace.log"))) or
  # DbiError($dbh->err, $dbh->errstr);

  Test($state or $table = FindNewTable($dbh)) or
    DbiError($dbh->err, $dbh->errstr); 
    
  Test($state or 
    $dbh->do("create table $table (a int not null, b double, primary key (a))")) or
    DbiError($dbh->err, $dbh->errstr); 

  Test($state or 
    $sth= $dbh->prepare("insert into $table values (?, ?)")) or
    DbiError($dbh->err, $dbh->errstr); 

  Test($state or
    $sth->bind_param(1,"10000 ",DBI::SQL_INTEGER)) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or
    $sth->bind_param(2,"1.22 ",DBI::SQL_DOUBLE)) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or
    $sth->execute()) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or
    $sth->bind_param(1,10001,DBI::SQL_INTEGER)) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or
    $sth->bind_param(2,.3333333,DBI::SQL_DOUBLE)) or
    DbiError($dbh->err, $dbh->errstr);
  
  Test ($state or
    $sth->execute()) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $dbh->do("DROP TABLE $table")) or
    DbiError($dbh->err, $dbh->errstr);
}
