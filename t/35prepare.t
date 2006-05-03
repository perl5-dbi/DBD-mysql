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

  # don't want this during make test!
  Test($state or (1 || $dbh->trace("3", "/tmp/trace.log"))) or
   DbiError($dbh->err, $dbh->errstr);

  Test($state or $table = FindNewTable($dbh)) or
    DbiError($dbh->err, $dbh->errstr); 

  Test($state or ($def = TableDefinition($table,
    ["id",   "INTEGER",  4, 0],
    ["name", "CHAR",    64, 0]),
  $dbh->do($def)))
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth = $dbh->prepare("SHOW TABLES LIKE '$table'"))
    or DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute())
    or DbiError($dbh->err, $dbh->errstr);

  Test(
    $state or 
    (defined($row= $sth->fetchrow_arrayref)  &&
    (!defined($errstr = $sth->errstr) || $sth->errstr eq '')))
         or DbiError($sth->err, $sth->errstr);

  Test ($state or ($row->[0] eq "$table")) 
      or print "results not equal to '$table' \n";

  Test($state or $sth=
    $dbh->do("INSERT INTO $table VALUES (1,'1st first value')")) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth=
    $dbh->prepare("INSERT INTO $table VALUES (1,'2nd second value')")) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $rows = $sth->execute()) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->finish) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth=
    $dbh->prepare("SELECT id, name FROM $table WHERE id = 1")) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute()) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $ret_ref = $sth->fetchall_arrayref()) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth=
    $dbh->prepare("INSERT INTO $table values (?, ?)"))
    or DbiError($dbh->err, $dbh->errstr);
	
  my $testInsertVals = {};
  for (my $i = 0 ; $i < 10; $i++)
  { 
    my @chars = grep !/[0O1Iil]/, 0..9, 'A'..'Z', 'a'..'z';
    my $random_chars= join '', map { $chars[rand @chars] } 0 .. 16;
    # save these values for later testing
    $testInsertVals->{$i}= $random_chars;
    Test($state or $rows= $sth->execute($i, $random_chars))
      or DbiError($dbh->err, $dbh->errstr);
  }
  Test($state or $sth->finish) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth=
    $dbh->prepare("SELECT * FROM $table WHERE id = ? OR id = ?")) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $rows = $sth->execute(1,2)) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $ret_ref = $sth->fetchall_arrayref()) or  
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth=
    $dbh->prepare("DROP TABLE IF EXISTS $table")) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute()) or 
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth=
    $dbh->prepare("DROP TABLE IF EXISTS t1")) or
    DbiError($dbh->err, $dbh->errstr);

  Test($state or $sth->execute()) or 
    DbiError($dbh->err, $dbh->errstr);
}
