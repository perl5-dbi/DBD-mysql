#!perl -w
# vim: ft=perl

use Test::More tests => 9;
use DBI;
use DBI::Const::GetInfoType;
use strict;
$|= 1;

my $mdriver= "";
our ($test_dsn, $test_user, $test_password);
foreach my $file ("lib.pl", "t/lib.pl") {
  do $file;
  if ($@) {
    print STDERR "Error while executing $file: $@\n";
    exit 10;
  }
  last if $mdriver ne '';
}

my $dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });
ok(defined $dbh, "Connected to database");

ok($dbh->do(qq{DROP TABLE IF EXISTS t1}), "making slate clean");

ok($dbh->do(qq{CREATE TABLE t1 (id INT(4), name VARCHAR(64))}), "creating table");

ok($dbh->do("INSERT INTO t1 VALUES(1, 'Alligator Descartes')"), "loading data");

ok($dbh->do("DELETE FROM t1 WHERE id = 1"), "deleting from table t1");

my $sth= $dbh->prepare("SELECT * FROM t1 WHERE id = 1");

ok($sth->execute());

ok(not $sth->fetchrow_arrayref());

ok($sth->finish());

ok($dbh->do("DROP TABLE t1"),"Dropping table");

$dbh->disconnect();


