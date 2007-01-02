#!perl -w
# vim: ft=perl

use Test::More tests => 16;
use DBI;
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
ok(defined $dbh, "connecting");

ok($dbh->do(qq{DROP TABLE IF EXISTS t1}), "making slate clean");

ok($dbh->do(qq{CREATE TABLE t1 (d DECIMAL(5,2))}), "creating table");

my $sth= $dbh->prepare("SELECT * FROM t1 WHERE 1 = 0");
ok($sth->execute(), "getting table information");

is_deeply($sth->{TYPE}, [ 3 ], "checking column type");

ok($sth->finish);

ok($dbh->do(qq{DROP TABLE t1}), "cleaning up");

#
# Bug #23936: bind_param() doesn't work with SQL_DOUBLE datatype
# Bug #24256: Another failure in bind_param() with SQL_DOUBLE datatype
#
ok($dbh->do(qq{CREATE TABLE t1 (num DOUBLE)}), "creating table");

$sth= $dbh->prepare("INSERT INTO t1 VALUES (?)");
ok($sth->bind_param(1, 2.1, DBI::SQL_DOUBLE), "binding parameter");
ok($sth->execute(), "inserting data");
ok($sth->finish);
ok($sth->bind_param(1, -1, DBI::SQL_DOUBLE), "binding parameter");
ok($sth->execute(), "inserting data");
ok($sth->finish);

is_deeply($dbh->selectall_arrayref("SELECT * FROM t1"), [ ['2.1'],  ['-1'] ]);

ok($dbh->do(qq{DROP TABLE t1}), "cleaning up");

$dbh->disconnect();
