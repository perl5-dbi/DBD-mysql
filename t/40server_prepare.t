#!perl -w
# vim: ft=perl

use Test::More tests => 9;
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

$test_dsn.= ";mysql_server_prepare=1";

my $dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });
ok(defined $dbh, "connecting");

ok($dbh->do(qq{DROP TABLE IF EXISTS t1}), "making slate clean");

#
# Bug #20559: Program crashes when using server-side prepare
#
ok($dbh->do(qq{CREATE TABLE t1 (id INT, num DOUBLE)}), "creating table");
ok($dbh->do(qq{INSERT INTO t1 VALUES (1,3.0),(2,-4.5)}), "loading data");

my $sth= $dbh->prepare("SELECT num FROM t1 WHERE id = ? FOR UPDATE");
ok($sth->bind_param(1, 1), "binding parameter");
ok($sth->execute(), "fetching data");
is_deeply($sth->fetchall_arrayref({}), [ { 'num' => '3' } ]);
ok($sth->finish);

ok($dbh->do(qq{DROP TABLE t1}), "cleaning up");

$dbh->disconnect();

