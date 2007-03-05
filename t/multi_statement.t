#!perl -w
# vim: ft=perl

use Test::More tests => 7;
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
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0,
                        mysql_multi_statements => 1 });
ok(defined $dbh, "Connected to database with multi statement support");

ok($dbh->do("DROP TABLE IF EXISTS t1"), "clean up");
ok($dbh->do("CREATE TABLE t1 (a INT)"), "create table");

ok($dbh->do("INSERT INTO t1 VALUES (1); INSERT INTO t1 VALUES (2);"));

$dbh->disconnect();

$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                   { RaiseError => 0, PrintError => 0, AutoCommit => 0,
                     mysql_multi_statements => 0 });
ok(defined $dbh, "Connected to database without multi statement support");

ok(not $dbh->do("INSERT INTO t1 VALUES (1); INSERT INTO t1 VALUES (2);"));

ok($dbh->do("DROP TABLE IF EXISTS t1"), "clean up");

$dbh->disconnect();
