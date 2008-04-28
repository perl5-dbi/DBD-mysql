#!perl -w
# vim: ft=perl

use Test::More;
use DBI;
use DBI::Const::GetInfoType;
use strict;
$|= 1;

our ($mdriver, $test_dsn, $test_user, $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

if (! defined $dbh) {
    plan skip_all => "Can't obtain driver handle. Can't continue test";
}
plan tests => 4;

ok(defined $dbh, "Connected to database");

ok($dbh->do(qq{DROP TABLE IF EXISTS t1}), "making slate clean");

ok($dbh->do(qq{CREATE TABLE t1 (id INT(4), name VARCHAR(64))}), "creating table");

ok($dbh->do(qq{DROP TABLE t1}), "dropping created table");

$dbh->disconnect();

