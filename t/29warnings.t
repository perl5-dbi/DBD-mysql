#!perl -w
# vim: ft=perl

use Test::More;
use DBI;
use lib '.', 't';
require 'lib.pl';
use strict;
$|= 1;

use vars qw($table $test_dsn $test_user $test_password);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0});};

if ($@) {
    plan skip_all => "ERROR: $@. Can't continue test";
}
plan tests => 8;

ok(defined $dbh, "Connected to database");

SKIP: {
  skip "Server doesn't report warnings", 7
    if !CheckMinimumVersion($dbh, '4.1');

  my $sth;
  ok($sth= $dbh->prepare("DROP TABLE IF EXISTS no_such_table"));
  ok($sth->execute());

  is($sth->{mysql_warning_count}, 1, 'warnings from sth');

  ok($dbh->do("SET sql_mode=''"));
  ok($dbh->do("CREATE TEMPORARY TABLE t (c CHAR(1))"));
  ok($dbh->do("INSERT INTO t (c) VALUES ('perl'), ('dbd'), ('mysql')"));
  is($dbh->{mysql_warning_count}, 3, 'warnings from dbh');
};

$dbh->disconnect;
