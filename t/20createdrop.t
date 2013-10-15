#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use DBI;
$|= 1;

use vars qw($table $test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};

if ($@) {
    plan skip_all => "ERROR: $DBI::errstr. Can't continue test";
}
plan tests => 4;

ok(defined $dbh, "Connected to database");

ok($dbh->do("DROP TABLE IF EXISTS $table"), "making slate clean");

ok($dbh->do("CREATE TABLE $table (id INT(4), name VARCHAR(64))"), "creating $table");

ok($dbh->do("DROP TABLE $table"), "dropping created $table");

$dbh->disconnect();
