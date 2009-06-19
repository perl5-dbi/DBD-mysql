#!perl -w
# vim: ft=perl

use Test::More;
use DBI;
use DBI::Const::GetInfoType;
use strict;
$|= 1;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};

if ($@) {
    plan skip_all => "ERROR: $DBI::errstr. Can't continue test";
}
plan tests => 8;

ok(defined $dbh, "Connected to database");

ok($dbh->{Active}, "checking for active handle");

ok($dbh->{mysql_auto_reconnect} = 1, "enabling reconnect");

ok($dbh->{AutoCommit} = 1, "enabling autocommit");

ok($dbh->disconnect(), "disconnecting active handle");

ok(!$dbh->{Active}, "checking for inactive handle");

ok($dbh->do("SELECT 1"), "implicitly reconnecting handle with 'do'");

ok($dbh->{Active}, "checking for reactivated handle");

$dbh->disconnect();
