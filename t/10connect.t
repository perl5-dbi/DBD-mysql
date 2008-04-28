#!perl -w
# vim: ft=perl

use Test::More ;
use DBI;
use DBI::Const::GetInfoType;
use strict;
use vars qw($mdriver);
$|= 1;

our ($mdriver, $test_dsn, $test_user, $test_password);
$mdriver = "";
use lib 't', '.';
require 'lib.pl';

my @dsn;
my $dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

if (! defined $dbh) {
    plan skip_all => "Can't connect to database. Can't continue test";
}
plan tests => 2; 

ok(defined $dbh, "Connected to database");

ok($dbh->disconnect());
