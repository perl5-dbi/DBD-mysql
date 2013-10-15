#!/usr/bin/perl

use strict;
use warnings;

use Test::More ;
use DBI;
use vars qw($mdriver);
$|= 1;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};

if ($@) {
    plan skip_all => "ERROR: $DBI::errstr Can't continue test";
}
plan tests => 2;

ok defined $dbh, "Connected to database";

ok $dbh->disconnect();
