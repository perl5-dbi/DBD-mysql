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
    # https://rt.cpan.org/Ticket/Display.html?id=31823
    if ($DBI::err == 1045) {
        Test::More::BAIL_OUT("ERROR: $DBI::errstr\nAborting remaining tests!"); 
    }
    plan skip_all => "ERROR: $DBI::errstr $DBI::err Can't continue test";
}
plan tests => 2;

ok defined $dbh, "Connected to database";

ok $dbh->disconnect();
