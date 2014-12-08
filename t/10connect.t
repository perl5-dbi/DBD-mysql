#!/usr/bin/perl

use strict;
use warnings;

use Test::More ;
use DBI;
use vars qw($mdriver);
$|= 1;

use vars qw($test_dsn $test_user $test_password $test_db);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    if ($DBI::err == 1049) {
        my $create_table_dsn = "DBI:mysql:information_schema";
        my $create_dbh;
        eval { $create_dbh = DBI->connect($create_table_dsn, $test_user, $test_password);};
        if ($@) {
            Test::More::BAIL_OUT("ERROR: $DBI::errstr\nUnable to create missing db $test_db!");
            plan skip_all => "ERROR: $DBI::errstr $DBI::err Can't continue test";
        }
        Test::More::diag("$test_db does not exist! Creating...");
        $create_dbh->do("CREATE DATABASE $test_db");
        Test::More::diag("done.");
        eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
    }
    if ($@) {
        # https://rt.cpan.org/Ticket/Display.html?id=31823
        if ($DBI::err == 1045) {
            Test::More::BAIL_OUT("ERROR: $DBI::errstr\nAborting remaining tests!"); 
        }
        plan skip_all => "ERROR: $DBI::errstr $DBI::err Can't continue test";
    }
}
plan tests => 2;

ok defined $dbh, "Connected to database";

ok $dbh->disconnect();
