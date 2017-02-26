use strict;
use warnings;

use Test::More ;
use DBI;
$|= 1;

use vars qw($test_user $test_password $test_db $test_dsn);
use lib 't', '.';
require 'lib.pl';

# remove database from DSN
$test_dsn =~ s/^DBI:mysql:([^:;]+)([:;]?)/DBI:mysql:$2/;

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });
plan tests => 2;

ok defined $dbh, "Connected to database";
eval{ $dbh->do("CREATE DATABASE IF NOT EXISTS $test_db") };
if($@) {
    if ( $ENV{CONNECTION_TESTING} ) {
        BAIL_OUT "No permission to '$test_db' database on '$test_dsn' for user '$test_user': $@";
    } else {
        diag "No permission to '$test_db' database on '$test_dsn' for user '$test_user': $@";
    }
} else {
    diag "Database '$test_db' accessible";
}

ok $dbh->disconnect();
