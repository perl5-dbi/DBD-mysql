use strict;
use warnings;

use Test::More ;
use DBI;
use vars qw($mdriver);
$|= 1;

use vars qw($test_user $test_password $test_db $test_dsn);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all => "no database connection";
}
plan tests => 3;

ok defined $dbh, "Connected to database";
ok $dbh->do("CREATE DATABASE IF NOT EXISTS $test_db");
ok $dbh->disconnect();
