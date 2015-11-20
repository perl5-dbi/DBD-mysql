use strict;
use warnings;

use Test::More ;
use DBI;
$|= 1;

use vars qw($test_user $test_password $test_db $test_dsn);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect('DBI:mysql:', $test_user, $test_password,
                      { RaiseError => 0, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    diag $@;
    plan skip_all => "no database connection";
}
plan tests => 3;

ok defined $dbh, "Connected to database";
eval{ $dbh->do("CREATE DATABASE IF NOT EXISTS $test_db") };
ok(!$@, 'CREATE DATABASE IF NOT EXISTS');
diag $@ if $@;

ok $dbh->disconnect();
