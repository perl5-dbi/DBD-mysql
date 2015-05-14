use strict;
use warnings;

use DBI;
use Test::More;
use vars qw($test_dsn $test_user $test_password);
# use vars qw($COL_NULLABLE $COL_KEY);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all => "no database connection";
}

if (!MinimumVersion($dbh, '5.5')) {
    plan skip_all =>
        "SKIP TEST: You must have MySQL version 5.5 and greater for this test to run";
}
plan tests => 6;

$dbh = DBI->connect($test_dsn . ';mysql_enable_utf8mb4=1', $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t55utf8mb4");
ok $dbh->do("CREATE TABLE dbd_mysql_t55utf8mb4 (id SERIAL, val TEXT CHARACTER SET utf8mb4)");

my $sth = $dbh->prepare("INSERT INTO dbd_mysql_t55utf8mb4(val) VALUES('😈')");
$sth->execute();

my $query = "SELECT val, HEX(val) FROM dbd_mysql_t55utf8mb4 LIMIT 1";
$sth = $dbh->prepare($query) or die "$DBI::errstr";
ok $sth->execute;

my $ref;
$ref = $sth->fetchrow_arrayref ;
$sth->finish;

ok defined $ref;

cmp_ok $ref->[0], 'eq', "😈";
cmp_ok $ref->[1], 'eq', "F09F9888";

$dbh->disconnect();
