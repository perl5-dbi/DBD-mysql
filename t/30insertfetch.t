use strict;
use warnings;

use Test::More;
use DBI;
use DBI::Const::GetInfoType;
use lib 't', '.';
require 'lib.pl';
$|= 1;

use vars qw($test_dsn $test_user $test_password);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all =>
        "no database connection";
}
plan tests => 10;

ok(defined $dbh, "Connected to database");

ok($dbh->do("DROP TABLE IF EXISTS dbd_mysql_t30"), "making slate clean");

ok($dbh->do("CREATE TABLE dbd_mysql_t30 (id INT(4), name VARCHAR(64))"), "creating table");

ok($dbh->do("INSERT INTO dbd_mysql_t30 VALUES(1, 'Alligator Descartes')"), "loading data");

ok($dbh->do("DELETE FROM dbd_mysql_t30 WHERE id = 1"), "deleting from table dbd_mysql_t30");

ok (my $sth= $dbh->prepare("SELECT * FROM dbd_mysql_t30 WHERE id = 1"));

ok($sth->execute());

ok(not $sth->fetchrow_arrayref());

ok($sth->finish());

ok($dbh->do("DROP TABLE dbd_mysql_t30"),"Dropping table");

$dbh->disconnect();
