use strict;
use warnings;

use Test::More;
use DBI;
use DBI::Const::GetInfoType;
use lib '.', 't';
require 'lib.pl';
$|= 1;

use vars qw($table $test_dsn $test_user $test_password);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
print "err perl $@\n";
if ($@) {
    plan skip_all =>
        "no database connection";
}
plan tests => 20;

ok(defined $dbh, "Connected to database");

SKIP: {
skip "New Data types not supported by server", 19
if !MinimumVersion($dbh, '5.0');

ok($dbh->do(qq{DROP TABLE IF EXISTS t1}), "making slate clean");

ok($dbh->do(qq{CREATE TABLE t1 (d DECIMAL(5,2))}), "creating table");

my $sth= $dbh->prepare("SELECT * FROM t1 WHERE 1 = 0");
ok($sth->execute(), "getting table information");

is_deeply($sth->{TYPE}, [ 3 ], "checking column type");

ok($sth->finish);

ok($dbh->do(qq{DROP TABLE t1}), "cleaning up");

#
# Bug #23936: bind_param() doesn't work with SQL_DOUBLE datatype
# Bug #24256: Another failure in bind_param() with SQL_DOUBLE datatype
#
ok($dbh->do(qq{CREATE TABLE t1 (num DOUBLE)}), "creating table");

$sth= $dbh->prepare("INSERT INTO t1 VALUES (?)");
ok($sth->bind_param(1, 2.1, DBI::SQL_DOUBLE), "binding parameter");
ok($sth->execute(), "inserting data");
ok($sth->finish);
ok($sth->bind_param(1, -1, DBI::SQL_DOUBLE), "binding parameter");
ok($sth->execute(), "inserting data");
ok($sth->finish);

is_deeply($dbh->selectall_arrayref("SELECT * FROM t1"), [ ['2.1'],  ['-1'] ]);

ok($dbh->do(qq{DROP TABLE t1}), "cleaning up");

#
# [rt.cpan.org #19212] Mysql Unsigned Integer Fields
#
ok($dbh->do(qq{CREATE TABLE t1 (num INT UNSIGNED)}), "creating table");
ok($dbh->do(qq{INSERT INTO t1 VALUES (0),(4294967295)}), "loading data");

is_deeply($dbh->selectall_arrayref("SELECT * FROM t1"),
          [ ['0'],  ['4294967295'] ]);

ok($dbh->do(qq{DROP TABLE t1}), "cleaning up");
};

$dbh->disconnect();

