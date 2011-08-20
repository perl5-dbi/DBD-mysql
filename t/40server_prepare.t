#!perl -w
# vim: ft=perl

use strict;
use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';
use vars qw($table $test_dsn $test_user $test_password);

$|= 1;

$test_dsn.= ";mysql_server_prepare=1";

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};

if ($@) {
    plan skip_all => "ERROR: $@. Can't continue test";
}
plan tests => 27; 

ok(defined $dbh, "connecting");

ok($dbh->do(qq{DROP TABLE IF EXISTS t1}), "making slate clean");

#
# Bug #20559: Program crashes when using server-side prepare
#
ok($dbh->do(qq{CREATE TABLE t1 (id INT, num DOUBLE)}), "creating table");

my $sth;
ok($sth= $dbh->prepare(qq{INSERT INTO t1 VALUES (?,?),(?,?)}), "loading data");
ok($sth->execute(1, 3.0, 2, -4.5));

ok ($sth= $dbh->prepare("SELECT num FROM t1 WHERE id = ? FOR UPDATE"));

ok ($sth->bind_param(1, 1), "binding parameter");

ok ($sth->execute(), "fetching data");

is_deeply($sth->fetchall_arrayref({}), [ { 'num' => '3' } ]);

ok ($sth->finish);

ok ($dbh->do(qq{DROP TABLE t1}), "cleaning up");

#
# Bug #42723: Binding server side integer parameters results in corrupt data
#
ok($dbh->do(qq{DROP TABLE IF EXISTS t2}), "making slate clean");

ok($dbh->do(q{CREATE TABLE `t2` (`i` int,`si` smallint,`ti` tinyint,`bi` bigint)}), "creating test table");

my $sth2;
ok($sth2 = $dbh->prepare('INSERT INTO t2 VALUES (?,?,?,?)'));

#bind test values
ok($sth2->bind_param(1, 101, DBI::SQL_INTEGER), "binding int");
ok($sth2->bind_param(2, 102, DBI::SQL_SMALLINT), "binding smallint");
ok($sth2->bind_param(3, 103, DBI::SQL_TINYINT), "binding tinyint");
ok($sth2->bind_param(4, 104, DBI::SQL_INTEGER), "binding bigint");

ok($sth2->execute(), "inserting data");

is_deeply($dbh->selectall_arrayref('SELECT * FROM t2'), [[101, 102, 103, 104]]);

ok ($dbh->do(qq{DROP TABLE t2}), "cleaning up");

#
# Bug LONGBLOB wants 4GB memory
#
ok($dbh->do(qq{DROP TABLE IF EXISTS t3}), "making slate clean");
ok($dbh->do(q{CREATE TABLE t3 (id INT, mydata LONGBLOB)}), "creating test table");
my $sth3;
ok($sth3 = $dbh->prepare(q{INSERT INTO t3 VALUES (?,?)}));
ok($sth3->execute(1, 2), "insert t3");

is_deeply($dbh->selectall_arrayref('SELECT id, mydata FROM t3'), [[1, 2]]);

ok ($dbh->do(qq{DROP TABLE t3}), "cleaning up");

$dbh->disconnect();
