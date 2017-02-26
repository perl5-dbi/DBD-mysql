use strict;
use warnings;

use B qw(svref_2object SVf_IOK SVf_NOK SVf_POK SVf_IVisUV);
use Test::More;
use DBI;
use DBI::Const::GetInfoType;
use lib '.', 't';
require 'lib.pl';
$|= 1;

use vars qw($test_dsn $test_user $test_password);

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });
plan tests => 40;

ok(defined $dbh, "Connected to database");

ok($dbh->do(qq{DROP TABLE IF EXISTS t1}), "making slate clean");

ok($dbh->do(qq{CREATE TABLE t1 (num INT)}), "creating table");
ok($dbh->do(qq{INSERT INTO t1 VALUES (100)}), "loading data");

my ($val) = $dbh->selectrow_array("SELECT * FROM t1");
is($val, 100);

my $sv = svref_2object(\$val);
ok($sv->FLAGS & SVf_IOK, "scalar is integer");
ok(!($sv->FLAGS & (SVf_IVisUV|SVf_NOK|SVf_POK)), "scalar is not unsigned intger or double or string");

ok($dbh->do(qq{DROP TABLE t1}), "cleaning up");

ok($dbh->do(qq{CREATE TABLE t1 (num VARCHAR(10))}), "creating table");
ok($dbh->do(qq{INSERT INTO t1 VALUES ('string')}), "loading data");

($val) = $dbh->selectrow_array("SELECT * FROM t1");
is($val, "string");

$sv = svref_2object(\$val);
ok($sv->FLAGS & SVf_POK, "scalar is string");
ok(!($sv->FLAGS & (SVf_IOK|SVf_NOK)), "scalar is not intger or double");

ok($dbh->do(qq{DROP TABLE t1}), "cleaning up");

SKIP: {
skip "New Data types not supported by server", 26
if !MinimumVersion($dbh, '5.0');

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

my $ret = $dbh->selectall_arrayref("SELECT * FROM t1");
is_deeply($ret, [ [2.1],  [-1] ]);

$sv = svref_2object(\$ret->[0]->[0]);
ok($sv->FLAGS & SVf_NOK, "scalar is double");
ok(!($sv->FLAGS & (SVf_IOK|SVf_POK)), "scalar is not integer or string");

$sv = svref_2object(\$ret->[1]->[0]);
ok($sv->FLAGS & SVf_NOK, "scalar is double");
ok(!($sv->FLAGS & (SVf_IOK|SVf_POK)), "scalar is not integer or string");

ok($dbh->do(qq{DROP TABLE t1}), "cleaning up");

#
# [rt.cpan.org #19212] Mysql Unsigned Integer Fields
#
ok($dbh->do(qq{CREATE TABLE t1 (num INT UNSIGNED)}), "creating table");
ok($dbh->do(qq{INSERT INTO t1 VALUES (0),(4294967295)}), "loading data");

$ret = $dbh->selectall_arrayref("SELECT * FROM t1");
is_deeply($ret, [ [0],  [4294967295] ]);

$sv = svref_2object(\$ret->[0]->[0]);
ok($sv->FLAGS & (SVf_IOK|SVf_IVisUV), "scalar is unsigned integer");
ok(!($sv->FLAGS & (SVf_NOK|SVf_POK)), "scalar is not double or string");

$sv = svref_2object(\$ret->[1]->[0]);
ok($sv->FLAGS & (SVf_IOK|SVf_IVisUV), "scalar is unsigned integer");
ok(!($sv->FLAGS & (SVf_NOK|SVf_POK)), "scalar is not double or string");

ok($dbh->do(qq{DROP TABLE t1}), "cleaning up");
};

$dbh->disconnect();

