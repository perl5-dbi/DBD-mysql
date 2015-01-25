use strict;
use warnings;

use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';
use vars qw($table $test_dsn $test_user $test_password);

$|= 1;

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });};
if ($@) {
    plan skip_all => "no database connection";
}
plan tests => 11;

ok(defined $dbh, "connecting");

#
# Bug #42723: Binding server side integer parameters results in corrupt data
#
ok($dbh->do('DROP TABLE IF EXISTS t1'), "making slate clean");

ok($dbh->do('CREATE TABLE `t1` (`i` int,`si` smallint,`ti` tinyint,`bi` bigint)'), "creating test table");

my $sth2;
ok($sth2 = $dbh->prepare('INSERT INTO t1 VALUES (?,?,?,?)'));

#bind test values
ok($sth2->bind_param(1, 101, DBI::SQL_INTEGER), "binding int");
ok($sth2->bind_param(2, 102, DBI::SQL_SMALLINT), "binding smallint");
ok($sth2->bind_param(3, 103, DBI::SQL_TINYINT), "binding tinyint");
ok($sth2->bind_param(4, 104, DBI::SQL_INTEGER), "binding bigint");

ok($sth2->execute(), "inserting data");

is_deeply($dbh->selectall_arrayref('SELECT * FROM t1'), [[101, 102, 103, 104]]);

ok ($dbh->do('DROP TABLE t1'), "cleaning up");

$dbh->disconnect();
