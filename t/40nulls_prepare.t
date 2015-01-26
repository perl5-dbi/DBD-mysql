use strict;
use warnings;

use Test::More;
use DBI;
use Carp qw(croak);
use lib 't', '.';
require 'lib.pl';

my ($row, $sth, $dbh);
my ($table, $def, $rows, $errstr, $ret_ref);
use vars qw($table $test_dsn $test_user $test_password);

eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
    { RaiseError => 1, AutoCommit => 1});};

if ($@) {
    plan skip_all => "no database connection",
}

ok(defined $dbh, "Connected to database");

ok($dbh->do("DROP TABLE IF EXISTS dbd_mysql_t40nullsprepare"), "Making slate clean");

my $create= <<EOSQL;
CREATE TABLE dbd_mysql_t40nullsprepare (
    id int,
    value0 varchar(10),
    value1 varchar(10),
    value2 varchar(10))
EOSQL

ok($dbh->do($create), "creating test table for bug 49719");

my ($sth_insert, $sth_lookup);

my $insert= 'INSERT INTO dbd_mysql_t40nullsprepare (id, value0, value1, value2) VALUES (?, ?, ?, ?)';

ok($sth_insert= $dbh->prepare($insert), "Prepare of insert");

my $select= "SELECT * FROM dbd_mysql_t40nullsprepare WHERE id = ?";

ok($sth_lookup= $dbh->prepare($select), "Prepare of query");

# Insert null value
ok($sth_insert->bind_param(1, 42, DBI::SQL_WVARCHAR), "bind_param(1,42, SQL_WARCHAR)");
ok($sth_insert->bind_param(2, 102, DBI::SQL_WVARCHAR), "bind_param(2,102,SQL_WARCHAR");
ok($sth_insert->bind_param(3, undef, DBI::SQL_WVARCHAR), "bind_param(3, undef,SQL_WVARCHAR)");
ok($sth_insert->bind_param(4, 10004, DBI::SQL_WVARCHAR), "bind_param(4, 10004,SQL_WVARCHAR)");
ok($sth_insert->execute(), "Executing the first insert");

# Insert afterwards none null value
# The bug would insert (DBD::MySQL-4.012) corrupted data....
# incorrect use of MYSQL_TYPE_NULL in prepared statement in dbdimp.c
ok($sth_insert->bind_param(1, 43, DBI::SQL_WVARCHAR),"bind_param(1,43,SQL_WVARCHAR)");
ok($sth_insert->bind_param(2, 2002, DBI::SQL_WVARCHAR),"bind_param(2,2002,SQL_WVARCHAR)");
ok($sth_insert->bind_param(3, 20003, DBI::SQL_WVARCHAR),"bind_param(3,20003,SQL_WVARCHAR)");
ok($sth_insert->bind_param(4, 200004, DBI::SQL_WVARCHAR),"bind_param(4,200004,SQL_WVARCHAR)");
ok($sth_insert->execute(), "Executing the 2nd insert");

# verify
ok($sth_lookup->execute(42), "Query for record of id = 42");
is_deeply($sth_lookup->fetchrow_arrayref(), [42, 102, undef, 10004]);

ok($sth_lookup->execute(43), "Query for record of id = 43");
is_deeply($sth_lookup->fetchrow_arrayref(), [43, 2002, 20003, 200004]);

ok($sth_insert->finish());
ok($sth_lookup->finish());

ok $dbh->do("DROP TABLE dbd_mysql_t40nullsprepare");

ok($dbh->disconnect(), "Testing disconnect");

done_testing;
