use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
require "t/lib.pl";

my $dbh1 = eval { DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1, AutoCommit => 0 }) };
plan skip_all => "no database connection" if $@ or not $dbh1;

my $dbh2 = eval { DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1, AutoCommit => 0 }) };
plan skip_all => "no database connection" if $@ or not $dbh2;

ok $dbh1->do("DROP TABLE IF EXISTS dbd_mysql_rt75353_innodb_lock_timeout"), "drop table if exists dbd_mysql_rt75353_innodb_lock_timeout";
ok $dbh1->do("CREATE TABLE dbd_mysql_rt75353_innodb_lock_timeout(id INT PRIMARY KEY) ENGINE=INNODB"), "create table dbd_mysql_rt75353_innodb_lock_timeout";

ok $dbh2->do("SET innodb_lock_wait_timeout=1"), "dbh2: set innodb_lock_wait_timeout to one second";

ok $dbh1->do("INSERT INTO dbd_mysql_rt75353_innodb_lock_timeout VALUES(1)"), "dbh1: acquire a row lock on table dbd_mysql_rt75353_innodb_lock_timeout";

my $error_handler_called = 0;
$dbh2->{HandleError} = sub { $error_handler_called = 1; die $_[0]; };
eval { $dbh2->selectcol_arrayref("SELECT id FROM dbd_mysql_rt75353_innodb_lock_timeout FOR UPDATE") };
my $error_message = $@;
$dbh2->{HandleError} = undef;
ok $error_message, "dbh2: acquiring same lock as dbh1 on table dbd_mysql_rt75353_innodb_lock_timeout failed";

like $error_message, qr/Lock wait timeout exceeded; try restarting transaction/, "dbh2: error message for acquiring lock is 'Lock wait timeout exceeded'";
ok $error_handler_called, "dbh2: error handler code ref was called";

$dbh2->disconnect();

ok $dbh1->do("DROP TABLE dbd_mysql_rt75353_innodb_lock_timeout"), "drop table dbd_mysql_rt75353_innodb_lock_timeout";
$dbh1->disconnect();

done_testing;
