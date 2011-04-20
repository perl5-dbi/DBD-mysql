#!perl -w
# vim: ft=perl

use Test::More;
use DBI;
use DBI::Const::GetInfoType;
use Time::HiRes qw(clock_gettime CLOCK_REALTIME);
use strict;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 0, PrintError => 0, AutoCommit => 0 });};

unless($dbh) {
    plan skip_all => "ERROR: $DBI::errstr Can't continue test";
}
unless($dbh->get_info($GetInfoType{'SQL_ASYNC_MODE'})) {
    plan skip_all => "Async support wasn't built into this version of DBD::mysql";
}
plan tests => 57;

is $dbh->get_info($GetInfoType{'SQL_ASYNC_MODE'}), 2; # statement-level async
is $dbh->get_info($GetInfoType{'SQL_MAX_ASYNC_CONCURRENT_STATEMENTS'}), 1;

$dbh->do(<<SQL);
CREATE TEMPORARY TABLE async_test (
    value0 INTEGER,
    value1 INTEGER,
    value2 INTEGER
);
SQL

ok $dbh->mysql_fd;
ok !defined($dbh->mysql_async_ready);

my ( $start, $end );
my $rows;
my $sth;
my ( $a, $b, $c );

$start = clock_gettime(CLOCK_REALTIME);
$rows = $dbh->do('INSERT INTO async_test VALUES (SLEEP(5), 0, 0)');
$end = clock_gettime(CLOCK_REALTIME);

is $rows, 1;
ok(($end - $start) >= 5);

$start = clock_gettime(CLOCK_REALTIME);
$rows = $dbh->do('INSERT INTO async_test VALUES (SLEEP(5), 0, 0)', { async => 1 });
ok defined($dbh->mysql_async_ready);
$end = clock_gettime(CLOCK_REALTIME);

ok $rows;
is $rows, '0E0';

ok(($end - $start) < 5);

sleep 1 until $dbh->mysql_async_ready;
$end = clock_gettime(CLOCK_REALTIME);
ok(($end - $start) >= 5);

$rows = $dbh->mysql_async_result;
ok !defined($dbh->mysql_async_ready);

is $rows, 1;

( $rows ) = $dbh->selectrow_array('SELECT COUNT(1) FROM async_test');

is $rows, 2;

$dbh->do('DELETE FROM async_test');

$start = clock_gettime(CLOCK_REALTIME);
$rows = $dbh->do('INSERT INTO async_test VALUES(SLEEP(5), ?, ?)', { async => 1 }, 1, 2);
$end = clock_gettime(CLOCK_REALTIME);

ok $rows;
is $rows, '0E0';

ok(($end - $start) < 5);

sleep 1 until $dbh->mysql_async_ready;
$end = clock_gettime(CLOCK_REALTIME);
ok(($end - $start) >= 5);

$rows = $dbh->mysql_async_result;

is $rows, 1;

( $a, $b, $c ) = $dbh->selectrow_array('SELECT * FROM async_test');

is $a, 0;
is $b, 1;
is $c, 2;

$sth = $dbh->prepare('SELECT SLEEP(5)');
ok !defined($sth->mysql_async_ready);
$start = clock_gettime(CLOCK_REALTIME);
ok $sth->execute;
$end = clock_gettime(CLOCK_REALTIME);
ok(($end - $start) >= 5);

$sth = $dbh->prepare('SELECT SLEEP(5)', { async => 1 });
ok !defined($sth->mysql_async_ready);
$start = clock_gettime(CLOCK_REALTIME);
ok $sth->execute;
ok defined($sth->mysql_async_ready);
$end = clock_gettime(CLOCK_REALTIME);
ok(($end - $start) < 5);

sleep 1 until $sth->mysql_async_ready;

my $row = $sth->fetch;
$end = clock_gettime(CLOCK_REALTIME);
ok $row;
is $row->[0], 0;
ok(($end - $start) >= 5);

$rows = $dbh->do('INSERT INTO async_test VALUES(SLEEP(5), ?, ?', { async => 1 }, 1, 2);

ok $rows;
ok !$dbh->errstr;
$rows = $dbh->mysql_async_result;
ok !$rows;
ok $dbh->errstr;

$dbh->do('DELETE FROM async_test');

$sth = $dbh->prepare('INSERT INTO async_test VALUES(SLEEP(5), ?, ?)', { async => 1 });
$start = clock_gettime(CLOCK_REALTIME);
$rows = $sth->execute(1, 2);
$end = clock_gettime(CLOCK_REALTIME);
ok(($end - $start) < 5);
ok $rows;
is $rows, '0E0';

$rows = $sth->mysql_async_result;
$end = clock_gettime(CLOCK_REALTIME);
ok(($end - $start) >= 5);
is $rows, 1;

( $a, $b, $c ) = $dbh->selectrow_array('SELECT * FROM async_test');

is $a, 0;
is $b, 1;
is $c, 2;

$sth  = $dbh->prepare('INSERT INTO async_test VALUES(SLEEP(5), ?, ?)', { async => 1 });
$rows = $dbh->do('INSERT INTO async_test VALUES(SLEEP(5), ?, ?)', undef, 1, 2);
is $rows, 1;

$start = clock_gettime(CLOCK_REALTIME);
$dbh->selectrow_array('SELECT SLEEP(5)', { async => 1 });
$end = clock_gettime(CLOCK_REALTIME);

ok(($end - $start) >= 5);
ok !defined($dbh->mysql_async_result);
ok !defined($dbh->mysql_async_ready);

$rows = $dbh->do('UPDATE async_test SET value0 = 0 WHERE value0 = 999', { async => 1 });
ok $rows;
is $rows, '0E0';
$rows = $dbh->mysql_async_result;
ok $rows;
is $rows, '0E0';

$sth  = $dbh->prepare('UPDATE async_test SET value0 = 0 WHERE value0 = 999', { async => 1 });
$rows = $sth->execute;
ok $rows;
is $rows, '0E0';
$rows = $sth->mysql_async_result;
ok $rows;
is $rows, '0E0';

undef $sth;
ok $dbh->disconnect;
