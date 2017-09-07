use strict;
use warnings;

use Test::Deep;
use Test::More;
use DBI;
use DBI::Const::GetInfoType;
use Time::HiRes;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 0, PrintError => 0, AutoCommit => 0 });};
if (!$dbh) {
    plan skip_all => "no database connection";
}
unless($dbh->get_info($GetInfoType{'SQL_ASYNC_MODE'})) {
    plan skip_all => "Async support wasn't built into this version of DBD::mysql";
}
plan tests => 92;

is $dbh->get_info($GetInfoType{'SQL_ASYNC_MODE'}), 2; # statement-level async
is $dbh->get_info($GetInfoType{'SQL_MAX_ASYNC_CONCURRENT_STATEMENTS'}), 1;

$dbh->do("DROP TABLE IF EXISTS async_test");

$dbh->do(<<SQL);
CREATE TABLE async_test (
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

$start = Time::HiRes::gettimeofday();
$rows = $dbh->do('INSERT INTO async_test VALUES (SLEEP(2), 0, 0)');
$end = Time::HiRes::gettimeofday();

is $rows, 1;
ok(($end - $start) >= 2);

$start = Time::HiRes::gettimeofday();
$rows = $dbh->do('INSERT INTO async_test VALUES (SLEEP(2), 0, 0)', { async => 1 });
ok defined($dbh->mysql_async_ready);
$end = Time::HiRes::gettimeofday();

ok $rows;
is $rows, '0E0';

ok(($end - $start) < 2);

sleep 1 until $dbh->mysql_async_ready;
$end = Time::HiRes::gettimeofday();
ok(($end - $start) >= 2);

$rows = $dbh->mysql_async_result;
ok !defined($dbh->mysql_async_ready);

is $rows, 1;

( $rows ) = $dbh->selectrow_array('SELECT COUNT(1) FROM async_test');

is $rows, 2;

$dbh->do('DELETE FROM async_test');

$start = Time::HiRes::gettimeofday();
$rows = $dbh->do('INSERT INTO async_test VALUES(SLEEP(2), ?, ?)', { async => 1 }, 1, 2);
$end = Time::HiRes::gettimeofday();

ok $rows;
is $rows, '0E0';

ok(($end - $start) < 2);

sleep 1 until $dbh->mysql_async_ready;
$end = Time::HiRes::gettimeofday();
ok(($end - $start) >= 2);

$rows = $dbh->mysql_async_result;

is $rows, 1;

( $a, $b, $c ) = $dbh->selectrow_array('SELECT * FROM async_test');

is $a, 0;
is $b, 1;
is $c, 2;

$sth = $dbh->prepare('SELECT SLEEP(2)');
ok !defined($sth->mysql_async_ready);
$start = Time::HiRes::gettimeofday();
ok $sth->execute;
$end = Time::HiRes::gettimeofday();
ok(($end - $start) >= 2);

$sth = $dbh->prepare('SELECT SLEEP(2)', { async => 1 });
ok !defined($sth->mysql_async_ready);
$start = Time::HiRes::gettimeofday();
ok $sth->execute;
ok defined($sth->mysql_async_ready);
$end = Time::HiRes::gettimeofday();
ok(($end - $start) < 2);

sleep 1 until $sth->mysql_async_ready;

my $row = $sth->fetch;
$end = Time::HiRes::gettimeofday();
ok $row;
is $row->[0], 0;
ok(($end - $start) >= 2);

$rows = $dbh->do('INSERT INTO async_test VALUES(SLEEP(2), ?, ?', { async => 1 }, 1, 2);

ok $rows;
ok !$dbh->errstr;
$rows = $dbh->mysql_async_result;
ok !$rows;
ok $dbh->errstr;

$dbh->do('DELETE FROM async_test');

$sth = $dbh->prepare('INSERT INTO async_test VALUES(SLEEP(2), ?, ?)', { async => 1 });
$start = Time::HiRes::gettimeofday();
$rows = $sth->execute(1, 2);
$end = Time::HiRes::gettimeofday();
ok(($end - $start) < 2);
ok $rows;
is $rows, '0E0';

$rows = $sth->mysql_async_result;
$end = Time::HiRes::gettimeofday();
ok(($end - $start) >= 2);
is $rows, 1;

( $a, $b, $c ) = $dbh->selectrow_array('SELECT * FROM async_test');

is $a, 0;
is $b, 1;
is $c, 2;

$sth  = $dbh->prepare('INSERT INTO async_test VALUES(SLEEP(2), ?, ?)', { async => 1 });
$rows = $dbh->do('INSERT INTO async_test VALUES(SLEEP(2), ?, ?)', undef, 1, 2);
is $rows, 1;

$start = Time::HiRes::gettimeofday();
$dbh->selectrow_array('SELECT SLEEP(2)', { async => 1 });
$end = Time::HiRes::gettimeofday();

ok(($end - $start) >= 2);
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

$sth->execute;
$rows = $dbh->do('INSERT INTO async_test VALUES(1, 2, 3)');
ok !$rows;
undef $sth;
$rows = $dbh->do('INSERT INTO async_test VALUES(1, 2, 3)');
is $rows, 1;

$sth = $dbh->prepare('SELECT 1, value0, value1, value2 FROM async_test WHERE value0 = ?', { async => 1 });
$sth->execute(1);
is $sth->{'NUM_OF_FIELDS'}, undef;
is $sth->{'NUM_OF_PARAMS'}, 1;
is $sth->{'NAME'}, undef;
is $sth->{'NAME_lc'}, undef;
is $sth->{'NAME_uc'}, undef;
is $sth->{'NAME_hash'}, undef;
is $sth->{'NAME_lc_hash'}, undef;
is $sth->{'NAME_uc_hash'}, undef;
is $sth->{'TYPE'}, undef;
is $sth->{'PRECISION'}, undef;
is $sth->{'SCALE'}, undef;
is $sth->{'NULLABLE'}, undef;
is $sth->{'Database'}, $dbh;
is $sth->{'Statement'}, 'SELECT 1, value0, value1, value2 FROM async_test WHERE value0 = ?';
$sth->mysql_async_result;
is $sth->{'NUM_OF_FIELDS'}, 4;
is $sth->{'NUM_OF_PARAMS'}, 1;
cmp_bag $sth->{'NAME'}, [qw/1 value0 value1 value2/];
cmp_bag $sth->{'NAME_lc'}, [qw/1 value0 value1 value2/];
cmp_bag $sth->{'NAME_uc'}, [qw/1 VALUE0 VALUE1 VALUE2/];
cmp_bag [ keys %{$sth->{'NAME_hash'}} ], [qw/1 value0 value1 value2/];
cmp_bag [ keys %{$sth->{'NAME_lc_hash'}} ], [qw/1 value0 value1 value2/];
cmp_bag [ keys %{$sth->{'NAME_uc_hash'}} ], [qw/1 VALUE0 VALUE1 VALUE2/];
is ref($sth->{'TYPE'}), 'ARRAY';
is ref($sth->{'PRECISION'}), 'ARRAY';
is ref($sth->{'SCALE'}), 'ARRAY';
is ref($sth->{'NULLABLE'}), 'ARRAY';
is $sth->{'Database'}, $dbh;
is $sth->{'Statement'}, 'SELECT 1, value0, value1, value2 FROM async_test WHERE value0 = ?';
$sth->finish;

$sth->execute(1);
$row = $sth->fetch;
is_deeply $row, [1, 1, 2, 3];
$sth->finish;

$sth->execute(1);
$row = $sth->fetchrow_arrayref;
is_deeply $row, [1, 1, 2, 3];
$sth->finish;

$sth->execute(1);
my @row = $sth->fetchrow_array;
is_deeply \@row, [1, 1, 2, 3];
$sth->finish;

$sth->execute(1);
$row = $sth->fetchrow_hashref;
cmp_bag [ keys %$row ], [qw/1 value0 value1 value2/];
cmp_bag [ values %$row ], [1, 1, 2, 3];
$sth->finish;

undef $sth;
ok $dbh->disconnect;
