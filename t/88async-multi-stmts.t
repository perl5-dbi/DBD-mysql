use strict;
use warnings;

use Test::More;
use DBI;
use DBI::Const::GetInfoType;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 0, PrintError => 0, AutoCommit => 0 });
plan tests => 8;

$dbh->do(<<SQL);
CREATE TEMPORARY TABLE async_test (
    value INTEGER
);
SQL

my $sth0 = $dbh->prepare('INSERT INTO async_test VALUES(0)', { async => 1 });
my $sth1 = $dbh->prepare('INSERT INTO async_test VALUES(1)', { async => 1 });

$sth0->execute;
ok !defined($sth1->mysql_async_ready);
ok $sth1->errstr;
ok !defined($sth1->mysql_async_result);
ok $sth1->errstr;

ok defined($sth0->mysql_async_ready);
ok !$sth1->errstr;
ok defined($sth0->mysql_async_result);
ok !$sth1->errstr;

undef $sth0;
undef $sth1;

$dbh->disconnect;
