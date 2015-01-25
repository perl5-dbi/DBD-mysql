use strict;
use warnings;

use Test::More;
use DBI;
use DBI::Const::GetInfoType;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });};
if ($@) {
    plan skip_all => "no database connection";
}

if($dbh->get_info($GetInfoType{'SQL_ASYNC_MODE'})) {
    plan skip_all => "Async support was built into this version of DBD::mysql";
}
plan tests => 14;

is $dbh->get_info($GetInfoType{'SQL_MAX_ASYNC_CONCURRENT_STATEMENTS'}), 0;

ok !$dbh->do('SELECT 1', { async => 1 });
ok $dbh->errstr;

ok !$dbh->prepare('SELECT 1', { async => 1 });
ok $dbh->errstr;

ok !$dbh->mysql_async_result;
ok $dbh->errstr;

ok !$dbh->mysql_async_ready;
ok $dbh->errstr;

my $sth = $dbh->prepare('SELECT 1');
ok $sth;

ok !$sth->mysql_async_result;
ok $dbh->errstr;

ok !$sth->mysql_async_ready;
ok $dbh->errstr;

$dbh->disconnect;
