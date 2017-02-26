use strict;
use warnings;

use DBI;
use Test::More;
use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

eval {
   $dbh->{PrintError} = 0;
   $dbh->do("SET NAMES 'utf8mb4'");
   $dbh->{PrintError} = 1;
   1;
} or do {
   $dbh->disconnect();
   plan skip_all => "no support for utf8mb4";
};

ok $dbh->do("CREATE TEMPORARY TABLE dbd_mysql_t55utf8mb4 (id SERIAL, val TEXT CHARACTER SET utf8mb4)");

my $sth = $dbh->prepare("INSERT INTO dbd_mysql_t55utf8mb4(val) VALUES('😈')");
$sth->execute();

my $query = "SELECT val, HEX(val) FROM dbd_mysql_t55utf8mb4 LIMIT 1";
$sth = $dbh->prepare($query) or die "$DBI::errstr";
ok $sth->execute;

ok(my $ref = $sth->fetchrow_arrayref, 'fetch row');
ok($sth->finish, 'close sth');
cmp_ok $ref->[0], 'eq', "😈";
cmp_ok $ref->[1], 'eq', "F09F9888";
ok(!utf8::is_utf8($ref->[0]), "utf8 flag is not set without mysql_enable_utf8mb4");

use utf8;
$dbh->{mysql_enable_utf8mb4} = 1;
$sth = $dbh->prepare($query) or die "$DBI::errstr";
ok $sth->execute;
ok($ref = $sth->fetchrow_arrayref, 'fetch row with mysql_enable_utf8mb4');
ok($sth->finish, 'close sth');
cmp_ok $ref->[0], 'eq', "😈", 'test U+1F608 with mysql_enable_utf8mb4 and utf8 pragma';
cmp_ok $ref->[1], 'eq', "F09F9888";
$dbh->{mysql_enable_utf8mb4} = 0;
no utf8;

$dbh->disconnect();
done_testing;
