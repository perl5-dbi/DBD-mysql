use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
require "t/lib.pl";

my $dbh = eval { DBI->connect($test_dsn, $test_user, $test_password, { PrintError => 1, RaiseError => 1, AutoCommit => 0, mysql_server_prepare => 1, mysql_server_prepare_disable_fallback => 1 }) };
plan skip_all => "no database connection" if $@ or not $dbh;

plan tests => 40;

my $sth;

ok $dbh->do("DROP TABLE IF EXISTS t");

ok $dbh->do("CREATE TABLE t (i INTEGER NOT NULL, n LONGBLOB)");

ok $sth = $dbh->prepare("INSERT INTO t(i, n) VALUES(?, ?)");
ok $sth->execute(1, "x" x 10);
ok $sth->execute(2, "x" x 100);
ok $sth->execute(3, "x" x 1000);
ok $sth->execute(4, "x" x 10000);
ok $sth->execute(5, "x" x 100000);
ok $sth->execute(6, "x" x 1000000);
ok $sth->finish();

ok $sth = $dbh->prepare("SELECT * FROM t WHERE i=? AND n=?");

ok $sth->bind_param(2, "x" x 1000000);
ok $sth->bind_param(1, "abcx", 12);
ok $sth->execute();

ok $sth->bind_param(2, "a" x 1000000);
ok $sth->bind_param(1, 1, 3);
ok $sth->execute();

ok $sth->finish();

ok $sth = $dbh->prepare("SELECT * FROM t WHERE i=? AND n=?");
ok $sth->execute();
ok $sth->finish();

ok $sth = $dbh->prepare("SELECT 1 FROM t WHERE i = ?" . (" OR i = ?" x 10000));
ok $sth->execute((1) x (10001));
ok $sth->finish();

my $test;
ok $sth = $dbh->prepare("SELECT i,n FROM t WHERE i = ?");

ok $sth->execute(1);
ok $sth->fetchrow_arrayref();

ok $sth->execute(2);
$test = map { $_ } 'a';
ok $sth->fetchrow_arrayref();

ok $sth->execute(3);
$test = map { $_ } 'b' x 10000000; # try to reuse released memory
ok $sth->fetchrow_arrayref();

ok $sth->execute(4);
$test = map { $_ } 'cd' x 10000000; # try to reuse of released memory
ok $sth->fetchrow_arrayref();

ok $sth->execute(5);
$test = map { $_ } 'efg' x 10000000; # try to reuse of released memory
ok $sth->fetchrow_arrayref();

ok $sth->execute(6);
$test = map { $_ } 'hijk' x 10000000; # try to reuse of released memory
ok $sth->fetchrow_arrayref();

ok $sth->finish();

ok $dbh->do("SELECT 1 FROM t WHERE i = ?" . (" OR i = ?" x 10000), {}, (1) x (10001));

ok $dbh->disconnect();
