use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
require "t/lib.pl";

my $dbh = eval { DBI->connect($test_dsn, $test_user, $test_password, { PrintError => 1, RaiseError => 1, AutoCommit => 0, mysql_server_prepare => 1, mysql_server_prepare_disable_fallback => 1 }) };
plan skip_all => "no database connection" if $@ or not $dbh;

plan tests => 17;

ok $dbh->do("CREATE TEMPORARY TABLE t (i INTEGER NOT NULL, n TEXT)");

ok my $sth = $dbh->prepare("SELECT * FROM t WHERE i=? AND n=?");

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

ok $dbh->do("SELECT 1 FROM t WHERE i = ?" . (" OR i = ?" x 10000), {}, (1) x (10001));

ok $dbh->disconnect();
