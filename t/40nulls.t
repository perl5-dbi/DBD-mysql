use strict;
use warnings;

use DBI;
use Test::More;
use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my ($dbh, $sth);
$dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });
plan tests => 10;

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t40nulls"), "DROP TABLE IF EXISTS dbd_mysql_t40nulls";

my $create= <<EOT;
CREATE TABLE dbd_mysql_t40nulls (
  id INT(4),
  name VARCHAR(64)
  )
EOT
ok $dbh->do($create), "create table $create";

ok $dbh->do("INSERT INTO dbd_mysql_t40nulls VALUES ( NULL, 'NULL-valued id' )"), "inserting nulls";

ok ($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t40nulls WHERE id IS NULL"));

do $sth->execute;

ok (my $aref = $sth->fetchrow_arrayref);

ok !defined($$aref[0]);

ok defined($$aref[1]);

ok $sth->finish;

ok $dbh->do("DROP TABLE dbd_mysql_t40nulls");

ok $dbh->disconnect;
