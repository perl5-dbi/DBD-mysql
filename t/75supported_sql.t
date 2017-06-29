use strict;
use warnings;

use vars qw($test_dsn $test_user $test_password);
use DBI;
use Test::More;
use lib 't', '.';
require 'lib.pl';

my ($row, $vers, $test_procs);

my $dbh;
eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1})};

if ($@) {
  plan skip_all => "no database connection";
}
plan tests => 12;

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t75supported");

my $create = <<EOT;
CREATE TABLE dbd_mysql_t75supported (
  id INT(4),
  name VARCHAR(32)
  )
EOT

ok $dbh->do($create),"create dbd_mysql_t75supported";

my $sth;
ok ($sth= $dbh->prepare("SHOW TABLES LIKE 'dbd_mysql_t75supported'"));

ok $sth->execute();

ok ($row= $sth->fetchrow_arrayref);

cmp_ok $row->[0], 'eq', 'dbd_mysql_t75supported', "\$row->[0] eq dbd_mysql_t75supported";

ok $sth->finish;

ok $dbh->do("DROP TABLE dbd_mysql_t75supported"), "drop dbd_mysql_t75supported";

ok $dbh->do("CREATE TABLE dbd_mysql_t75supported (a int)"), "creating dbd_mysql_t75supported again with 1 col";

ok $dbh->do("ALTER TABLE dbd_mysql_t75supported ADD COLUMN b varchar(31)"), "alter dbd_mysql_t75supported ADD COLUMN";

ok $dbh->do("DROP TABLE dbd_mysql_t75supported"), "drop dbd_mysql_t75supported";

ok $dbh->disconnect;
