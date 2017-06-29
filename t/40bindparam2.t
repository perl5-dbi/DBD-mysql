use strict;
use warnings;

use Test::More;
use DBI;
use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1}) or ServerError();};

if ($@) {
    plan skip_all => "no database connection";
}
plan tests => 13;

ok $dbh->do('SET @@auto_increment_offset = 1');
ok $dbh->do('SET @@auto_increment_increment = 1');

my $create= <<EOT;
CREATE TEMPORARY TABLE dbd_mysql_t40bindparam2 (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    num INT(3))
EOT

ok $dbh->do($create), "create table dbd_mysql_t40bindparam2";

ok $dbh->do("INSERT INTO dbd_mysql_t40bindparam2 VALUES(NULL, 1)"), "insert into dbd_mysql_t40bindparam2 (null, 1)";

my $rows;
ok ($rows= $dbh->selectall_arrayref("SELECT * FROM dbd_mysql_t40bindparam2"));

is $rows->[0][1], 1, "\$rows->[0][1] == 1";

ok (my $sth = $dbh->prepare("UPDATE dbd_mysql_t40bindparam2 SET num = ? WHERE id = ?"));

ok ($sth->bind_param(2, 1, SQL_INTEGER()));

ok ($sth->execute());

ok ($sth->finish());

ok ($rows = $dbh->selectall_arrayref("SELECT * FROM dbd_mysql_t40bindparam2"));

ok !defined($rows->[0][1]);

ok ($dbh->disconnect());
