use strict;
use warnings;

use Test::More;
use DBI;
use vars qw($table $test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1}) or ServerError();};

if ($@) {
    plan skip_all => "ERROR: $DBI::errstr. Can't continue test";
}
plan tests => 13;

ok $dbh->do('SET @@auto_increment_offset = 1');
ok $dbh->do('SET @@auto_increment_increment = 1');

my $create= <<EOT;
CREATE TEMPORARY TABLE $table (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    num INT(3))
EOT

ok $dbh->do($create), "create table $table";

ok $dbh->do("INSERT INTO $table VALUES(NULL, 1)"), "insert into $table (null, 1)";

my $rows;
ok ($rows= $dbh->selectall_arrayref("SELECT * FROM $table"));

is $rows->[0][1], 1, "\$rows->[0][1] == 1";

ok (my $sth = $dbh->prepare("UPDATE $table SET num = ? WHERE id = ?"));

ok ($sth->bind_param(2, 1, SQL_INTEGER()));

ok ($sth->execute());

ok ($sth->finish());

ok ($rows = $dbh->selectall_arrayref("SELECT * FROM $table"));

ok !defined($rows->[0][1]);

ok ($dbh->disconnect());
