use strict;
use warnings;

use DBI;
use Test::More;
use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my ($dbh, $sth, $aref);
$dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });
plan tests => 30;

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t40numrows");

my $create= <<EOT;
CREATE TABLE dbd_mysql_t40numrows (
  id INT(4) NOT NULL DEFAULT 0,
  name varchar(64) NOT NULL DEFAULT ''
)
EOT

ok $dbh->do($create), "CREATE TABLE dbd_mysql_t40numrows";

ok $dbh->do("INSERT INTO dbd_mysql_t40numrows VALUES( 1, 'Alligator Descartes' )"), 'inserting first row';

ok ($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t40numrows WHERE id = 1"));

ok $sth->execute;

is $sth->rows, 1, '\$sth->rows should be 1';

ok ($aref= $sth->fetchall_arrayref);

is scalar @$aref, 1, 'Verified rows should be 1';

ok $sth->finish;

ok $dbh->do("INSERT INTO dbd_mysql_t40numrows VALUES( 2, 'Jochen Wiedmann' )"), 'inserting second row';

ok ($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t40numrows WHERE id >= 1"));

ok $sth->execute;

is $sth->rows, 2, '\$sth->rows should be 2';

ok ($aref= $sth->fetchall_arrayref);

is scalar @$aref, 2, 'Verified rows should be 2';

ok $sth->finish;

ok $dbh->do("INSERT INTO dbd_mysql_t40numrows VALUES(3, 'Tim Bunce')"), "inserting third row";

ok ($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t40numrows WHERE id >= 2"));

ok $sth->execute;

is $sth->rows, 2, 'rows should be 2';

ok ($aref= $sth->fetchall_arrayref);

is scalar @$aref, 2, 'Verified rows should be 2';

ok $sth->finish;

ok ($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t40numrows"));

ok $sth->execute;

is $sth->rows, 3, 'rows should be 3';

ok ($aref= $sth->fetchall_arrayref);

is scalar @$aref, 3, 'Verified rows should be 3';

ok $dbh->do("DROP TABLE dbd_mysql_t40numrows"), "drop table dbd_mysql_t40numrows";

ok $dbh->disconnect;
