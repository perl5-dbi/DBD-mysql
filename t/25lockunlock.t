use strict;
use warnings;

use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password);

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

plan tests => 13;

my $create= <<EOT;
CREATE TABLE dbd_mysql_t25lockunlock (
    id int(4) NOT NULL default 0,
    name varchar(64) NOT NULL default ''
    )
EOT

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t25lockunlock"), "drop table if exists dbd_mysql_t25lockunlock";

ok $dbh->do($create), "create table dbd_mysql_t25lockunlock";

ok $dbh->do("LOCK TABLES dbd_mysql_t25lockunlock WRITE"), "lock table dbd_mysql_t25lockunlock";

ok $dbh->do("INSERT INTO dbd_mysql_t25lockunlock VALUES(1, 'Alligator Descartes')"), "Insert ";

ok $dbh->do("DELETE FROM dbd_mysql_t25lockunlock WHERE id = 1"), "Delete";

my $sth;
eval {$sth= $dbh->prepare("SELECT * FROM dbd_mysql_t25lockunlock WHERE id = 1")};

ok !$@, "Prepare of select";

ok defined($sth), "Prepare of select";

ok  $sth->execute , "Execute";

my ($row, $errstr);
$errstr= '';
$row = $sth->fetchrow_arrayref;
$errstr= $sth->errstr;
ok !defined($row), "Fetch should have failed";
ok !defined($errstr), "Fetch should have failed";

ok $dbh->do("UNLOCK TABLES"), "Unlock tables";

ok $dbh->do("DROP TABLE dbd_mysql_t25lockunlock"), "Drop table dbd_mysql_t25lockunlock";
ok $dbh->disconnect, "Disconnecting";
