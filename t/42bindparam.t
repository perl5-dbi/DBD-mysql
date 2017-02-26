use strict;
use warnings;

use vars qw($test_dsn $test_user $test_password $mdriver);
use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

plan tests => 12;

ok $dbh->do("drop table if exists dbd_mysql_t42bindparams");

my $create= <<EOT;
create table dbd_mysql_t42bindparams (
    a int not null,
    b double,
    primary key (a))
EOT

ok $dbh->do($create);

ok (my $sth= $dbh->prepare("insert into dbd_mysql_t42bindparams values (?, ?)"));

ok $sth->bind_param(1,"10000 ",DBI::SQL_INTEGER);

ok $sth->bind_param(2,"1.22 ",DBI::SQL_DOUBLE);

ok $sth->execute();

ok $sth->bind_param(1,10001,DBI::SQL_INTEGER);

ok $sth->bind_param(2,.3333333,DBI::SQL_DOUBLE);

ok $sth->execute();

ok $dbh->do("DROP TABLE dbd_mysql_t42bindparams");

ok $sth->finish;

ok $dbh->disconnect;
