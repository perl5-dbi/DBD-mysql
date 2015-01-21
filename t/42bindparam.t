use strict;
use warnings;

use vars qw($table $test_dsn $test_user $test_password $mdriver);
use Test::More;
use DBI;
use Carp qw(croak);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all => "ERROR: $@. Can't continue test";
}
plan tests => 12;

ok $dbh->do("drop table if exists $table");

my $create= <<EOT;
create table $table (
    a int not null,
    b double,
    primary key (a))
EOT

ok $dbh->do($create);

ok (my $sth= $dbh->prepare("insert into $table values (?, ?)"));

ok $sth->bind_param(1,"10000 ",DBI::SQL_INTEGER);

ok $sth->bind_param(2,"1.22 ",DBI::SQL_DOUBLE);

ok $sth->execute();

ok $sth->bind_param(1,10001,DBI::SQL_INTEGER);

ok $sth->bind_param(2,.3333333,DBI::SQL_DOUBLE);

ok $sth->execute();

ok $dbh->do("DROP TABLE $table");

ok $sth->finish;

ok $dbh->disconnect;
