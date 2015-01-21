use strict;
use warnings;

use vars qw($table $test_dsn $test_user $test_password);
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
plan tests => 19;

ok $dbh->do("drop table if exists $table");

my $create= <<EOT;
create table $table (
    a int,
    primary key (a)
)
EOT

ok $dbh->do($create);

my $sth;
eval {$sth= $dbh->prepare("insert into $table values (?)")};

ok ! $@, "prepare: $@";

ok $sth->bind_param(1,10000,DBI::SQL_INTEGER);

ok $sth->execute();

ok $sth->bind_param(1,10001,DBI::SQL_INTEGER);

ok $sth->execute();

ok $dbh->do("DROP TABLE $table");

ok $dbh->do("create table $table (a int, b double, primary key (a))");

eval { $sth= $dbh->prepare("insert into $table values (?, ?)")};

ok ! $@, "prepare: $@";

ok $sth->bind_param(1,"10000 ",DBI::SQL_INTEGER);

ok $sth->bind_param(2,"1.22 ",DBI::SQL_DOUBLE);

ok $sth->execute();

ok $sth->bind_param(1,10001,DBI::SQL_INTEGER);

ok $sth->bind_param(2,.3333333,DBI::SQL_DOUBLE);

ok $sth->execute();

ok $sth->finish;

ok $dbh->do("DROP TABLE $table");

ok $dbh->disconnect;
