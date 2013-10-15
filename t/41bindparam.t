#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use Test::More;
use Carp qw(croak);
use vars qw($table $test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my ($dbh, $sth);
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all =>
        "ERROR: $DBI::errstr. Can't continue test";
}
plan tests => 11;

my ($rows, $errstr, $ret_ref);
ok $dbh->do("drop table if exists $table"), "drop table $table";

ok $dbh->do("create table $table (a int not null, primary key (a))"), "create table $table";

ok ($sth= $dbh->prepare("insert into $table values (?)"));

ok $sth->bind_param(1,10000,DBI::SQL_INTEGER), "bind param 10000 col1";

ok $sth->execute(), 'execute';

ok $sth->bind_param(1,10001,DBI::SQL_INTEGER), "bind param 10001 col1";

ok $sth->execute(), 'execute';

ok ($sth= $dbh->prepare("DROP TABLE $table"));

ok $sth->execute();

ok $sth->finish;

ok $dbh->disconnect;
