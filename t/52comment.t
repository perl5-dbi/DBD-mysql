#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use DBI::Const::GetInfoType;
use Test::More;
use lib 't', '.';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password $table);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all => 
        "ERROR: $DBI::errstr. Can't continue test";
}
plan tests => 9; 

ok $dbh->do("DROP TABLE IF EXISTS $table"), "drop table if exists $table";

my $create= <<"EOTABLE";
create table $table (
    id bigint unsigned not null default 0
    )
EOTABLE


ok $dbh->do($create), "creating table";

my $statement= "insert into $table (id) values (?)";

my $sth;
ok $sth= $dbh->prepare($statement);

my $rows;
ok $rows= $sth->execute('1');
cmp_ok $rows, '==',  1;
$sth->finish();

$statement= <<EOSTMT;
SELECT id 
FROM $table
-- it's a bug?
WHERE id = ?
EOSTMT

my $retrow= $dbh->selectrow_arrayref($statement, {}, 1);
cmp_ok $retrow->[0], '==', 1;

$statement= "SELECT id FROM $table /* it's a bug? */ WHERE id = ?";

$retrow= $dbh->selectrow_arrayref($statement, {}, 1);
cmp_ok $retrow->[0], '==', 1;

ok $dbh->do("DROP TABLE IF EXISTS $table"), "drop table if exists $table";

ok $dbh->disconnect;
