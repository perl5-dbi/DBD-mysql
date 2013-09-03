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
plan tests => 25; 

ok $dbh->do("DROP TABLE IF EXISTS $table"), "drop table if exists $table";

my $create= <<"EOTABLE";
create table $table (
    id bigint unsigned not null default 0
    )
EOTABLE


ok $dbh->do($create), "creating table";

my $statement= "insert into $table (id) values (?)";

my $sth1;
ok $sth1= $dbh->prepare($statement);

my $rows;
ok $rows= $sth1->execute('9999999999999999');
cmp_ok $rows, '==',  1;

$statement= "update $table set id = ?";
my $sth2;
ok $sth2= $dbh->prepare($statement);

ok $rows= $sth2->execute('9999999999999998');
cmp_ok $rows, '==',  1;

$dbh->{mysql_bind_type_guessing}= 1;
ok $rows= $sth1->execute('9999999999999997');
cmp_ok $rows, '==',  1;

$statement= "update $table set id = ? where id = ?";

ok $sth2= $dbh->prepare($statement);
ok $rows= $sth2->execute('9999999999999996', '9999999999999997');

my $retref;
ok $retref= $dbh->selectall_arrayref("select * from $table");

cmp_ok $retref->[0][0], '==', 9999999999999998;
cmp_ok $retref->[1][0], '==', 9999999999999996;

# checking varchars/empty strings/misidentification:
$create= <<"EOTABLE";
create table $table (
    str varchar(80),
    num bigint
    )
EOTABLE
ok $dbh->do("DROP TABLE IF EXISTS $table"), "drop table if exists $table";
ok $dbh->do($create), "creating table w/ varchar";
my $sth3;
ok $sth3= $dbh->prepare("insert into $table (str, num) values (?, ?)");
ok $rows= $sth3->execute(52.3, 44);
ok $rows= $sth3->execute('', '     77');
ok $rows= $sth3->execute(undef, undef);

ok $sth3= $dbh->prepare("select * from $table limit ?");
ok $rows= $sth3->execute(1);
ok $rows= $sth3->execute('   1');
$sth3->finish();

ok $dbh->disconnect;
