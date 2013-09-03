#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use Data::Dumper;

my $create= <<'EOTABLE';
create table bigt1 (
    id bigint unsigned not null default 0
    )
EOTABLE

#my $dbh= DBI->connect('DBI:mysql:test', 'root', 'root', { mysql_bind_type_guessing => 2}) 
#    or die "unable to connect $DBI::errstr";
my $dbh= DBI->connect('DBI:mysql:test', 'root', 'root') 
    or die "unable to connect $DBI::errstr";

$dbh->{mysql_bind_type_guessing}= 1;

$dbh->do('drop table if exists bigt1');
$dbh->do($create);

my $statement= 'insert into bigt1 (id) values (?)';

my $sth= $dbh->prepare($statement);

my $rows= $sth->execute('9999999999999999');
print "rows $rows\n";

$statement= 'update bigt1 set id = ?';
$sth= $dbh->prepare($statement);
$rows= $sth->execute('9999999999999998');
print "rows $rows\n";

my $retref= $dbh->selectall_arrayref('select * from bigt1');
print Dumper $retref;
