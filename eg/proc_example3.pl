#!/usr/bin/perl

use strict;
use DBI;
use Data::Dumper;

my $db='test';
my $host='localhost';
my $user='root';
my $password='';

my $dbh = DBI->connect("DBI:mysql:$db:$host",
		    "$user", "$password",
		  { PrintError => 0}) || die $DBI::errstr;

$dbh->do("drop procedure if exists testproc") or print $DBI::errstr;

$dbh->do("create procedure testproc() deterministic
    begin
    declare a,b,c,d,e,f int;
    set a=1;
    set b=2;
    set c=3;
    set d=4;
    set e=5;
    set f=6;
    select a, b, c, d;
    select d, c, b, a;
    select b, a, c, d;
    select c, b, d, a;
    select a, d;
    select f;
    select a, b, c, d, e, f; 
    end") or print $DBI::errstr;

my $sth= $dbh->prepare('call testproc()') || 
die $DBI::err.": ".$DBI::errstr;

$sth->execute || die DBI::err.": ".$DBI::errstr;
do {
  my $row= $sth->fetchrow_arrayref();
  print Dumper $row;
} while ($sth->more_results())


