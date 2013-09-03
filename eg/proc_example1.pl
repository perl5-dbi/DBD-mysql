#!/usr/bin/perl

use strict;
use DBI;

my $db='test';
my $host='localhost';
my $user='root';
my $password='';
my $i= 0;

my $dbh = DBI->connect("DBI:mysql:$db:$host",
    "$user", "$password",
    { PrintError => 0}) || die $DBI::errstr;

$dbh->do("drop procedure if exists testproc") or print $DBI::errstr;

$dbh->do("create procedure testproc() deterministic
    begin
    declare a,b,c,d int;
    set a=1;
    set b=2;
    set c=3;
    set d=4;
    select a, b, c, d;
    select d, c, b, a;
    select b, a, c, d;
    select c, b, d, a;
    end") or print $DBI::errstr;

my $sth= $dbh->prepare('call testproc()') || 
die $DBI::err.": ".$DBI::errstr;

$sth->execute || die DBI::err.": ".$DBI::errstr;
do {
  print "\nResult set ".++$i."\n---------------------------------------\n\n";
  for my $colno (0..$sth->{NUM_OF_FIELDS}) {
    print $sth->{NAME}->[$colno]."\t";
  }
  print "\n";
  while (my @row= $sth->fetchrow_array())  {
    for my $field (0..$#row) {
      print $row[$field]."\t";
    }
    print "\n";
  }
} until (!$sth->more_results)


