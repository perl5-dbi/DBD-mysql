#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use Data::Dumper;

my $db='test';
my $host='localhost';
my $user='root';
my $password='';
my $i= 0;

my $dbh = DBI->connect("DBI:mysql:$db:$host",
		    "$user", "$password",
		  { PrintError => 0}) || die $DBI::errstr;

# DROP TABLE IF EXISTS 
$dbh->do("DROP TABLE IF EXISTS users") or print $DBI::errstr;
# CREATE TABLE
$dbh->do("CREATE TABLE users (id INT, name VARCHAR(32))") or print $DBI::errstr;

my $sth= $dbh->prepare("INSERT INTO users VALUES (?, ?)");

for $i(1 .. 20)
{
  my @chars = grep !/[0O1Iil]/, 0..9, 'A'..'Z', 'a'..'z';
  my $random_chars = join '', map { $chars[rand @chars] } 0 .. 31;

  my $rows = $sth->execute($i, $random_chars);
}

$dbh->do("DROP PROCEDURE IF EXISTS users_proc") or print $DBI::errstr;

$dbh->do("CREATE PROCEDURE users_proc() DETERMINISTIC 
BEGIN 
  SELECT id, name FROM users;
  SELECT name, id FROM users;
END") or print $DBI::errstr;

$sth = $dbh->prepare('call users_proc()') || 
 die $DBI::err.": ".$DBI::errstr;

 $sth->execute || die DBI::err.": ".$DBI::errstr;
 do {
  print "\nResult set ".++$i."\n---------------------------------------\n\n";
  for my $colno (0..$sth->{NUM_OF_FIELDS}-1) {
    print $sth->{NAME}->[$colno]."\t";
  }
  print "\n";
  while (my $rowref=$sth->fetchrow_arrayref())  {
    for my $field (0..$#$rowref) {
      print $rowref->[$field]."\t";
    }
    print "\n";
  }
 } while ($sth->more_results())


