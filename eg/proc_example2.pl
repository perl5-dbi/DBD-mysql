#!/usr/bin/perl

use DBI;

$DATABASE='test';
$HOST='localhost';
$PORT=3306; $USER='root';
$PASSWORD='';

#DBI->trace(3);
$dbh = DBI->connect("DBI:mysql:$DATABASE:$HOST:$PORT",
		    "$USER", "$PASSWORD",
		  { PrintError => 0}) || die $DBI::errstr;

# DROP TABLE IF EXISTS 
$dbh->do("DROP TABLE IF EXISTS users") or print $DBI::errstr;
# CREATE TABLE
$dbh->do("CREATE TABLE users (id INT, name VARCHAR(32))") or print $DBI::errstr;

my $sth= $dbh->prepare("INSERT INTO users VALUES (?, ?)");

for $i(1 .. 20) {
  my @chars = grep !/[0O1Iil]/, 0..9, 'A'..'Z', 'a'..'z';
  my $random_chars = join '', map { $chars[rand @chars] } 0 .. 31;

  my $rows = $sth->execute($i, $random_chars);
}

$dbh->do("DROP PROCEDURE IF EXISTS users_proc") or print $DBI::errstr;

$dbh->do("CREATE PROCEDURE users_proc() DETERMINISTIC 
BEGIN 
  SELECT id, name FROM users;
END") or print $DBI::errstr;

$sth = $dbh->prepare('call users_proc()') || 
 die $DBI::err.": ".$DBI::errstr;

 $sth->execute || die DBI::err.": ".$DBI::errstr; $rowset=0;
 do {
   print "\nRowset ".++$i."\n---------------------------------------\n\n";
   foreach $colno (0..$sth->{NUM_OF_FIELDS}) {
     print $sth->{NAME}->[$colno]."\t";
   }
   print "\n";
   while (@row=$sth->fetchrow_array())  {
     foreach $field (0..$#row) {
       print $row[$field]."\t";
     }
     print "\n";
   }
 } until (!$sth->more_results)


