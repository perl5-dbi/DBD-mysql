#!/usr/bin/perl

use DBI;

$DATABASE='test';
$HOST='localhost';
$PORT=3306; $USER='root';
$PASSWORD='';

$db_my = DBI->connect("DBI:mysql:$DATABASE:$HOST:$PORT",
		    "$USER", "$PASSWORD",
		  { PrintError => 0}) || die $DBI::errstr;

 $db_my->do("drop procedure if exists testproc") or print $DBI::errstr;

 $db_my->do("create procedure testproc() deterministic
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

 $sth=$db_my->prepare('call testproc()') || 
 die $DBI::err.": ".$DBI::errstr;

 $sth->execute || die DBI::err.": ".$DBI::errstr; $rowset=0;
 do {
   print "\nRowset ".++$i."\n---------------------------------------\n\n";
   foreach $colno (0..$sth->{NUM_OF_FIELDS}) {
     print $sth->{NAME}->[$colno]."\t";
   }
   print "\n";
   while (@row= $sth->fetchrow_array())  {
     foreach $field (0..$#row) {
       print $row[$field]."\t";
     }
     print "\n";
   }
 } until (!$sth->more_results)


