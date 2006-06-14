#!/usr/bin/perl -w

use strict;
use DBI;

my $ssp = 1;
my $count = 0;

my $query = "SELECT 1 FROM DUAL";

my $dbh = DBI->connect (
    "dbi:mysql:database=test:host=localhost;mysql_emulated_prepare=0",
    "root", "",
    { RaiseError => 1, PrintError => 0 },
    );

my $s_q = $dbh->prepare($query);

while (1) {
  $s_q->execute();
  my @data = $s_q->fetchrow_array();
  $s_q->finish;

  $count++;

  print "ran $count queries\r";

  sleep(0.3);
}

