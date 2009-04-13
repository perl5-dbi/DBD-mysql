#!/usr/bin/perl

use GD;
use DBI; # Load Database Interface Module
use Data::Dumper;

# Connect to database
my $dbh = DBI->connect
('DBI:mysql:database=test;host=localhost:mysql_server_prepare=1',
 'myUser', 'myPassword', {RaiseError => 1})
or die "$0: Can not connect to database: " . $DBI::errstr;

# create a new image
$im = new GD::Image(6490,4000);

# allocate color black
$black = $im->colorAllocate(0,0,0);

# The maximum id value in table is 25958999
my $sth = $dbh->prepare("SELECT id FROM myTable WHERE id=?");

my $id = 1;
foreach $x (0..6489) {
  print "x=$x/6490 id=$id\n";
  foreach $y (0..3999){
    $sth->execute($id);
    if ($sth->fetchrow_array) {
      $im->setPixel($x,$y,$black);
    }
    $id = $id + 1;
  }
  open(OUT,">/tmp/id.png") or die "can not write output file";
  binmode OUT;
  print OUT $im->png;
  close(OUT);
}

