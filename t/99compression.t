use strict;
use warnings;

use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password);
my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};

if ($@) {
  diag $@;
  plan skip_all => "no database connection";
}

if ($dbh->{mysql_serverversion} < 80000) {
  diag $dbh->{mysql_serverversion};
  plan skip_all => "test requires 8.x or newer";
}

if ($dbh->{'mysql_serverinfo'} =~ 'MariaDB') {
  plan skip_all => "No zstd or Compression_algorithm on MariaDB";
}

foreach my $compression ( "zlib", "zstd", "0", "1" ) {
  my ($dbh, $sth, $row);
  
  eval {$dbh = DBI->connect($test_dsn . ";mysql_compression=$compression", $test_user, $test_password,
      { RaiseError => 1, AutoCommit => 1});};
  
  ok ($sth= $dbh->prepare("SHOW SESSION STATUS LIKE 'Compression_algorithm'"));
  
  ok $sth->execute();
  
  ok ($row= $sth->fetchrow_arrayref);

  my $exp = $compression;
  if ($exp eq "1") { $exp = "zlib" };
  if ($exp eq "0") { $exp = "" };
  cmp_ok $row->[1], 'eq', $exp, "\$row->[1] eq $exp";
  
  ok $sth->finish;
}

plan tests => 4*5;
