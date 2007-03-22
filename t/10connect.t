#!perl -w
# vim: ft=perl

use Test::More tests => 2;
use DBI;
use DBI::Const::GetInfoType;
use strict;
use vars qw($mdriver);
$|= 1;

our ($test_dsn, $test_user, $test_password);
$mdriver = "";
for my $file ("lib.pl", "t/lib.pl", "DBD-mysql/t/lib.pl") {
  do $file; if ($@)
  {
    print STDERR "Error while executing lib.pl: $@\n";
    exit 10;
  }
  if ($mdriver ne '')
  {
    last;
  }
}
my @dsn;
my $dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

ok(defined $dbh, "Connected to database");

ok($dbh->disconnect());
