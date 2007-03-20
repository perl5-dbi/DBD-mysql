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
print "Driver is $mdriver\n";

my @dsn;
my $dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

ok(defined $dbh, "Connected to database");

ok((@dsn = DBI->data_sources($mdriver, {user=> $test_user, password=> $test_password})));

$dbh->disconnect();

#   Try different DSN's
my (@dsnList);
if (($mdriver eq 'mysql' or $mdriver eq 'mysqlEmb')
  and  $test_dsn eq "DBI:$mdriver:test")
{
	@dsnList = ("DBI:$mdriver:test:localhost",
		    "DBI:$mdriver:test;localhost",
		    "DBI:$mdriver:database=test;host=localhost");
}

for my $dsn (@dsnList)
{
  ok(($dbh = DBI->connect($dsn, $test_user, $test_password)));
	ok(dbh->disconnect());
}
