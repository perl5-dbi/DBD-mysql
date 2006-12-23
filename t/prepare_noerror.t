# -*- cperl -*-
# Test problem in 3.0002_4 and 3.0005 where if a statement is prepared
# and multiple executes are performed, if any execute fails all subsequent
# executes report an error but may have worked.

use strict;
use DBI ();
use Data::Dumper;

use vars qw($test_dsn $test_user $test_password $state);
my ($mdriver,$file) = ('','');
foreach $file ("lib.pl", "t/lib.pl")
{
  do $file; if ($@) { print STDERR "Error while executing lib.pl: $@\n";
    exit 10;
  }
  if ($mdriver ne '') {
    last;
  }
}

my $tmp_dbh= DBI->connect("$test_dsn",
  $test_user, $test_password, {RaiseError => 0});

my $tmp_sth= $tmp_dbh->prepare("select version()"); 

$tmp_sth->execute();

my $tmp_ref= $tmp_sth->fetchall_arrayref();

my $tmp_version= $tmp_ref->[0][0];

$tmp_version =~ /^(\d)\.(\d)/;
#print "version $tmp_version version # $1 dot $2\n";

$tmp_sth->finish();
$tmp_dbh->disconnect();

if ($1 < 5 && $2 < 1)
{
  print "1..0 # Skip test - will only run with MySQL 4.1 and above.\n";
  exit(0);
}
if ($test_dsn =~ /emulated/)
{
  print "1..0 # Skip test - will only run in server-side prepare mode.\n";
  exit(0);
}


while (Testing()) {
  my ($dbh, $sth);
  #
  # Connect to the database
  Test($state or
       ($dbh = DBI->connect("$test_dsn;mysql_server_prepare=1", $test_user, $test_password,
                           {RaiseError => 0})));

  #
  # execute invalid SQL to make sure we get an error
  #
  my $q = "select select select";	# invalid SQL
  $dbh->{PrintError} = 0;
  $dbh->{PrintWarn} = 0;
  eval {$sth = $dbh->prepare($q);};
  $dbh->{PrintError} = 1;
  $dbh->{PrintWarn} = 1;
  Test($state or (defined($DBI::errstr) && ($DBI::errstr ne "")));
  print "errstr $DBI::errstr\n" if $DBI::errstr;
  #
  # Close the database connection
  Test($state or ($dbh->disconnect() or 1));
}

