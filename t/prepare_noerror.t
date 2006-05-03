# -*- cperl -*-
# Test problem in 3.0002_4 and 3.0005 where if a statement is prepared
# and multiple executes are performed, if any execute fails all subsequent
# executes report an error but may have worked.

use strict;
use DBI ();

use vars qw($test_dsn $test_user $test_password $state);
require "t/lib.pl";

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
  #print STDERR $DBI::errstr;
  #
  # Close the database connection
  Test($state or ($dbh->disconnect() or 1));
}

