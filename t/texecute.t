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
       ($dbh = DBI->connect($test_dsn, $test_user, $test_password,
                           {RaiseError => 0})));

  # Find a possible new table name
  #
  my $table = 't1';
  # Drop the table
  Test($state or $dbh->do("DROP TABLE IF EXISTS $table"));
  #
  # Create a new table
  #
  my $q = <<"QUERY";
CREATE TABLE $table (id INTEGER PRIMARY KEY NOT NULL,
                     name VARCHAR(64))
QUERY
  Test($state or $dbh->do($q));

  #
  # Insert a row
  #
  $q = "INSERT INTO $table (id, name) VALUES (?,?)";
  Test($state or ($sth = $dbh->prepare($q)));
  Test($state or $sth->execute(1, "Jocken"));
  $sth->{PrintError} = 0;
  Test($state or !($sth->execute(1, "Jochen")));
  $sth->{PrintError} = 1;
  Test($state or $sth->execute(2, "Jochen"));

  #
  # Drop the table
  Test($state or $dbh->do("DROP TABLE $table"));

  #
  # Close the database connection
  Test($state or ($dbh->disconnect() or 1));
}

