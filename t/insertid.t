# -*- cperl -*-

use strict;
use DBI ();

use vars qw($test_dsn $test_user $test_password $state);
require "t/lib.pl";

while (Testing()) {
  my $dbh;
  #
  # Connect to the database
  Test($state or
       ($dbh = DBI->connect($test_dsn, $test_user, $test_password,
			    {RaiseError => 1})));

  #
  # Find a possible new table name
  #
  my $table = "";
  Test($state or
       ($table = FindNewTable($dbh)));

  #
  # Create a new table
  #
  my $q = <<"QUERY";
CREATE TABLE $table (id INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL,
                     name VARCHAR(64))
QUERY
  Test($state or $dbh->do($q));

  #
  # Insert a row
  #
  $q = "INSERT INTO $table (name) VALUES (?)";
  Test($state or $dbh->do($q, undef, "Jochen"));

  #
  # Verify $dbh->insertid
  Test($state or ($dbh->{'mysql_insertid'} eq "1"));
  Test($state or $dbh->last_insert_id(undef,undef,undef,undef,undef) eq 1);

  #
  # Insert another row
  #
  my $sth;
  Test($state or ($sth = $dbh->prepare($q)));
  Test($state or $sth->execute("Jochen"));
  Test($state or $sth->{'mysql_insertid'} eq 2);
  Test($state or $dbh->{'mysql_insertid'} eq 2);

  Test($state or $dbh->last_insert_id(undef,undef,undef,undef,undef) eq 2);
  Test($state or $sth->finish());

  #
  # Drop the table
  Test($state or $dbh->do("DROP TABLE $table"));

  #
  # Close the database connection
  Test($state or ($dbh->disconnect() or 1));
}
