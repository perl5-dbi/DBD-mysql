# -*- cperl -*-

use strict;
use DBI ();

use vars qw($test_dsn $test_user $test_password $state);
require "t/lib.pl";

while (Testing()) {
  my ($dbh, $sth, $sth2);
  my $max_id;
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

  #
  # Insert another row
  #
  Test($state or ($sth = $dbh->prepare($q)));
  Test($state or $sth->execute());
  Test($state or ($sth2= $dbh->prepare("SELECT max(id) FROM $table"))); 
  Test($state or $sth2->execute());
  Test($state or ($max_id= $sth2->fetch()));
  # IMPORTANT: this will fail if you are using replication with
  # an offset and auto_increment_increment, where your 
  # auto_increment values are stepped (ex: 1, 11, 21, ...)
  Test($state or $sth->{'mysql_insertid'} == $max_id->[0]);
  Test($state or $dbh->{'mysql_insertid'} == $max_id->[0]);
  Test($state or $sth->finish());
  Test($state or $sth2->finish());

  #
  # Drop the table
  Test($state or $dbh->do("DROP TABLE $table"));

  #
  # Close the database connection
  Test($state or ($dbh->disconnect() or 1));
}
