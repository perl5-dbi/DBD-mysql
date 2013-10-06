#/usr/bin/perl

# Test special characters inside comments
# http://bugs.debian.org/311040
# http://bugs.mysql.com/27625

use strict;
use warnings;

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

  my $q;

  #
  # Placeholder inside a comment
  #
  $q = " -- Does the question mark at the end confuse DBI::MySQL?\nselect ?";

  Test($state or ($sth = $dbh->prepare($q)));
  Test($state or ($sth->execute(42)));
  Test($state or ($sth->{ParamValues}));
  Test($state or ($sth->finish));

  #
  # Quote inside a comment
  #
  $q = " -- 'Tis the quote that confuses DBI::MySQL\nSELECT ?";

  Test($state or ($sth = $dbh->prepare($q)));
  Test($state or ($sth->execute(42)));
  Test($state or ($sth->{ParamValues}));
  Test($state or ($sth->finish));

  #
  # Close the database connection
  Test($state or ($dbh->disconnect() or 1));
}

