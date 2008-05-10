# -*- cperl -*-
# Test problem in 3.0002_4 and 3.0005 where if a statement is prepared
# and multiple executes are performed, if any execute fails all subsequent
# executes report an error but may have worked.

use strict;
use DBI ();
use Test::More;

use vars qw($table $test_dsn $test_user $test_password);
use lib '.','t';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all => "ERROR: $@. Can't continue test";
}
plan tests => 7; 

ok $dbh->do("DROP TABLE IF EXISTS $table");

my $create = <<EOT;
CREATE TABLE $table (id INTEGER,
                     name VARCHAR(64))
EOT

ok $dbh->do($create), "create $table";

my $query = "INSERT INTO $table (id, name) VALUES (?,?)";

my $sth = $dbh->prepare($query) or die "$DBI::errstr";

ok $sth->execute(1, 'two');

ok $sth->{ParamValues};

ok $dbh->do("DROP TABLE $table");

ok $sth->finish;

ok $dbh->disconnect();
