use strict;
use warnings;

use DBI;
use Test::More;
use Carp qw(croak);
use Data::Dumper;
use vars qw($table $test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my ($dbh, $sth);
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all =>
        "ERROR: $DBI::errstr. Can't continue test";
}
plan tests => 10;

ok $dbh->do("DROP TABLE IF EXISTS $table"), "DROP TABLE IF EXISTS $table";

my $create= <<EOT;
CREATE TABLE $table (
  id INT(4),
  name VARCHAR(64)
  )
EOT
ok $dbh->do($create), "create table $create";

ok $dbh->do("INSERT INTO $table VALUES ( NULL, 'NULL-valued id' )"), "inserting nulls";

ok ($sth = $dbh->prepare("SELECT * FROM $table WHERE id IS NULL"));

do $sth->execute;

ok (my $aref = $sth->fetchrow_arrayref);

ok !defined($$aref[0]);

ok defined($$aref[1]);

ok $sth->finish;

ok $dbh->do("DROP TABLE $table");

ok $dbh->disconnect;
