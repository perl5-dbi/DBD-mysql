use strict;
use warnings;

use vars qw($table $test_dsn $test_user $test_password);
use Carp qw(croak);
use DBI;
use Test::More;
use lib 't', '.';
require 'lib.pl';

my ($row, $vers, $test_procs);

my $dbh;
eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1})};

if ($@) {
  plan skip_all => "ERROR: $DBI::errstr. Can't continue test";
}
plan tests => 12;

ok $dbh->do("DROP TABLE IF EXISTS $table");

my $create = <<EOT;
CREATE TABLE $table (
  id INT(4),
  name VARCHAR(32)
  )
EOT

ok $dbh->do($create),"create $table";

my $sth;
ok ($sth= $dbh->prepare("SHOW TABLES LIKE '$table'"));

ok $sth->execute();

ok ($row= $sth->fetchrow_arrayref);

cmp_ok $row->[0], 'eq', $table, "\$row->[0] eq $table";

ok $sth->finish;

ok $dbh->do("DROP TABLE $table"), "drop $table";

ok $dbh->do("CREATE TABLE $table (a int)"), "creating $table again with 1 col";

ok $dbh->do("ALTER TABLE $table ADD COLUMN b varchar(31)"), "alter $table ADD COLUMN";

ok $dbh->do("DROP TABLE $table"), "drop $table";

ok $dbh->disconnect;
