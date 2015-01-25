use strict;
use warnings;

use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';
$|= 1;

use vars qw($table $test_dsn $test_user $test_password);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0,
                        mysql_multi_statements => 1 });};

if ($@) {
    plan skip_all => "no database connection";
}
plan tests => 25;

ok (defined $dbh, "Connected to database with multi statement support");

$dbh->{mysql_server_prepare}= 0;

SKIP: {
  skip "Server doesn't support multi statements", 24
  if !MinimumVersion($dbh, '4.1');

  ok($dbh->do("SET SQL_MODE=''"),"init connection SQL_MODE non strict");

  ok($dbh->do("DROP TABLE IF EXISTS $table"), "clean up");

  ok($dbh->do("CREATE TABLE $table (a INT)"), "create table");

  ok($dbh->do("INSERT INTO $table VALUES (1); INSERT INTO $table VALUES (2);"), "2 inserts");

   # Check that a second do() doesn't fail with an 'Out of sync' error
  ok($dbh->do("INSERT INTO $table VALUES (3); INSERT INTO $table VALUES (4);"), "2 more inserts");

  # Check that more_results works for non-SELECT results too
  my $sth;
  ok($sth = $dbh->prepare("UPDATE $table SET a=5 WHERE a=1; UPDATE $table SET a='6-' WHERE a<4"));
  ok($sth->execute(), "Execute updates");
  is($sth->rows, 1, "First update affected 1 row");
  is($sth->{mysql_warning_count}, 0, "First update had no warnings");
  ok($sth->{Active}, "Statement handle is Active");
  ok($sth->more_results());
  is($sth->rows, 2, "Second update affected 2 rows");
  is($sth->{mysql_warning_count}, 2, "Second update had 2 warnings");
  ok(not $sth->more_results());
  ok($sth->finish());

  # Now run it again without calling more_results().
  ok($sth->execute(), "Execute updates again");
  ok($sth->finish());

  # Check that do() doesn't fail with an 'Out of sync' error
  is($dbh->do("DELETE FROM $table"), 4, "Delete all rows");

  # Test that do() reports errors from all result sets
  $dbh->{RaiseError} = $dbh->{PrintError} = 0;
  ok(!$dbh->do("INSERT INTO $table VALUES (1); INSERT INTO bad_$table VALUES (2);"), "do() reports errors");

  # Test that execute() reports errors from only the first result set
  ok($sth = $dbh->prepare("UPDATE $table SET a=2; UPDATE bad_$table SET a=3"));
  ok($sth->execute(), "Execute updates");
  ok(!$sth->err(), "Err was not set after execute");
  ok(!$sth->more_results());
  ok($sth->err(), "Err was set after more_results");
};

$dbh->disconnect();
