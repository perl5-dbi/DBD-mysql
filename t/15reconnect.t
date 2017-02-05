use strict;
use warnings;

use Test::More;
use DBI;
$|= 1;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh;
my $sth;
eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1})};

if ($@) {
  plan skip_all => "no database connection";
}
plan tests => 13 * 2;

for my $mysql_server_prepare (0, 1) {
$dbh= DBI->connect("$test_dsn;mysql_server_prepare=$mysql_server_prepare;mysql_server_prepare_disable_fallback=1", $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

ok(defined $dbh, "Connected to database");

ok($dbh->{Active}, "checking for active handle");

ok($dbh->{mysql_auto_reconnect} = 1, "enabling reconnect");

ok($dbh->{AutoCommit} = 1, "enabling autocommit");

ok($dbh->disconnect(), "disconnecting active handle");

ok(!$dbh->{Active}, "checking for inactive handle");

ok($dbh->do("SELECT 1"), "implicitly reconnecting handle with 'do'");

ok($dbh->{Active}, "checking for reactivated handle");

ok($dbh->disconnect(), "disconnecting active handle");

ok(!$dbh->{Active}, "checking for inactive handle");

ok($sth = $dbh->prepare("SELECT 1"), "prepare statement");

ok($sth->execute(), "implicitly reconnecting handle with executing prepared statement");

ok($dbh->{Active}, "checking for reactivated handle");

$sth->finish();

$dbh->disconnect();
}
