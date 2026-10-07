use strict;
use warnings;

use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';

# https://github.com/perl5-dbi/DBD-mysql/issues/486
# With AutoCommit off, mysql_auto_reconnect does not reconnect, so prepare()
# on a disconnected handle must fail instead of returning a dead statement.

use vars qw($test_dsn $test_user $test_password);

my $dbh;
eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
    { RaiseError => 1, AutoCommit => 1});};

if ($@) {
  diag $@;
  plan skip_all => "no database connection";
}
$dbh->disconnect();

plan tests => 2 * 4;

for my $mysql_server_prepare (0, 1) {
  $dbh = DBI->connect("$test_dsn;mysql_server_prepare=$mysql_server_prepare",
    $test_user, $test_password,
    { RaiseError => 0, PrintError => 0, AutoCommit => 0, mysql_auto_reconnect => 1 });

  ok($dbh->disconnect(), "disconnect (server_prepare=$mysql_server_prepare)");
  ok(!$dbh->prepare("SELECT 1"), "prepare fails after disconnect");
  is($dbh->errstr, "Statement not active", "error message");
  ok(!$dbh->{Active}, "handle is still inactive");
}
