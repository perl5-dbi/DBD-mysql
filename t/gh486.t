use strict;
use warnings;

use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';

# https://github.com/perl5-dbi/DBD-mysql/issues/486
# prepare() on a disconnected handle should reconnect when
# mysql_auto_reconnect is enabled.

use vars qw($test_dsn $test_user $test_password);

my $dbh;
eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
    { RaiseError => 1, AutoCommit => 1});};

if ($@) {
  diag $@;
  plan skip_all => "no database connection";
}
$dbh->disconnect();

plan tests => 2 * 9;

for my $mysql_server_prepare (0, 1) {
  $dbh = DBI->connect("$test_dsn;mysql_server_prepare=$mysql_server_prepare",
    $test_user, $test_password,
    { RaiseError => 1, PrintError => 0, AutoCommit => 1, mysql_auto_reconnect => 1 });

  my $sth = $dbh->prepare("SELECT 1");
  ok($sth->execute(), "execute before disconnect (server_prepare=$mysql_server_prepare)");
  is_deeply($sth->fetchrow_arrayref(), [1], "fetch before disconnect");
  $sth->finish();

  ok($dbh->disconnect(), "disconnect");
  ok(!$dbh->{Active}, "handle is inactive");

  $sth = eval { $dbh->prepare("SELECT 2") };
  ok($sth, "prepare reconnects after disconnect") or diag $@;
  SKIP: {
    skip "prepare failed", 3 unless $sth;
    ok($dbh->{Active}, "handle is active again");
    ok($sth->execute(), "execute after reconnect");
    is_deeply($sth->fetchrow_arrayref(), [2], "fetch after reconnect");
    $sth->finish();
  }

  ok($dbh->disconnect(), "final disconnect");
}
