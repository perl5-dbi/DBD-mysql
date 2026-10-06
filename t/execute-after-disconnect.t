use strict;
use warnings;

use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';

# Executing a statement handle after its database handle was disconnected
# used to segfault as the closed MYSQL connection was still being used.

use vars qw($test_dsn $test_user $test_password);

my $dbh;
eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
    { RaiseError => 1, PrintError => 0, AutoCommit => 1});};
if ($@) {
  diag $@;
  plan skip_all => "no database connection";
}
$dbh->disconnect;

for my $server_prepare (0, 1) {
  for my $auto_reconnect (0, 1) {
    my $desc = "mysql_server_prepare=$server_prepare mysql_auto_reconnect=$auto_reconnect";
    $dbh = DBI->connect($test_dsn, $test_user, $test_password,
      { RaiseError => 1, PrintError => 0, AutoCommit => 1,
        mysql_server_prepare => $server_prepare,
        mysql_auto_reconnect => $auto_reconnect });

    my $sth = $dbh->prepare("SELECT ?");
    ok $dbh->disconnect, "disconnect ($desc)";

    my $row = eval { $sth->execute("1"); $sth->fetchrow_arrayref };
    if ($auto_reconnect && !$server_prepare) {
      is $@, '', "execute reconnects ($desc)";
      is_deeply $row, ['1'], "got result after reconnect ($desc)";
    }
    else {
      # Server side prepared statements belong to the closed connection
      # and can't be executed on a new connection, so don't reconnect.
      like $@, qr/execute failed: Database handle not active/,
        "execute fails ($desc)";
      ok !defined $row, "no result ($desc)";
      ok !$dbh->{Active}, "not reconnected ($desc)";
    }

    $sth->finish;
    $dbh->disconnect;
  }
}

done_testing;
