use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1 });
my $have_ssl = eval { $dbh->selectrow_hashref("SHOW VARIABLES WHERE Variable_name = 'have_ssl'") };
$dbh->disconnect();
plan skip_all => 'Server does not support SSL connections' unless $have_ssl and $have_ssl->{Value} eq 'YES';

plan tests => 4;

$dbh = DBI->connect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 0, mysql_ssl => 1, mysql_ssl_optional => 1 });
ok(defined $dbh, 'DBD::mysql supports mysql_ssl=1 with mysql_ssl_optional=1 and connect to server') or diag('Error code: ' . ($DBI::err || 'none') . "\n" . 'Error message: ' . ($DBI::errstr || 'unknown'));

ok(defined $dbh && defined $dbh->{mysql_ssl_cipher}, 'SSL connection was established') and diag("mysql_ssl_cipher is: ". $dbh->{mysql_ssl_cipher});

$dbh = DBI->connect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 0, mysql_ssl => 1 });
if (defined $dbh) {
  pass('DBD::mysql supports mysql_ssl=1 without mysql_ssl_optional=1 and connect to server');
  ok(defined $dbh->{mysql_ssl_cipher}, 'SSL connection was established');
} else {
  is($DBI::errstr, 'SSL connection error: Enforcing SSL encryption is not supported', 'DBD::mysql supports mysql_ssl=1 without mysql_ssl_optional=1 and fail because cannot enforce SSL encryption') or diag('Error message: ' . ($DBI::errstr || 'unknown'));
  is($DBI::err, 2026, 'DBD::mysql error code is SSL related') or diag('Error code: ' . ($DBI::err || 'unknown'));
}
