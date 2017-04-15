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
plan skip_all => 'Server supports SSL connections, cannot test fallback to plain text' if $have_ssl and $have_ssl->{Value} eq 'YES';

plan tests => 2;

$dbh = DBI->connect($test_dsn, $test_user, $test_password, { PrintError => 1, RaiseError => 0, mysql_ssl => 1, mysql_ssl_optional => 1 });
ok(defined $dbh, 'DBD::mysql supports mysql_ssl_optional=1 and connect via plain text protocol when SSL is not supported by server') or diag('Error code: ' . ($DBI::err || 'none') . "\n" . 'Error message: ' . ($DBI::errstr || 'unknown'));

$dbh = DBI->connect($test_dsn, $test_user, $test_password, { PrintError => 1, RaiseError => 0, mysql_ssl => 1, mysql_ssl_optional => 1, mysql_ssl_ca_file => "" });
ok(defined $dbh, 'DBD::mysql supports mysql_ssl_optional=1 and connect via plain text protocol when SSL is not supported by server even with mysql_ssl_ca_file') or diag('Error code: ' . ($DBI::err || 'none') . "\n" . 'Error message: ' . ($DBI::errstr || 'unknown'));
