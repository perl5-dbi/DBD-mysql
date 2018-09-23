use strict;
use warnings;

use Test::More ;
use DBI;
$|= 1;

use vars qw($test_user $test_password $test_db $test_dsn);
use lib 't', '.';
require 'lib.pl';

# remove database from DSN
$test_dsn =~ s/^DBI:mysql:([^:]+)(:?)/DBI:mysql:$2/;

# This should result in a cached sha2 password entry
# The result is that subsequent connections don't need
# TLS or the RSA pubkey.
$test_dsn .= ';mysql_ssl=1;mysql_get_server_pubkey=1';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    diag $@;
    plan skip_all => "no database connection";
}
plan tests => 2;

ok defined $dbh, "Connected to database";
ok $dbh->disconnect();
