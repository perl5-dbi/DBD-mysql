use strict;
use warnings;

use Test::More;
use DBI;
$|= 1;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

plan tests => 4;

ok(defined $dbh, "Connected to database");

ok($dbh->do("DROP TABLE IF EXISTS dbd_mysql_t20createdrop"), "making slate clean");

ok($dbh->do("CREATE TABLE dbd_mysql_t20createdrop (id INT(4), name VARCHAR(64))"), "creating dbd_mysql_t20createdrop");

ok($dbh->do("DROP TABLE dbd_mysql_t20createdrop"), "dropping created dbd_mysql_t20createdrop");

$dbh->disconnect();
