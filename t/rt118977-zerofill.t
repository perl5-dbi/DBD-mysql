use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
require "t/lib.pl";

my $dbh = eval { DBI->connect($test_dsn, $test_user, $test_password, { PrintError => 1, RaiseError => 1 }) };
plan skip_all => "no database connection" if $@ or not $dbh;

# Tested with TiDB v8.5.1.
if ($dbh->{'mysql_serverinfo'} =~ 'TiDB') {
    plan skip_all =>
        # https://docs.pingcap.com/tidb/stable/mysql-compatibility/#incompatibility-due-to-deprecated-features
        "SKIP TEST: TiDB doesn't support ZEROFILL";
}

plan tests => 4*2;

for my $mysql_server_prepare (0, 1) {

	$dbh->{mysql_server_prepare} = $mysql_server_prepare;

	ok $dbh->do("DROP TABLE IF EXISTS t");
	ok $dbh->do("CREATE TEMPORARY TABLE t(id smallint(5) unsigned zerofill)");
	ok $dbh->do("INSERT INTO t(id) VALUES(1)");
	is $dbh->selectcol_arrayref("SELECT id FROM t")->[0], "00001";

}
