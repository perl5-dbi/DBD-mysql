use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { PrintError => 1, RaiseError => 1 });

plan tests => 4*2;

for my $mysql_server_prepare (0, 1) {
SKIP: {
	skip 'SKIP TEST: You must have MySQL version 4.1 and greater for this test to run', 4
		if !MinimumVersion($dbh, '4.1') && $mysql_server_prepare == 1;

	$dbh->{mysql_server_prepare} = $mysql_server_prepare;

	ok $dbh->do("DROP TABLE IF EXISTS t");
	ok $dbh->do("CREATE TEMPORARY TABLE t(id smallint(5) unsigned zerofill)");
	ok $dbh->do("INSERT INTO t(id) VALUES(1)");
	is $dbh->selectcol_arrayref("SELECT id FROM t")->[0], "00001";
}
}
