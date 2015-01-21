use strict;
use warnings;

use DBI;
use Test::More;

use vars qw($mdriver);
$|= 1;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';
my $dbh;
my $ur = 0;

# yes, we will reconnect, but I want to keep the "fail if not connect"
# separate from the actual test where we reconnect
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 1 });};
if ($@) {
    plan skip_all => "ERROR: $DBI::errstr $DBI::err Can't continue test";
}
plan tests => 6;

for $ur (0,1) {
    $test_dsn .= ";mysql_use_result=1" if $ur;
    # reconnect
    ok ($dbh->disconnect());
    ok ($dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 }));
    is $dbh->{mysql_use_result}, $ur, "mysql_use_result set to $ur";
}
