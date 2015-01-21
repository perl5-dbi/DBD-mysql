use strict;
use warnings;

use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';

use Test::More;

$| = 1;

use vars qw($test_dsn $test_user $test_password);

my $dbh;
eval {$dbh= DBI->connect( $test_dsn, $test_user, $test_password);};
if ($@) {
    plan skip_all => "$@. Can't continue test";
}

my $drh    = $dbh->{Driver};
if (! defined $drh) {
    plan skip_all => "Can't obtain driver handle. Can't continue test";
}

unless ($DBI::VERSION ge '1.607') {
    plan skip_all => "version of DBI $DBI::VERSION doesn't support this test. Can't continue test";
}
unless ($dbh->can('take_imp_data')) {
    plan skip_all => "version of DBI $DBI::VERSION doesn't support this test. Can't continue test";
}
plan tests => 10;

pass("Connected to database");
pass("Obtained driver handle");

my $connection_id1 = connection_id($dbh);

is $drh->{Kids},       1, "1 kid";
is $drh->{ActiveKids}, 1, "1 active kid";

my $imp_data = $dbh->take_imp_data;
is $drh->{Kids},       0, "no kids";
is $drh->{ActiveKids}, 0, "no active kids";
$dbh = DBI->connect( $test_dsn, $test_user, $test_password,
      { dbi_imp_data => $imp_data } );
my $connection_id2 = connection_id($dbh);
is $connection_id1, $connection_id2, "got same session";

is $drh->{Kids},       1, "1 kid";
is $drh->{ActiveKids}, 1, "1 active kid";

ok $dbh->disconnect, "Disconnect OK";
