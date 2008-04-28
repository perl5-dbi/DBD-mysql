#!perl -w
#
#   $Id$
#
#   This is a skeleton test. For writing new tests, take this file
#   and modify/extend it.
#

use Test::More;
use DBI ();
use strict;
use lib 't', '.';
require 'lib.pl';
$|= 1;
our ($dbh, $drh, $state, $test_dsn, $test_user, $test_password, $mdriver, $dbdriver);

$drh = DBI->install_driver($mdriver);

if (! defined $drh) {
    plan skip_all => "Can't obtain driver handle ERROR: $DBI::errstr. Can't continue test";
}

$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

if (! defined $dbh) {
    plan skip_all => "Can't connect to database ERROR: $DBI::errstr. Can't continue test";
}
if (! defined $drh) {
    plan skip_all => "Can't obtain driver handle. Can't continue test";
}

unless ($dbh->can('take_imp_data')) {
    plan skip_all => "version of DBI $DBI::VERSION doesn't support this test. Can't continue test";
}
plan tests => 21;

pass("obtained driver handle");
pass("connected to database");

my $id= connection_id($dbh);
ok defined($id), "Initial connection: $id\n";

$drh = $dbh->{Driver};
ok $drh, "Driver handle defined\n";

my $imp_data;
$imp_data = $dbh->take_imp_data;

ok $imp_data, "Didn't get imp_data";

my $imp_data_length= length($imp_data);
cmp_ok $imp_data_length, '>=', 80, 
    "test that our imp_data is greater than or equal to 80, actual $imp_data_length";

is $drh->{Kids}, 0, 
    'our Driver should have 0 Kid(s) after calling take_imp_data';

{
    my $warn;
    local $SIG{__WARN__} = sub { ++$warn if $_[0] =~ /after take_imp_data/ };

    my $drh = $dbh->{Driver};
    ok !defined($drh), '... our Driver should be undefined';

    my $trace_level = $dbh->{TraceLevel};
    ok !defined($trace_level) ,'our TraceLevel should be undefined';

    ok !defined($dbh->disconnect), 'disconnect should return undef';

    ok !defined($dbh->quote(42)), 'quote should return undefined';

    is $warn, 4, 'we should have received 4 warnings';
}

my $dbh2 = DBI->connect($test_dsn, $test_user, $test_password,
    { dbi_imp_data => $imp_data });

# XXX: how can we test that the same connection is used?
my $id2 = connection_id($dbh2);
print "Overridden connection: $id2\n";

cmp_ok $id,'==', $id2, "the same connection: $id => $id2\n";

my $drh2;
ok $drh2 = $dbh2->{Driver}, "can't get the driver\n";

ok $dbh2->isa("DBI::db"), 'isa test';
# need a way to test dbi_imp_data has been used

is $drh2->{Kids}, 1,
    "our Driver should have 1 Kid(s) again: having " .  $drh2->{Kids} . "\n";

is $drh2->{ActiveKids}, 1,
    "our Driver should have 1 ActiveKid again: having " .  $drh2->{ActiveKids} . "\n";

read_write_test($dbh2);

# must cut the connection data again
ok ($imp_data = $dbh2->take_imp_data), "didn't get imp_data";


sub read_write_test {
    my ($dbh)= @_;

    # now the actual test:

    my $table= 't1';
    ok $dbh->do("DROP TABLE IF EXISTS $table"), "Drop table $table if exists error: Error $dbh->err, $dbh->errstr";

    my $def= <<EOT;
CREATE TABLE $table (
        id int(4) NOT NULL default 0,
        name varchar(64) NOT NULL default '' );
EOT
    
    ok $dbh->do($def) ,"Create table $table error: $dbh->err, $dbh->errstr";

    ok $dbh->do("DROP TABLE $table"), "Drop table $table error: $dbh->err, $dbh->errstr";
}

