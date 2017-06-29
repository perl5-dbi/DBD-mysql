use strict;
use warnings;

use Test::More;
use DBI;
use DBI::Const::GetInfoType;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my @common_safe_methods = qw/
can                    err   errstr    parse_trace_flag    parse_trace_flags
private_attribute_info trace trace_msg visit_child_handles
/;

my @db_safe_methods   = (@common_safe_methods, qw/
clone mysql_async_ready
/);

my @db_unsafe_methods = qw/
data_sources       do                 last_insert_id     selectrow_array
selectrow_arrayref selectrow_hashref  selectall_arrayref selectall_hashref
selectcol_arrayref prepare            prepare_cached     commit
rollback           begin_work         ping               get_info
table_info         column_info        primary_key_info   primary_key
foreign_key_info   statistics_info    tables             type_info_all
type_info          quote              quote_identifier
/;

my @st_safe_methods   = qw/
fetchrow_arrayref fetch            fetchrow_array fetchrow_hashref
fetchall_arrayref fetchall_hashref finish         rows
/;

my @st_unsafe_methods = qw/
bind_param bind_param_inout bind_param_array execute execute_array
execute_for_fetch bind_col bind_columns
/;

my %dbh_args = (
    can                 => ['can'],
    parse_trace_flag    => ['SQL'],
    parse_trace_flags   => ['SQL'],
    trace_msg           => ['message'],
    visit_child_handles => [sub { }],
    quote               => ['string'],
    quote_identifier    => ['Users'],
    do                  => ['SELECT 1'],
    last_insert_id      => [undef, undef, undef, undef],
    selectrow_array     => ['SELECT 1'],
    selectrow_arrayref  => ['SELECT 1'],
    selectrow_hashref   => ['SELECT 1'],
    selectall_arrayref  => ['SELECT 1'],
    selectall_hashref   => ['SELECT 1', '1'],
    selectcol_arrayref  => ['SELECT 1'],
    prepare             => ['SELECT 1'],
    prepare_cached      => ['SELECT 1'],
    get_info            => [$GetInfoType{'SQL_DBMS_NAME'}],
    column_info         => [undef, undef, '%', '%'],
    primary_key_info    => [undef, undef, 'async_test'],
    primary_key         => [undef, undef, 'async_test'],
    foreign_key_info    => [undef, undef, 'async_test', undef, undef, undef],
    statistics_info     => [undef, undef, 'async_test', 0, 1],
);

my %sth_args = (
    fetchall_hashref  => [1],
    bind_param        => [1, 1],
    bind_param_inout  => [1, \(my $scalar = 1), 64],
    bind_param_array  => [1, [1]],
    execute_array     => [{ ArrayTupleStatus => [] }, [1]],
    execute_for_fetch => [sub { undef } ],
    bind_col          => [1, \(my $scalar2 = 1)],
    bind_columns      => [\(my $scalar3)],
);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 0, PrintError => 0, AutoCommit => 0 });};
if (!$dbh) {
    plan skip_all => "no database connection";
}
unless($dbh->get_info($GetInfoType{'SQL_ASYNC_MODE'})) {
    plan skip_all => "Async support wasn't built into this version of DBD::mysql";
}
plan tests =>
  2 * @db_safe_methods     +
  4 * @db_unsafe_methods   +
  7 * @st_safe_methods     +
  2 * @common_safe_methods +
  2 * @st_unsafe_methods   +
  3;

$dbh->do(<<SQL);
CREATE TEMPORARY TABLE async_test (
    value INTEGER
)
SQL

foreach my $method (@db_safe_methods) {
    $dbh->do('SELECT 1', { async => 1 });
    my $args = $dbh_args{$method} || [];
    $dbh->$method(@$args);
    ok !$dbh->errstr, "Testing method '$method' on DBD::mysql::db during asynchronous operation";

    ok defined($dbh->mysql_async_result);
}

$dbh->do('SELECT 1', { async => 1 });
ok defined($dbh->mysql_async_result);

foreach my $method (@db_unsafe_methods) {
    $dbh->do('SELECT 1', { async => 1 });
    my $args = $dbh_args{$method} || [];
    my @values = $dbh->$method(@$args); # some methods complain unless they're called in list context
    like $dbh->errstr, qr/Calling a synchronous function on an asynchronous handle/, "Testing method '$method' on DBD::mysql::db during asynchronous operation";

    ok defined($dbh->mysql_async_result);
}

foreach my $method (@common_safe_methods) {
    my $sth = $dbh->prepare('SELECT 1', { async => 1 });
    $sth->execute;
    my $args = $dbh_args{$method} || []; # they're common methods, so this should be ok!
    $sth->$method(@$args);
    ok !$sth->errstr, "Testing method '$method' on DBD::mysql::db during asynchronous operation";
    ok defined($sth->mysql_async_result);
}

foreach my $method (@st_safe_methods) {
    my $sth = $dbh->prepare('SELECT 1', { async => 1 });
    $sth->execute;
    my $args = $sth_args{$method} || [];
    $sth->$method(@$args);
    ok !$sth->errstr, "Testing method '$method' on DBD::mysql::st during asynchronous operation";

    # statement safe methods clear async state
    ok !defined($sth->mysql_async_result), "Testing DBD::mysql::st method '$method' clears async state";
    like $sth->errstr, qr/Gathering asynchronous results for a synchronous handle/;
}

foreach my $method (@st_safe_methods) {
    my $sync_sth  = $dbh->prepare('SELECT 1');
    my $async_sth = $dbh->prepare('SELECT 1', { async => 1 });
    $dbh->do('SELECT 1', { async => 1 });
    ok !$sync_sth->execute;
    ok $sync_sth->errstr;
    ok !$async_sth->execute;
    ok $async_sth->errstr;
    $dbh->mysql_async_result;
}

foreach my $method (@db_unsafe_methods) {
    my $sth = $dbh->prepare('SELECT 1', { async => 1 });
    $sth->execute;
    ok !$dbh->do('SELECT 1', { async => 1 });
    ok $dbh->errstr;
    $sth->mysql_async_result;
}

foreach my $method (@st_unsafe_methods) {
    my $sth = $dbh->prepare('SELECT value FROM async_test WHERE value = ?', { async => 1 });
    $sth->execute(1);
    my $args = $sth_args{$method} || [];
    my @values = $sth->$method(@$args);
    like $dbh->errstr, qr/Calling a synchronous function on an asynchronous handle/, "Testing method '$method' on DBD::mysql::st during asynchronous operation";

    ok(defined $sth->mysql_async_result);
}

my $sth = $dbh->prepare('SELECT 1', { async => 1 });
$sth->execute;
ok defined($sth->mysql_async_ready);
ok $sth->mysql_async_result;

undef $sth;
$dbh->disconnect;
