use strict;
use warnings;

use lib 't', '.';
require 'lib.pl';
use DBI;
use Test::More;
use Carp qw(croak);
use vars qw($table $test_dsn $test_user $test_password);

my ($row, $vers, $test_procs, $dbh, $sth);
eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1})};

if ($@) {
    plan skip_all =>
        "no database connection";
}

#
# DROP/CREATE PROCEDURE will give syntax error
# for versions < 5.0
#
if (!MinimumVersion($dbh, '5.0') ) {
    plan skip_all =>
        "You must have MySQL version 5.0 and greater for this test to run";
}

if (!CheckRoutinePerms($dbh)) {
    plan skip_all =>
        "Your test user does not have ALTER_ROUTINE privileges.";
}

plan tests => 31;

$dbh->disconnect();

ok ($dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1}));

ok $dbh->do("DROP TABLE IF EXISTS $table");

my $drop_proc= "DROP PROCEDURE IF EXISTS testproc";

ok ($dbh->do($drop_proc), "DROP PROCEDURE") or diag "errstr=$DBI::errstr, err=$DBI::err";


my $proc_create = <<EOPROC;
create procedure testproc() deterministic
  begin
    declare a,b,c,d int;
    set a=1;
    set b=2;
    set c=3;
    set d=4;
    select a, b, c, d;
    select d, c, b, a;
    select b, a, c, d;
    select c, b, d, a;
  end
EOPROC

ok $dbh->do($proc_create);

my $proc_call = 'CALL testproc()';

ok $dbh->do($proc_call);

my $proc_select = 'SELECT @a';
ok ($sth = $dbh->prepare($proc_select));

ok $sth->execute();

ok $sth->finish;

ok $dbh->do("DROP PROCEDURE testproc");

ok $dbh->do("drop procedure if exists test_multi_sets");

$proc_create = <<EOT;
        create procedure test_multi_sets ()
        deterministic
        begin
        select user() as first_col;
        select user() as first_col, now() as second_col;
        select user() as first_col, now() as second_col, now() as third_col;
        end
EOT

ok $dbh->do($proc_create);

ok ($sth = $dbh->prepare("call test_multi_sets()"));

ok $sth->execute();

is $sth->{NUM_OF_FIELDS}, 1, "num_of_fields == 1";

my $resultset;
ok ($resultset = $sth->fetchrow_arrayref());

ok defined $resultset;

is @$resultset, 1, "1 row in resultset";

undef $resultset;

ok $sth->more_results();

is $sth->{NUM_OF_FIELDS}, 2, "NUM_OF_FIELDS == 2";

ok ($resultset= $sth->fetchrow_arrayref());

ok defined $resultset;

is @$resultset, 2, "2 rows in resultset";

undef $resultset;

ok $sth->more_results();

is $sth->{NUM_OF_FIELDS}, 3, "NUM_OF_FIELDS == 3";

ok ($resultset= $sth->fetchrow_arrayref());

ok defined $resultset;

is @$resultset, 3, "3 Rows in resultset";

ok $sth->more_results();

is $sth->{NUM_OF_FIELDS}, 0, "NUM_OF_FIELDS == 0"; +

local $SIG{__WARN__} = sub { die @_ };

ok $sth->finish;

ok $dbh->disconnect();
