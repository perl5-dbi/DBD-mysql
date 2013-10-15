#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use Carp qw(croak);
use Test::More;
use vars qw($table $test_dsn $test_user $test_password);
use vars qw($COL_NULLABLE $COL_KEY);
use lib 't', '.';
require 'lib.pl';

my $dbh;
$test_dsn .= ';mysql_server_prepare=1';
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all => "ERROR: $@. Can't continue test";
}

#
# DROP/CREATE PROCEDURE will give syntax error for these versions
#
if (!MinimumVersion($dbh, '5.0')) {
    plan skip_all =>
        "SKIP TEST: You must have MySQL version 5.0 and greater for this test to run";
}

plan tests => 11;

ok $dbh->do("DROP TABLE IF EXISTS $table");

my $create =<<EOT;
CREATE TABLE $table (
    data LONGBLOB
)
EOT

ok $dbh->do($create);

$dbh->do("insert into $table (data) values(null)");

my $sth = $dbh->prepare("select data from $table");
ok $sth->execute;
my $row = $sth->fetch;
is $row->[0] => undef;

ok $sth->finish;

$dbh->do("insert into $table (data) values('a')");
$sth = $dbh->prepare("select data from $table");
ok $sth->execute;
$row = $sth->fetch;
is $row->[0] => undef;
$row = $sth->fetch;
is $row->[0] => 'a';

ok $sth->finish;

ok $dbh->do("DROP TABLE $table");

ok $dbh->disconnect;
