#!/usr/bin/perl
use strict;
use warnings;

use DBI;

use vars qw($test_dsn $test_user $test_password $state);
require "t/lib.pl";

use Test::More;

my $dbh;
eval {$dbh= DBI->connect( $test_dsn, $test_user, $test_password);};

unless($dbh) {
    plan skip_all => "ERROR: $DBI::errstr Can't continue test";
}

plan tests => 7;

my $create = <<EOC;
CREATE TEMPORARY TABLE dbd_mysql_rt50304_column_info (
    id int(10)unsigned NOT NULL AUTO_INCREMENT,
    problem_column SET('','(Some Text)') DEFAULT NULL,
    regular_column SET('','Some Text') DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY id (id)
);
EOC

ok $dbh->do($create), "create temporary table dbd_mysql_rt50304_column_info";

my $info;

my $sth = $dbh->column_info(undef, undef, 'dbd_mysql_rt50304_column_info', 'problem_column');
$info = $sth->fetchall_arrayref({});
is ( scalar @{$info->[0]->{mysql_values}}, 2, 'problem_column values');
is ( $info->[0]->{mysql_values}->[0], '', 'problem_column first value');
is ( $info->[0]->{mysql_values}->[1], '(Some Text)', 'problem_column second value');

$sth= $dbh->column_info(undef, undef, 'dbd_mysql_rt50304_column_info', 'regular_column');
$info = $sth->fetchall_arrayref({});
is ( scalar @{$info->[0]->{mysql_values}}, 2, 'regular_column values');
is ( $info->[0]->{mysql_values}->[0], '', 'regular_column first value');
is ( $info->[0]->{mysql_values}->[1], 'Some Text', 'regular_column second value');
