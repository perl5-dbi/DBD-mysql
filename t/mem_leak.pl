#!/usr/bin/perl

use strict;
use DBI;

my $test_dsn = 'DBI:mysql:test';
my $test_user= 'root';
my $test_password= '';

my $dbh = DBI->connect($test_dsn, $test_user, $test_password);

while (1) {
    my $tmp=$dbh->{mysql_dbd_stats};
}
