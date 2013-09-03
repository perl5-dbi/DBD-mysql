#!/usr/bin/perl

use DBI;
use strict;
use Data::Dumper;
use warnings;
my $connect_string = 'DBI:mysql:database=test';
my $username = 'root';
my $password = '';
my $attributes = {};
my $dbh = DBI->connect($connect_string, $username,
$password, $attributes) || die $DBI::errstr;

# Execute this AT LEAST once so the stored procedure someproc() exists
# at least
$dbh->do("drop procedure if exists someproc") or print $DBI::errstr;

# Comment this out to reproduce the bug
$dbh->do("create procedure someproc() deterministic begin ".
 "declare a,b,c,d int; set a=1; set b=2; set c=3; set d=4; ".
 "select a, b, c, d; select d, c, b, a; select b, a, c, d; ".
 "select c, b, d, a; end") or print $DBI::errstr;
my $sth=$dbh->prepare('call someproc()') || die $DBI::errstr;
$sth->execute || die $DBI::errstr;
my @row= $sth->fetchrow_array();
print Dumper \@row;
my $more_results = $sth->more_results();
print $more_results . "\n";
@row= $sth->fetchrow_array();
print Dumper \@row;
$more_results = $sth->more_results();
print $more_results . "\n";
@row= $sth->fetchrow_array();
print Dumper \@row;
$more_results = $sth->more_results();
print $more_results . "\n";
@row= $sth->fetchrow_array();
print Dumper \@row;
$more_results = $sth->more_results();
print $more_results . "\n";
@row= $sth->fetchrow_array();
print Dumper \@row;
$more_results = $sth->more_results();
print $more_results . "\n";
