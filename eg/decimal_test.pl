#!/usr/bin/perl

use DBI;

## get DBI type map
my %map;
foreach (@{ $DBI::EXPORT_TAGS{sql_types} }) {
    $map{&{"DBI::$_"}} = $_;
}

my $dbh = DBI->connect('DBI:mysql:test;mysql_emulated_prepare=1', 'root');
my $table = 'mysql5bug';
my $drop   = "DROP TABLE IF EXISTS $table";
my $create = "CREATE TABLE $table (value decimal(5,2));";
my $select = "SELECT * FROM $table WHERE 1 = 0";

## create table and get column types
$dbh->do($drop)   or die $dbh->errstr;
$dbh->do($create) or die $dbh->errstr;
my $sth = $dbh->prepare( $select );
my $rv = $sth->execute;
my $fields = $sth->{NAME};
my $types  = $sth->{TYPE};

## print out column types
foreach (0..$#$fields) {
    printf("%8s  %3d  %s\n", $fields->[$_], $types->[$_],
    $map{$types->[$_]});
}

## cleanup
$dbh->do($drop)   or die $dbh->errstr;
$sth->finish;
$dbh->disconnect;

1;
