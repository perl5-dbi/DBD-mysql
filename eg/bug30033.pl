#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use Getopt::Long;
use DBI;

our $opt_prepared;

GetOptions (
    "p|prepared"  => \$opt_prepared
    );


my $dsn = "DBI:mysql:database=test;host=localhost";
$dsn .= ";mysql_server_prepare=1" if $opt_prepared;

my $dbh = DBI->connect( $dsn, 'root', '', { RaiseError => 1 } )
or die $DBI::errstr;

unlink('./bug3033.trace.log');
$dbh->trace(4, './bug3033_trace.log'); 

$dbh->do('DROP TABLE IF EXISTS buggy');
$dbh->do('CREATE TABLE buggy ( id int(3) )');
$dbh->do('INSERT INTO buggy (id) VALUES (1)');
my $query= "SELECT id FROM
-- It's a bug
buggy WHERE id = ?";
print "with var:\n";
my $ref= $dbh->selectall_arrayref($query, {}, 1);
print Dumper $ref;
print "with string terminator:\n";
$ref= $dbh->selectall_arrayref(<<END, {}, 1);
SELECT id
FROM buggy
-- It's a bug!
WHERE id = ?
END
print Dumper $ref;
