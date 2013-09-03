#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;

use DBI;

my $dsn = "DBI:Pg:database=postgres;host=localhost";
my $dbh = DBI->connect( $dsn, 'postgres', '', { RaiseError => 1 } )
or die $DBI::errstr;

$dbh->do('CREATE TABLE buggy ( id INT )');
$dbh->do('INSERT INTO buggy (id) VALUES (1)');
my $ref= $dbh->selectrow_arrayref(<<END, {}, 1);
SELECT id
FROM buggy
-- it's a bug!
WHERE id = ?
END
print Dumper $ref;
$dbh->do('DROP TABLE buggy');
