#!perl -w
#
#   $Id$
#
#   This tests, whether the number of rows can be retrieved.
#
use strict;
use DBI;
use Test::More;
use Carp qw(croak);
use Data::Dumper;
use vars qw($table $test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my ($dbh, $sth, $aref);
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all => 
        "ERROR: $DBI::errstr. Can't continue test";
}
plan tests => 22; 

ok $dbh->do("DROP TABLE IF EXISTS $table");

my $create= <<EOT;
CREATE TABLE $table (
  id INT(4) NOT NULL DEFAULT 0,
  name varchar(64) NOT NULL DEFAULT ''
) 
EOT

ok $dbh->do($create), "CREATE TABLE $table";

ok $dbh->do("INSERT INTO $table VALUES( 1, 'Alligator Descartes' )"), 'inserting first row';

$sth = $dbh->prepare("SELECT * FROM $table WHERE id = 1") or die "$DBI::errstr";

ok $sth->execute;

cmp_ok $sth->rows, '==', 1, '\$sth->rows should be 1';

$aref= $sth->fetchall_arrayref or die "$DBI::errstr";

cmp_ok scalar @$aref, '==', 1, 'Verified rows should be 1';

ok $sth->finish;

ok $dbh->do("INSERT INTO $table VALUES( 2, 'Jochen Wiedmann' )"), 'inserting second row';

$sth = $dbh->prepare("SELECT * FROM $table WHERE id >= 1") or die "$DBI::errstr";

ok $sth->execute;

cmp_ok $sth->rows, '==', 2, '\$sth->rows should be 2';

$aref= $sth->fetchall_arrayref or die "$DBI::errstr";

cmp_ok scalar @$aref, '==', 2, 'Verified rows should be 2';

ok $sth->finish;

ok $dbh->do("INSERT INTO $table VALUES(3, 'Tim Bunce')"), "inserting third row";

$sth = $dbh->prepare("SELECT * FROM $table WHERE id >= 2") or die "$DBI::errstr";

ok $sth->execute;

cmp_ok $sth->rows, '==', 2, 'rows should be 2'; 

$aref= $sth->fetchall_arrayref or die "$DBI::errstr";

cmp_ok scalar @$aref, '==', 2, 'Verified rows should be 2';

ok $sth->finish;

$sth = $dbh->prepare("SELECT * FROM $table") or die "$DBI::errstr";

ok $sth->execute;

cmp_ok $sth->rows, '==', 3, 'rows should be 3'; 

$aref= $sth->fetchall_arrayref or die "$DBI::errstr";

cmp_ok scalar @$aref, '==', 3, 'Verified rows should be 3';

ok $dbh->do("DROP TABLE $table"), "drop table $table";

ok $dbh->disconnect;
