#!perl -w
# vim: ft=perl
#
#   $Id$
#
#   This driver should check whether 'ChopBlanks' works.
#

use strict;
use DBI;
use DBI::Const::GetInfoType;
use Test::More;
use lib 't', '.';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password $table);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all => 
        "ERROR: $DBI::errstr. Can't continue test";
}
plan tests => 29; 

ok $dbh->do("DROP TABLE IF EXISTS $table"), "drop table if exists $table";

my $create= <<EOT;
CREATE TABLE $table (
  id INT(4),
  name VARCHAR(64)
)
EOT

ok $dbh->do($create), "create table $table";

ok (my $sth= $dbh->prepare("INSERT INTO $table (id, name) VALUES (?, ?)"));

ok (my $sth2= $dbh->prepare("SELECT id, name FROM $table WHERE id = ?"));

my $rows;

if ($dbh->get_info($GetInfoType{SQL_DBMS_VER}) lt "4.1") {
    $rows = [ [1, ''], [2, ''], [3, ' a b c']];
}
else {
    $rows = [ [1, ''], [2, ' '], [3, ' a b c ']];
}

my $ref;
for $ref (@$rows) {
	my ($id, $name) = @$ref;
    ok $sth->execute($id, $name), "inserting ($id, $name) into $table";
	ok $sth2->execute($id), "selecting where id = $id";

	# First try to retreive without chopping blanks.
	$sth2->{'ChopBlanks'} = 0;
	ok ($ref = $sth2->fetchrow_arrayref);
	cmp_ok $$ref[1], 'eq', $name, "\$name should not have blanks chopped";

	# Now try to retreive with chopping blanks.
	$sth2->{'ChopBlanks'} = 1;

	ok $sth2->execute($id);

	my $n = $name;
	$n =~ s/\s+$//;
	ok ($ref = $sth2->fetchrow_arrayref);

	cmp_ok $$ref[1], 'eq', $n, "should have blanks chopped";

}
ok $sth->finish;
ok $sth2->finish;
ok $dbh->do("DROP TABLE $table"), "drop $table";
ok $dbh->disconnect;
