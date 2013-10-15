#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use Test::More;
use lib 't', '.';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password $table);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 1 });};
if ($@) {
    plan skip_all =>
        "ERROR: $DBI::errstr. Can't continue test";
}
plan tests => 36;

for my $mysql_server_prepare (0) {
eval {$dbh= DBI->connect($test_dsn . ';mysql_server_prepare=' . $mysql_server_prepare, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 1 });};

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

$rows = [ [1, ''], [2, ' '], [3, ' a b c '], [4, 'blah'] ];

for my $ref (@$rows) {
	my ($id, $name) = @$ref;
        ok $sth->execute($id, $name), "insert into $table values ($id, '$name')";
	ok $sth2->execute($id), "select id, name from $table where id = $id";

	# First try to retreive without chopping blanks.
	$sth2->{'ChopBlanks'} = 0;
        my $ret_ref = [];
	ok ($ret_ref = $sth2->fetchrow_arrayref);
	cmp_ok $ret_ref->[1], 'eq', $name, "\$name should not have blanks chopped";

	# Now try to retreive with chopping blanks.
	$sth2->{'ChopBlanks'} = 1;

	ok $sth2->execute($id);

	my $n = $name;
	$n =~ s/\s+$//;
        $ret_ref = [];
	ok ($ret_ref = $sth2->fetchrow_arrayref);

	cmp_ok $ret_ref->[1], 'eq', $n, "should have blanks chopped";

}
ok $sth->finish;
ok $sth2->finish;
ok $dbh->do("DROP TABLE $table"), "drop $table";
ok $dbh->disconnect;
}
