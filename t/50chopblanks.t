use strict;
use warnings;

use DBI;
use Test::More;
use lib 't', '.';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 1 });};
if ($@) {
    plan skip_all => "no database connection";
}
plan tests => 36 * 2;

for my $mysql_server_prepare (0, 1) {
eval {$dbh= DBI->connect("$test_dsn;mysql_server_prepare=$mysql_server_prepare;mysql_server_prepare_disable_fallback=1", $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 1 });};

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t50chopblanks"), "drop table if exists dbd_mysql_t50chopblanks";

my $create= <<EOT;
CREATE TABLE dbd_mysql_t50chopblanks (
  id INT(4),
  name VARCHAR(64)
)
EOT

ok $dbh->do($create), "create table dbd_mysql_t50chopblanks";

ok (my $sth= $dbh->prepare("INSERT INTO dbd_mysql_t50chopblanks (id, name) VALUES (?, ?)"));

ok (my $sth2= $dbh->prepare("SELECT id, name FROM dbd_mysql_t50chopblanks WHERE id = ?"));

my $rows;

$rows = [ [1, ''], [2, ' '], [3, ' a b c '], [4, 'blah'] ];

for my $ref (@$rows) {
	my ($id, $name) = @$ref;
        ok $sth->execute($id, $name), "insert into dbd_mysql_t50chopblanks values ($id, '$name')";
	ok $sth2->execute($id), "select id, name from dbd_mysql_t50chopblanks where id = $id";

	# First try to retrieve without chopping blanks.
	$sth2->{'ChopBlanks'} = 0;
        my $ret_ref = [];
	ok ($ret_ref = $sth2->fetchrow_arrayref);
	cmp_ok $ret_ref->[1], 'eq', $name, "\$name should not have blanks chopped";

	# Now try to retrieve with chopping blanks.
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
ok $dbh->do("DROP TABLE dbd_mysql_t50chopblanks"), "drop dbd_mysql_t50chopblanks";
ok $dbh->disconnect;
}
