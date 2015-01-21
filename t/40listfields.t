use strict;
use warnings;

use DBI;
use Test::More;
use vars qw($COL_NULLABLE $test_dsn $test_user $test_password);
use lib '.', 't';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password);
my $quoted;

my $create;

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};

if ($@) {
    plan skip_all => "ERROR: $DBI::errstr. Can't continue test";
}
plan tests => 25;

$dbh->{mysql_server_prepare}= 0;

$create = <<EOC;
CREATE TEMPORARY TABLE dbd_mysql_40listfields (
    id INT(4) NOT NULL,
    name VARCHAR(64),
    key id (id)
    )
EOC

ok $dbh->do($create), "create table dbd_mysql_40listfields";

ok $dbh->table_info(undef,undef,'dbd_mysql_40listfields'), "table info for dbd_mysql_40listfields";

ok $dbh->column_info(undef,undef,'dbd_mysql_40listfields','%'), "column_info for dbd_mysql_40listfields";

my $sth= $dbh->column_info(undef,undef,"this_does_not_exist",'%');

ok $sth, "\$sth defined";

ok !$sth->err(), "not error";

$sth = $dbh->prepare("SELECT * FROM dbd_mysql_40listfields");

ok $sth, "prepare succeeded";

ok $sth->execute, "execute select";

my $res;
$res = $sth->{'NUM_OF_FIELDS'};

ok $res, "$sth->{NUM_OF_FIELDS} defined";

is $res, 2, "\$res $res == 2";

my $ref = $sth->{'NAME'};

ok $ref, "\$sth->{NAME} defined";

cmp_ok $$ref[0], 'eq', 'id', "$$ref[0] eq 'id'";

cmp_ok $$ref[1], 'eq', 'name', "$$ref[1] eq 'name'";

$ref = $sth->{'NULLABLE'};

ok $ref, "nullable";

ok !($$ref[0] xor (0 & $COL_NULLABLE));
ok !($$ref[1] xor (1 & $COL_NULLABLE));

$ref = $sth->{TYPE};

cmp_ok $ref->[0], 'eq', DBI::SQL_INTEGER(), "SQL_INTEGER";

cmp_ok $ref->[1], 'eq', DBI::SQL_VARCHAR(), "SQL_VARCHAR";

ok ($sth= $dbh->prepare("DROP TABLE dbd_mysql_40listfields"));

ok($sth->execute);

ok (! defined $sth->{'NUM_OF_FIELDS'});

$quoted = eval { $dbh->quote(0, DBI::SQL_INTEGER()) };

ok (!$@);

cmp_ok $quoted, 'eq', '0', "equals '0'";

$quoted = eval { $dbh->quote('abc', DBI::SQL_VARCHAR()) };

ok (!$@);

cmp_ok $quoted, 'eq', "\'abc\'", "equals 'abc'";


ok($dbh->disconnect());
