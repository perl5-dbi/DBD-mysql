use strict;
use warnings;

use DBI;
use DBI::Const::GetInfoType;
use Test::More;
select(($|=1,select(STDERR),$|=1)[1]);
use lib 't', '.';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password);

my ($dbh, $t);
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 1 });};
if ($@) {
    plan skip_all => "no database connection";
}
plan tests => 98;

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t51bind_type_guessing"),
            "drop table if exists dbd_mysql_t51bind_type_guessing";

my $create= <<"EOTABLE";
create table dbd_mysql_t51bind_type_guessing (
    id bigint unsigned not null default 0
    )
EOTABLE

ok $dbh->do($create), "creating table";

my $statement= "insert into dbd_mysql_t51bind_type_guessing (id) values (?)";

my $sth1;
ok $sth1= $dbh->prepare($statement);

my $rows;
ok $rows= $sth1->execute('9999999999999999');
cmp_ok $rows, '==',  1;

$statement= "update dbd_mysql_t51bind_type_guessing set id = ?";
my $sth2;
ok $sth2= $dbh->prepare($statement);

ok $rows= $sth2->execute('9999999999999998');
cmp_ok $rows, '==',  1;

$dbh->{mysql_bind_type_guessing}= 1;
ok $rows= $sth1->execute('9999999999999997');
cmp_ok $rows, '==',  1;

$statement= "update dbd_mysql_t51bind_type_guessing set id = ? where id = ?";

ok $sth2= $dbh->prepare($statement);
ok $rows= $sth2->execute('9999999999999996', '9999999999999997');

my $retref;
ok $retref= $dbh->selectall_arrayref(
    "select * from dbd_mysql_t51bind_type_guessing");

cmp_ok $retref->[0][0], '==', 9999999999999998;
cmp_ok $retref->[1][0], '==', 9999999999999996;

# checking varchars/empty strings/misidentification:
$create= <<"EOTABLE";
create table dbd_mysql_t51bind_type_guessing (
    id bigint default 0 not null,
    nn bigint default 0,
    dd double(12,4),
    str varchar(80),
    primary key (id)
    ) engine=innodb
EOTABLE

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t51bind_type_guessing"), "drop table if exists dbd_mysql_t51bind_type_guessing";

ok $dbh->do($create), "creating table with int, double, and varchar";

my @sts;
$t= "prepare insert integer col nn into dbd_mysql_t51bind_type_guessing";
ok $sts[0] = $dbh->prepare("insert into dbd_mysql_t51bind_type_guessing (id,nn) values (?,?)"), $t;
$t= "prepare update double col dd dbd_mysql_t51bind_type_guessing";
ok $sts[1] = $dbh->prepare("update dbd_mysql_t51bind_type_guessing set dd = ? where id = ?"), $t;
$t= "prepare update string col str dbd_mysql_t51bind_type_guessing";
ok $sts[2] = $dbh->prepare("update dbd_mysql_t51bind_type_guessing set str = ? where id = ?"), $t;

# various values to try including issue 251
my @vals = ( 52.3,
    '     77.7777',
    '.1',
    '5e3',
    +1,
    -1,
    undef,
    '5e',
    '1+',
    '+',
    '.',
    'e5',
);

my $val;
# the tests for 'like' are when values fail to be inserted/updated
for my $i (0 .. 11) {
    $val = $vals[$i];
    if (defined $val) {
        $t= "insert int val $val id $i"
    }
    else {
        $t= "insert undef into int id $i";
    }
    if ($i >= 8) {
        eval {
            $rows= $sts[0]->execute($i, $val);
        };
        if ($i == 8) {
            like ($@, qr{Data truncated for column}, $t);
        }
        else {
            like ($@, qr{Incorrect integer value}, $t);
        }
        $rows= $sts[0]->execute($i, 0);
    }
    else {
        ok $rows= $sts[0]->execute($i, $val),$t;
    }

    if (defined $val) {
        $t= "update double val $val id $i";
    }
    else {
        $t= "update double val undefined id $i";
    }
    if ($i >= 7) {
        eval {
            $rows = $sts[1]->execute($val, $i);
        };
        if ($dbh->{mysql_serverversion} < 90000) {
            like ($@, qr{Data truncated for column}, $t);
        } else {
            like ($@, qr{Incorrect DOUBLE value}, $t);
        }
        $rows= $sts[1]->execute(0, $i);
    }
    else {
        ok $rows= $sts[1]->execute($val,$i),$t;
    }

    if (defined $val) {
        $t= "update string val $val id $i";
    }
    else {
        $t= "update string val undef id $i";
    }
    ok $rows = $sts[2]->execute($val,$i),$t;
}

for my $i (0 .. 2) {
    $sts[$i]->finish();
}

# expected results
my $res= [
          [ 0, 52, '52.3', '52.3' ],
          [ 1, 78, '77.7777', '77.7777' ],
          [ 2, 0, '0.1', '0.1' ],
          [ 3, 5000, '5000', '5e3' ],
          [ 4, 1, '1', '1' ],
          [ 5, -1, '-1', '-1' ],
          [ 6, undef, undef, undef ],
          [ 7, 5, '0', '5e' ],
          [ 8, 0, '0', '1+' ],
          [ 9, 0, '0', '+' ],
          [ 10, 0, '0', '.' ],
          [ 11, 0, '0', 'e5' ]
	  ];

$t= "Select all values";
my $query= "select * from dbd_mysql_t51bind_type_guessing";

ok $retref = $dbh->selectall_arrayref($query), $t;

for my $i (0 .. $#$res) {
    if ($i == 6) {
        is($retref->[$i][1], undef, "$i: nn undefined as expected");
        is($retref->[$i][2], undef, "$i: dd undefined as expected");
        is($retref->[$i][3], undef, "$i: str undefined as expected");
    }
    else {
        cmp_ok $retref->[$i][1], '==', $res->[$i][1],
            "test: " . "$retref->[$i][1], '==', $res->[$i][1]";
        cmp_ok $retref->[$i][2], 'eq', $res->[$i][2],
            "test: " . "$retref->[$i][2], '==', $res->[$i][2]";
        cmp_ok $retref->[$i][3], 'eq', $res->[$i][3],
            "test: " . "$retref->[$i][2], '==', $res->[$i][2]";
    }
}

my $sth3;
$t = "Prepare limit statement";
ok $sth3= $dbh->prepare("select * from dbd_mysql_t51bind_type_guessing limit ?"), $t;
$val = 1;
$t = "select with limit $val statement";
ok $rows= $sth3->execute($val), $t;
$val = '    1';
$t = "select with limit $val statement";
ok $rows= $sth3->execute($val), $t;
$sth3->finish();

ok $dbh->do("DROP TABLE dbd_mysql_t51bind_type_guessing");
ok $dbh->disconnect;
