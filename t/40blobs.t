#!perl -w
#
#   $Id$
#
#   This is a test for correct handling of BLOBS; namely $dbh->quote
#   is expected to work correctly.
#


use DBI ();
use Test::More;
use vars qw($table $test_dsn $test_user $test_password);
use lib '.', 't';
require 'lib.pl';

sub ShowBlob($) {
    my ($blob) = @_;
    for ($i = 0;  $i < 8;  $i++) {
        if (defined($blob)  &&  length($blob) > $i) {
            $b = substr($blob, $i*32);
        }
        else {
            $b = "";
        }
        printf("%08lx %s\n", $i*32, unpack("H64", $b));
    }
}

my $dbh;
eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1}) or ServerError() ;};

if ($@) {
    plan skip_all => "ERROR: $DBI::errstr. Can't continue test";
}
plan tests => 12;

my $size= 128;
ok $dbh->do("DROP TABLE IF EXISTS $table"), "Drop table if exists $table";
my $create = <<EOT;
CREATE TABLE $table (
    id INT(3) NOT NULL DEFAULT 0,
    name BLOB ) DEFAULT CHARSET=utf8
EOT

ok ($dbh->do($create));

my ($blob, $qblob) = "";
my $b = "";
for ($j = 0;  $j < 256;  $j++) {
    $b .= chr($j);
}
for ($i = 0;  $i < $size;  $i++) {
    $blob .= $b;
}
$qblob = $dbh->quote($blob);
ok $qblob, 'Blob properly quoted';

#   Insert a row into the test table.......
my ($query);
$query = "INSERT INTO $table VALUES(1, $qblob)";
ok ($dbh->do($query));

#   Now, try SELECT'ing the row out.
$sth = $dbh->prepare("SELECT * FROM $table WHERE id = 1")
        or die "unable to query $table " . $dbh->errstr;

ok $sth, "prepare of query of $table succeeded";
ok ($sth->execute);

$row = $sth->fetchrow_arrayref or die "Unable to select row from query";
ok defined($row), "row returned defined";

cmp_ok @$row, '==', 2, "records from $table returned 2";
cmp_ok $$row[0], '==', 1, 'id set to 1';
cmp_ok byte_string($$row[1]), 'eq', byte_string($blob), 'blob set equal to blob returned';

ShowBlob($blob), ShowBlob(defined($$row[1]) ? $$row[1] : "");

ok ($sth->finish);

ok $dbh->do("DROP TABLE $table"), "Drop table $table";
