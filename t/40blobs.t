use strict;
use warnings;

use Test::More;
use DBI;
use vars qw($test_dsn $test_user $test_password);
use lib '.', 't';
require 'lib.pl';

sub ShowBlob($) {
    my ($blob) = @_;
    my $b;
    for (my $i = 0;  $i < 8;  $i++) {
        if (defined($blob)  &&  length($blob) > $i) {
            $b = substr($blob, $i*32);
        }
        else {
            $b = "";
        }
        note sprintf("%08lx %s\n", $i*32, unpack("H64", $b));
    }
}

my $charset= 'DEFAULT CHARSET=utf8';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1});

plan tests => 14;

if (!MinimumVersion($dbh, '4.1')) {
    $charset= '';
}

my $size= 128;

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t40blobs"), "Drop table if exists dbd_mysql_t40blobs";

my $create = <<EOT;
CREATE TABLE dbd_mysql_t40blobs (
    id INT(3) NOT NULL DEFAULT 0,
    name BLOB ) $charset
EOT

ok ($dbh->do($create));

my ($blob, $qblob) = "";
my $b = "";
for (my $j = 0;  $j < 256;  $j++) {
    $b .= chr($j);
}
for (1 .. $size) {
    $blob .= $b;
}
ok ($qblob = $dbh->quote($blob));

#   Insert a row into the test table.......
my ($query);
$query = "INSERT INTO dbd_mysql_t40blobs VALUES(1, $qblob)";
ok ($dbh->do($query));

#   Now, try SELECT'ing the row out.
ok (my $sth = $dbh->prepare("SELECT * FROM dbd_mysql_t40blobs WHERE id = 1"));

ok ($sth->execute);

ok (my $row = $sth->fetchrow_arrayref);

ok defined($row), "row returned defined";

is @$row, 2, "records from dbd_mysql_t40blobs returned 2";

is $$row[0], 1, 'id set to 1';

cmp_ok byte_string($$row[1]), 'eq', byte_string($blob), 'blob set equal to blob returned';

ShowBlob($blob), ShowBlob(defined($$row[1]) ? $$row[1] : "");

ok ($sth->finish);

ok $dbh->do("DROP TABLE dbd_mysql_t40blobs"), "Drop table dbd_mysql_t40blobs";

ok $dbh->disconnect;
