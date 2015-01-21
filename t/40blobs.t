use strict;
use warnings;

use Test::More;
use DBI;
use vars qw($table $test_dsn $test_user $test_password);
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
        printf("%08lx %s\n", $i*32, unpack("H64", $b));
    }
}

my $dbh;
my $charset= 'DEFAULT CHARSET=utf8';

eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1}) or ServerError() ;};

if ($@) {
    plan skip_all => "ERROR: $DBI::errstr. Can't continue test";
}
else {
    plan tests => 14;
}

if (!MinimumVersion($dbh, '4.1')) {
    $charset= '';
}

my $size= 128;

ok $dbh->do("DROP TABLE IF EXISTS $table"), "Drop table if exists $table";

my $create = <<EOT;
CREATE TABLE $table (
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
$query = "INSERT INTO $table VALUES(1, $qblob)";
ok ($dbh->do($query));

#   Now, try SELECT'ing the row out.
ok (my $sth = $dbh->prepare("SELECT * FROM $table WHERE id = 1"));

ok ($sth->execute);

ok (my $row = $sth->fetchrow_arrayref);

ok defined($row), "row returned defined";

is @$row, 2, "records from $table returned 2";

is $$row[0], 1, 'id set to 1';

cmp_ok byte_string($$row[1]), 'eq', byte_string($blob), 'blob set equal to blob returned';

ShowBlob($blob), ShowBlob(defined($$row[1]) ? $$row[1] : "");

ok ($sth->finish);

ok $dbh->do("DROP TABLE $table"), "Drop table $table";

ok $dbh->disconnect;
