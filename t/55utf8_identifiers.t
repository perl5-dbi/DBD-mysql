use strict;
use warnings;

use Test::More;
use DBI;
use Encode;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

sub for_db {
    my ($mysql_enable_utf8, $value) = @_; # Value is in internal Perl Unicode.

    my $ret;
    if ($mysql_enable_utf8 >= 1) {
        $ret = $value;
    } else {
        $ret = Encode::encode('UTF-8', $value);
    }

    return $ret;
}

my $dbh;
eval {
    $dbh = DBI->connect($test_dsn, $test_user, $test_password,
                       { RaiseError => 1, PrintError => 1, AutoCommit => 0 });
};
if ($@) {
    plan skip_all => "no database connection";
}
$dbh->disconnect();

plan tests => 12 * 3 + 12 * 2;

# All in internal Perl Unicode
my $jpnTable = "\N{U+8868}"; # Japanese table
my $jpnColumn = "\N{U+6027}\N{U+5225}"; # Japanese column - word "gender"
my $jpnData1 = "\N{U+5c71}\N{U+7530}\N{U+592a}\N{U+90ce}"; # Japanese data - person name
my $jpnData2 = "\N{U+7537}"; # Japanese daya - word "male"
my $chiTable = "\N{U+5927}\N{U+99AC}"; # Chinese table XXX MySQL doesn't support utf8mb4 in table names
my $chiColumn = "\N{U+5C0F}\N{U+96EA}\N{U+4EBA}"; # Chinese column XXX MySQL doesn't support utf8mb4 in column names
my $chiData1 = "\N{U+30001}"; # Chinese data
my $chiData2 = "\N{U+30002}"; # Chinese data

foreach my $mysql_enable_utf8 (0, 1, 2) {
    my %utf8_params = ();
    if ($mysql_enable_utf8 == 1) {
        $utf8_params{'mysql_enable_utf8'} = 1;
        diag "Enabled mysql_enable_utf8.";
    } elsif ($mysql_enable_utf8 == 2) {
        $utf8_params{'mysql_enable_utf8mb4'} = 1;
        diag "Enabled mysql_enable_utf8mb4.";
    } else {
        diag "Disabled mysql_enable_utf8.";
    }
    $dbh = DBI->connect($test_dsn, $test_user, $test_password,
                       { RaiseError => 1, PrintError => 1, AutoCommit => 1, %utf8_params });

    my $jpnTable_db = for_db($mysql_enable_utf8, $jpnTable);
    my $jpnColumn_db = for_db($mysql_enable_utf8, $jpnColumn);
    my $jpnData1_db = for_db($mysql_enable_utf8, $jpnData1);
    my $jpnData2_db = for_db($mysql_enable_utf8, $jpnData2);
    my ($chiTable_db, $chiColumn_db, $chiData1_db, $chiData2_db);
    if ($mysql_enable_utf8 == 0 || $mysql_enable_utf8 == 2) {
        $chiTable_db = for_db($mysql_enable_utf8, $chiTable);
        $chiColumn_db = for_db($mysql_enable_utf8, $chiColumn);
        $chiData1_db = for_db($mysql_enable_utf8, $chiData1);
        $chiData2_db = for_db($mysql_enable_utf8, $chiData2);
    }

    my $sth;
    my $row;

    ok($dbh->do("DROP TABLE IF EXISTS $jpnTable_db"), 'Drop table for Japanese testing.');
    if ($mysql_enable_utf8 == 0 || $mysql_enable_utf8 == 2) {
        ok($dbh->do("DROP TABLE IF EXISTS $chiTable_db"), 'Drop table for Chinese testings.');
    }

    ok($dbh->do(<<"END"
CREATE TABLE IF NOT EXISTS $jpnTable_db (
  name VARCHAR(20),
  $jpnColumn_db CHAR(1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
END
    ), 'Create temporay table with Japanese characters.');
    if ($mysql_enable_utf8 == 0 || $mysql_enable_utf8 == 2) {
      ok($dbh->do(<<"END"
CREATE TABLE IF NOT EXISTS $chiTable_db (
  name VARCHAR(20),
  $chiColumn_db CHAR(1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
END
      ), 'Create temporay table with Chinese characters.');
    }

    ok($sth = $dbh->prepare("INSERT INTO $jpnTable_db (name, $jpnColumn_db) VALUES (?, ?)"), 'Prepare insert statement with Japanese values.');
    ok($sth->execute($jpnData1_db, $jpnData2_db), 'Execute insert statement with Japanese values.');
    if ($mysql_enable_utf8 == 0 || $mysql_enable_utf8 == 2) {
        ok($sth = $dbh->prepare("INSERT INTO $chiTable_db (name, $chiColumn_db) VALUES (?, ?)"), 'Prepare insert statement with Chinese values.');
        ok($sth->execute($chiData1_db, $chiData2_db), 'Execute insert statement with Chinese values.');
    }

    ok($sth = $dbh->prepare("SELECT * FROM $jpnTable_db"), 'Prepare select statement with Japanese values.');
    ok($sth->execute(), 'Execute select statement with Japanese values.');
    ok($row = $sth->fetchrow_hashref(), 'Fetch hashref with Japanese values.');
    is($row->{name}, $jpnData1_db, "Japanese value.");
    ok(!exists $row->{$jpnColumn}, 'Not exists Japanese key in internal Perl Unicode.'); # XXX
    is($row->{Encode::encode('UTF-8', $jpnColumn)}, $jpnData2_db, 'Exists Japanese key in octets and value.'); # XXX
    is_deeply($sth->{NAME}, [ 'name', Encode::encode('UTF-8', $jpnColumn) ], 'Statement Japanese column name is in octets.'); # XXX
    is_deeply($sth->{mysql_table}, [ Encode::encode('UTF-8', $jpnTable), Encode::encode('UTF-8', $jpnTable) ], 'Statement Japanese table name is in octets.'); # XXX
    if ($mysql_enable_utf8 == 0 || $mysql_enable_utf8 == 2) {
        ok($sth = $dbh->prepare("SELECT * FROM $chiTable_db"), 'Prepare select statement with Chinese values.');
        ok($sth->execute(), 'Execute select statement with Chinese values.');
        ok($row = $sth->fetchrow_hashref(), 'Fetch hashref with Chinese values.');
        is($row->{name}, $chiData1_db, "Chinese value.");
        ok(!exists $row->{$chiColumn}, 'Not exists Chinese key in internal Perl Unicode.'); # XXX
        is($row->{Encode::encode('UTF-8', $chiColumn)}, $chiData2_db, 'Exists Chinese key in octets and value.'); # XXX
        is_deeply($sth->{NAME}, [ 'name', Encode::encode('UTF-8', $chiColumn) ], 'Statement Chinese column name is in octets.'); # XXX
        is_deeply($sth->{mysql_table}, [ Encode::encode('UTF-8', $chiTable), Encode::encode('UTF-8', $chiTable) ], 'Statement Chinese table name is in octets.'); # XXX
    }

    $sth->finish();
    $dbh->disconnect();
}
done_testing;
