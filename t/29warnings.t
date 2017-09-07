use strict;
use warnings;

use Test::More;
use DBI;
use lib '.', 't';
require 'lib.pl';
$|= 1;

use vars qw($test_dsn $test_user $test_password);

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0});};

if ($@) {
    plan skip_all => "no database connection";
}

if ( !MinimumVersion($dbh, '4.1') ) {
    plan skip_all => "Servers < 4.1 do not report warnings";
}

my $expected_warnings = 1;
if ( MinimumVersion($dbh, '5.5') ) {
    $expected_warnings = 2;
}

plan tests => 16;

ok(defined $dbh, "Connected to database");

ok(my $sth= $dbh->prepare("DROP TABLE IF EXISTS no_such_table"));
ok($sth->execute());

is($sth->{mysql_warning_count}, 1, 'warnings from sth');

ok($dbh->do("SET sql_mode=''"));
ok($dbh->do("DROP TABLE IF EXISTS dbd_drv_sth_warnings"));
ok($dbh->do("CREATE TABLE dbd_drv_sth_warnings (c CHAR(1))"));
ok($dbh->do("INSERT INTO dbd_drv_sth_warnings (c) VALUES ('perl'), ('dbd'), ('mysql')"));
is($dbh->{mysql_warning_count}, 3, 'warnings from dbh');


# tests to make sure mysql_warning_count is the same as reported by mysql_info();
# see https://rt.cpan.org/Ticket/Display.html?id=29363

ok($dbh->do("DROP TABLE IF EXISTS dbd_drv_count_warnings"));

ok($dbh->do("CREATE TABLE dbd_drv_count_warnings (i TINYINT NOT NULL)") );

my $q = "INSERT INTO dbd_drv_count_warnings VALUES (333),('as'),(3)";

ok($sth = $dbh->prepare($q));
ok($sth->execute());

is($sth->{'mysql_warning_count'}, 2 );

# $dbh->{info} actually uses mysql_info()
my $str = $dbh->{info};
my $numwarn;
if ( $str =~ /Warnings:\s(\d+)$/ ) {
    $numwarn = $1;
}

# this test passes on mysql 5.5.x and fails on 5.1.x
# but I'm not sure which versions, so I'll just disable it for now
is($numwarn, $expected_warnings);

ok($dbh->disconnect);
