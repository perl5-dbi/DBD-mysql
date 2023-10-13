use strict;
use warnings;

use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';

my ($dbhA, $dbhB);
use vars qw($test_dsn $test_user $test_password);

my $dsnA = $test_dsn . ';mysql_enable_utf8mb4=1';
eval {$dbhA = DBI->connect($dsnA, $test_user, $test_password,
    { RaiseError => 1, AutoCommit => 1});};

my $dsnB = $test_dsn;
$dsnB =~ s/DBI:mysql/DBI:mysql(mysql_enable_utf8mb4=1)/;
eval {$dbhB = DBI->connect($dsnB . ';mysql_enable_utf8mb4=1', $test_user, $test_password,
    { RaiseError => 1, AutoCommit => 1});};

plan tests => 2;

ok($dbhA->{mysql_enable_utf8mb4} == 1, 'mysql_enable_utf8mb4 == 1 with regular DSN');

ok($dbhB->{mysql_enable_utf8mb4} == 1, 'mysql_enable_utf8mb4 == 1 with driver DSN');
