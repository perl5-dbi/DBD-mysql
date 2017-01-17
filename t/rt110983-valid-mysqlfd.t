use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
require "t/lib.pl";

my $dbh = eval { DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1, AutoCommit => 0 }) };
plan skip_all => "no database connection" if $@ or not $dbh;

plan tests => 4;

ok($dbh->mysql_fd >= 0, '$dbh->mysql_fd returns valid file descriptor when $dbh connection is open');
ok($dbh->{sockfd} >= 0, '$dbh->{sockfd} returns valid file descriptor when $dbh connection is open');

$dbh->disconnect;

ok(!defined $dbh->mysql_fd, '$dbh->mysql_fd returns undef when $dbh connection was closed');
ok(!defined $dbh->{sockfd}, '$dbh->{sockfd} returns undef when $dbh connection was closed');
