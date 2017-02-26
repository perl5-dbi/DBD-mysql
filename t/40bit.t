use strict;
use warnings;

use Test::More;
use DBI;
use vars qw($test_dsn $test_user $test_password);
use lib '.', 't';
require 'lib.pl';

sub VerifyBit ($) {
}

my $charset= 'DEFAULT CHARSET=utf8';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1});

if ($dbh->{mysql_serverversion} < 50008) {
    plan skip_all => "Servers < 5.0.8 do not support b'' syntax";
}

plan tests => 15;

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_b1"), "Drop table if exists dbd_mysql_b1";

ok( $dbh->do('CREATE TABLE dbd_mysql_b1 (b BIT(8))') );

ok ($dbh->do("insert into dbd_mysql_b1 set b = b'11111111'"));
ok ($dbh->do("insert into dbd_mysql_b1 set b = b'1010'"));
ok ($dbh->do("insert into dbd_mysql_b1 set b = b'0101'"));

ok (my $sth = $dbh->prepare("select BIN(b+0) FROM dbd_mysql_b1"));

ok ($sth->execute);

ok (my $result = $sth->fetchall_arrayref);

ok defined($result), "result returned defined";

is $result->[0][0], 11111111, "should be 11111111";
is $result->[1][0], 1010, "should be 1010";
is $result->[2][0], 101, "should be 101";

ok ($sth->finish);

ok $dbh->do("DROP TABLE dbd_mysql_b1"), "Drop table dbd_mysql_b1";

ok $dbh->disconnect;
