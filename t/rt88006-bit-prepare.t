use strict;
use warnings;

use vars qw($test_dsn $test_user $test_password);
use DBI;
use Test::More;
use lib 't', '.';
require 'lib.pl';

for my $scenario (qw(prepare noprepare)) {

my $dbh;

my $dsn = $test_dsn;
$dsn .= ';mysql_server_prepare=1;' if $scenario eq 'prepare';
eval {$dbh = DBI->connect($dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1})};

if ($@) {
  plan skip_all => "no database connection";
}

my $create = <<EOT;
CREATE TEMPORARY TABLE `dbd_mysql_rt88006_bit_prep` (
  `id_binary_test` bigint(20) NOT NULL auto_increment,
  `flags` bit(32) NOT NULL,
  PRIMARY KEY  (`id_binary_test`),
  KEY `flags` (`flags`)
)
EOT

ok $dbh->do($create),"create table for $scenario";

ok $dbh->do("INSERT INTO `dbd_mysql_rt88006_bit_prep` (`id_binary_test`, `flags`) VALUES (2, b'10'), (1, b'1')");

my $sth = $dbh->prepare("SELECT id_binary_test,flags FROM dbd_mysql_rt88006_bit_prep");
ok $sth->execute() or die("Execute failed: ".$DBI::errstr);
ok (my $r = $sth->fetchrow_hashref(), "fetchrow_hashref for $scenario");
is ($r->{id_binary_test}, 1, 'id_binary test contents');
TODO: {
  local $TODO = "rt88006" if $scenario eq 'prepare';
  ok ($r->{flags}, 'flags has contents');
}
ok $sth->finish;

ok $sth = $dbh->prepare("SELECT id_binary_test,BIN(flags) FROM dbd_mysql_rt88006_bit_prep");
ok $sth->execute() or die("Execute failed: ".$DBI::errstr);
ok ($r = $sth->fetchrow_hashref(), "fetchrow_hashref for $scenario with BIN()");
is ($r->{id_binary_test}, 1, 'id_binary test contents');
ok ($r->{'BIN(flags)'}, 'flags has contents');

ok $sth->finish;
ok $dbh->disconnect;
}

done_testing;
