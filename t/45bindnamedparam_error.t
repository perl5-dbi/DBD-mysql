use strict;
use warnings;

use Test::More;
use DBI;
use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1}) or ServerError();};

if ($@) {
    plan skip_all => "no database connection";
}
plan tests => 15;

SKIP: {
    ok $dbh->do('SET @@auto_increment_offset = 1');
    ok $dbh->do('SET @@auto_increment_increment = 1');
}

my $create= <<EOT;
CREATE TEMPORARY TABLE dbd_mysql_t45bindnamedparam (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    num INT(3))
EOT

ok $dbh->do($create), "create table dbd_mysql_t45bindnamedparam";

ok $dbh->do("INSERT INTO dbd_mysql_t45bindnamedparam VALUES(NULL, 1)"), "insert into dbd_mysql_t45bindnamedparam (null, 1)";

my $rows;
ok ($rows= $dbh->selectall_arrayref("SELECT * FROM dbd_mysql_t45bindnamedparam"));

is $rows->[0][1], 1, "\$rows->[0][1] == 1";

my $sth;
ok ($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t45bindnamedparam WHERE num = :num"));

$dbh->{PrintError} = 0;
$dbh->{PrintWarn} = 0;
eval {($sth->bind_param(":num", 1, SQL_INTEGER()));};
$dbh->{PrintError} = 1;
$dbh->{PrintWarn} = 1;
ok defined($DBI::errstr);

like($DBI::errstr, qr/named parameters are unsupported/, 'bind_param reports expected error with named parameter (string type)');

ok ($sth->finish());


ok ($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t45bindnamedparam WHERE num = :num"));

$dbh->{PrintError} = 0;
$dbh->{PrintWarn} = 0;
eval {($sth->bind_param("\0:num", 1, SQL_INTEGER()));};
$dbh->{PrintError} = 1;
$dbh->{PrintWarn} = 1;
ok defined($DBI::errstr);

like($DBI::errstr, qr/could not be coerced to a C string/, 'bind_param reports expected error with named parameter (non-string type)');

ok ($sth->finish());


ok ($dbh->disconnect());
