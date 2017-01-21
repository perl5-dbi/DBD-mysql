use strict;
use warnings FATAL => 'all';

use DBI;
use Test::More;
use vars qw($test_dsn $test_user $test_password);
use vars qw($COL_NULLABLE $COL_KEY);
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};
if ($@) {
    plan skip_all => "no database connection";
}

#
# DROP/CREATE PROCEDURE will give syntax error for these versions
#
if (!MinimumVersion($dbh, '5.0')) {
    plan skip_all =>
        "SKIP TEST: You must have MySQL version 5.0 and greater for this test to run";
}
plan tests => 92 * 2;

for my $mysql_server_prepare (0, 1) {
$dbh= DBI->connect("$test_dsn;mysql_server_prepare=$mysql_server_prepare;mysql_server_prepare_disable_fallback=1", $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t55utf8");

my $create =<<EOT;
CREATE TABLE dbd_mysql_t55utf8 (
    name VARCHAR(64) CHARACTER SET utf8,
    bincol BLOB,
    shape GEOMETRY,
    binutf VARCHAR(64) CHARACTER SET utf8 COLLATE utf8_bin,
    profile TEXT CHARACTER SET utf8,
    str2 VARCHAR(64) CHARACTER SET utf8,
    ascii VARCHAR(64) CHARACTER SET latin1,
    latin VARCHAR(64) CHARACTER SET latin1
)
EOT

ok $dbh->do($create);

my $unicode_str        = "\N{U+0100}dam";   # Unicode "Adam" with a macron (internally stored as utf8)
my $quoted_unicode_str = "'\N{U+0100}dam'";

my $blob               = "\x{c4}\x{80}dam"; # UTF-8 representation of $unicode_str
my $quoted_blob        = "'\x{c4}\x{80}dam'";

my $unicode_str2       = "\x{c1}dam";       # Unicode "Adam" with a acute (internally stored as latin1)
my $ascii_str          = "?dam";            # ASCII representation of $unicode_str (and also $unicode_str2)
my $latin1_str2        = "\x{c1}dam";       # Latin1 representation of $unicode_str2 (well, really same as $unicode_str2)
my $blob2              = "\x{c3}\x{81}dam"; # UTF-8 representation of $unicode_str2

cmp_ok $dbh->quote($unicode_str), 'eq', $quoted_unicode_str, 'testing quoting of utf 8 string';

cmp_ok $dbh->quote($blob), 'eq', $quoted_blob, 'testing quoting of blob';

$dbh->{mysql_enable_utf8}=1;
ok $dbh->do("SET NAMES utf8"), 'SET NAMES utf8';
ok $dbh->do("SET SQL_MODE=''"), 'SET SQL_MODE=\'\'';

# GeomFromText() is deprecated as of MySQL 5.7.6, use ST_GeomFromText() instead
my $geomfromtext = $dbh->{mysql_serverversion} >= 50706 ? 'ST_GeomFromText' : 'GeomFromText';

my $query = <<EOI;
INSERT INTO dbd_mysql_t55utf8 (name, bincol, shape, binutf, profile, str2, ascii, latin)
    VALUES (?, ?, $geomfromtext('Point(132865 501937)'), ?, ?, ?, ?, ?)
EOI

# Do not use prepared statements because ST_GeomFromText() is not supported
# With SET SQL_MODE='' is mysql_server_prepare_disable_fallback not working
# And without SET SQL_MODE='' below 'Incorrect string value' are fatal errors, not warnings...
my $sth = $dbh->prepare($query, { mysql_server_prepare => 0 }) or die "$DBI::errstr";
ok $sth->bind_param(1, $unicode_str);
ok $sth->bind_param(2, $blob, DBI::SQL_BINARY);
ok $sth->bind_param(3, $unicode_str);
ok $sth->bind_param(4, $unicode_str);
ok $sth->bind_param(5, $unicode_str2);
ok $sth->bind_param(6, $unicode_str);
ok $sth->bind_param(7, $unicode_str2);
ok $sth->execute() or die("Execute failed: ".$DBI::errstr);
ok $sth->finish;

cmp_ok($dbh->{mysql_warning_count}, '==', 1, 'got warning for INSERT') or do { diag("SHOW WARNINGS:"); diag($_->[2]) foreach @{$dbh->selectall_arrayref("SHOW WARNINGS", { mysql_server_prepare => 0 })}; };
like($dbh->selectrow_arrayref("SHOW WARNINGS", { mysql_server_prepare => 0 })->[2], qr/^(?:Incorrect string value: '\\xC4\\x80dam'|Data truncated) for column 'ascii' at row 1$/);

# AsBinary() is deprecated as of MySQL 5.7.6, use ST_AsBinary() instead
my $asbinary = $dbh->{mysql_serverversion} >= 50706 ? 'ST_AsBinary' : 'AsBinary';

$query = "SELECT name,bincol,$asbinary(shape), binutf, profile, str2, ascii, latin FROM dbd_mysql_t55utf8 LIMIT 1";
$sth = $dbh->prepare($query) or die "$DBI::errstr";

ok $sth->execute;

my $ref;
$ref = $sth->fetchrow_arrayref ;

ok defined $ref;

cmp_ok $ref->[0], 'eq', $unicode_str;
cmp_ok $ref->[1], 'eq', $blob;
cmp_ok $ref->[3], 'eq', $unicode_str;
cmp_ok $ref->[4], 'eq', $unicode_str;
cmp_ok $ref->[5], 'eq', $unicode_str2;
cmp_ok $ref->[6], 'eq', $ascii_str;
cmp_ok $ref->[7], 'eq', $latin1_str2;

ok !utf8::is_utf8($ref->[1]), 'returned blob is not internally stored as utf8';
ok !utf8::is_utf8($ref->[2]), 'returned blob is not internally stored as utf8';

cmp_ok $ref->[1], 'eq', $blob, "compare $ref->[1] eq $blob";

ok $sth->finish;

$dbh->{mysql_enable_utf8}=0;
$ref = $dbh->selectrow_arrayref($query);
ok defined $ref, 'got data';
cmp_ok $ref->[0], 'eq', $blob, 'utf8 data are not utf8 decoded when mysql_enable_utf8 is disabled';
cmp_ok $ref->[1], 'eq', $blob, 'utf8 data are not utf8 decoded when mysql_enable_utf8 is disabled';
cmp_ok $ref->[3], 'eq', $blob, 'utf8 data are not utf8 decoded when mysql_enable_utf8 is disabled';
cmp_ok $ref->[4], 'eq', $blob, 'utf8 data are not utf8 decoded when mysql_enable_utf8 is disabled';
cmp_ok $ref->[5], 'eq', $blob2, 'utf8 data are not utf8 decoded when mysql_enable_utf8 is disabled';
cmp_ok $ref->[6], 'eq', $ascii_str, 'latin1 data are not utf8 decoded when mysql_enable_utf8 is disabled';
cmp_ok $ref->[7], 'eq', $blob2, 'latin1 data are not utf8 decoded when mysql_enable_utf8 is disabled';
ok !utf8::is_utf8($ref->[0]), 'value does not have utf8 flag when mysql_enable_utf8 is disabled';
ok !utf8::is_utf8($ref->[1]), 'value does not have utf8 flag when mysql_enable_utf8 is disabled';
ok !utf8::is_utf8($ref->[2]), 'value does not have utf8 flag when mysql_enable_utf8 is disabled';
ok !utf8::is_utf8($ref->[3]), 'value does not have utf8 flag when mysql_enable_utf8 is disabled';
ok !utf8::is_utf8($ref->[4]), 'value does not have utf8 flag when mysql_enable_utf8 is disabled';
ok !utf8::is_utf8($ref->[5]), 'value does not have utf8 flag when mysql_enable_utf8 is disabled';
ok !utf8::is_utf8($ref->[6]), 'value does not have utf8 flag when mysql_enable_utf8 is disabled';
ok !utf8::is_utf8($ref->[7]), 'value does not have utf8 flag when mysql_enable_utf8 is disabled';

my @warnmsgs;
$SIG{__WARN__} = sub { push @warnmsgs, $_[0]; };
ok $dbh->selectrow_arrayref("SELECT 1 FROM dbd_mysql_t55utf8 WHERE name = ? AND str2 = $quoted_unicode_str", {}, $unicode_str);
$SIG{__WARN__} = 'DEFAULT';
is scalar @warnmsgs, 2, 'got warnings for wide character without mysql_enable_utf8';
like $warnmsgs[0], qr/^Wide character in statement but mysql_enable_utf8 not set /, '';
like $warnmsgs[1], qr/^Wide character in field 1 but mysql_enable_utf8 not set /, '';

@warnmsgs = ();
$SIG{__WARN__} = sub { push @warnmsgs, $_[0]; };
ok $sth = $dbh->prepare("SELECT 1 FROM dbd_mysql_t55utf8 WHERE name = ? AND bincol = ? AND str2 = $quoted_unicode_str");
like $warnmsgs[0], qr/^Wide character in statement but mysql_enable_utf8 not set /, '';
ok $sth->execute($unicode_str, $unicode_str);
like $warnmsgs[1], qr/^Wide character in field 1 but mysql_enable_utf8 not set /, '';
like $warnmsgs[2], qr/^Wide character in field 2 but mysql_enable_utf8 not set /, '';
ok $sth->execute($blob, $blob2);
ok $sth->finish();
$SIG{__WARN__} = 'DEFAULT';
is scalar @warnmsgs, 3, 'got warnings for wide character';

@warnmsgs = ();
$SIG{__WARN__} = sub { push @warnmsgs, $_[0]; };
ok $sth = $dbh->prepare("SELECT 1 FROM dbd_mysql_t55utf8 WHERE name = ? AND bincol = ? AND str2 = $quoted_unicode_str");
like $warnmsgs[0], qr/^Wide character in statement but mysql_enable_utf8 not set /, '';
ok $sth->bind_param(1, $unicode_str);
like $warnmsgs[1], qr/^Wide character in field 1 but mysql_enable_utf8 not set /, '';
ok $sth->bind_param(2, $unicode_str, DBI::SQL_BINARY);
like $warnmsgs[2], qr/^Wide character in binary field 2 /, '';
ok $sth->execute();
ok $sth->finish();
$SIG{__WARN__} = 'DEFAULT';
is scalar @warnmsgs, 3, 'got warnings for wide character';

$dbh->{mysql_enable_utf8}=1;
ok $dbh->do("SET NAMES latin1"), 'SET NAMES latin1';
$ref = $dbh->selectrow_arrayref($query);
ok defined $ref, 'got data';
cmp_ok $ref->[0], 'eq', $ascii_str, 'utf8 data are returned as latin1 when NAMES is latin1';
cmp_ok $ref->[1], 'eq', $blob, 'blob is unchanged when NAMES is latin1';
cmp_ok $ref->[3], 'eq', $ascii_str, 'utf8 data are returned as latin1 when NAMES is latin1';
cmp_ok $ref->[4], 'eq', $ascii_str, 'utf8 data are returned as latin1 when NAMES is latin1';
cmp_ok $ref->[5], 'eq', $latin1_str2, 'utf8 data are returned as latin1 when NAMES is latin1';
cmp_ok $ref->[6], 'eq', $ascii_str, 'latin1 data are returned as latin1 when NAMES is latin1';
cmp_ok $ref->[7], 'eq', $latin1_str2, 'latin1 data are returned as latin1 when NAMES is latin1';
ok !utf8::is_utf8($ref->[0]), 'value does not have utf8 flag when NAMES is latin1';
ok !utf8::is_utf8($ref->[1]), 'value does not have utf8 flag when NAMES is latin1';
ok !utf8::is_utf8($ref->[2]), 'value does not have utf8 flag when NAMES is latin1';
ok !utf8::is_utf8($ref->[3]), 'value does not have utf8 flag when NAMES is latin1';
ok !utf8::is_utf8($ref->[4]), 'value does not have utf8 flag when NAMES is latin1';
ok !utf8::is_utf8($ref->[5]), 'value does not have utf8 flag when NAMES is latin1';
ok !utf8::is_utf8($ref->[6]), 'value does not have utf8 flag when NAMES is latin1';
ok !utf8::is_utf8($ref->[7]), 'value does not have utf8 flag when NAMES is latin1';

@warnmsgs = ();
$SIG{__WARN__} = sub { push @warnmsgs, $_[0]; };
ok $sth = $dbh->prepare("SELECT 1 FROM dbd_mysql_t55utf8 WHERE bincol = ?");
ok $sth->bind_param(1, $unicode_str, DBI::SQL_BINARY);
like $warnmsgs[0], qr/^Wide character in binary field 1 /, '';
ok $sth->execute();
ok $sth->finish();
$SIG{__WARN__} = 'DEFAULT';
is scalar @warnmsgs, 1, 'got warnings for UTF-8 encoded binary field';

ok $dbh->do("DROP TABLE dbd_mysql_t55utf8");

ok $dbh->disconnect;
}
