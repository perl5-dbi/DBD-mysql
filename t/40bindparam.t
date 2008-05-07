#!perl -w
#
#   $Id$ 
#

use vars qw($table $test_dsn $test_user $test_password);

use DBI ();
use Test::More;
use lib 't', '.';
require 'lib.pl';

my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};

if ($@) {
    plan skip_all => "ERROR: $DBI::errstr. Can't continue test";
}

$sth= $dbh->prepare("select version()") or 
            die "Unable to select version " . $dbh->errstr;

$sth->execute() or die "Unable to execute select version " . $dbh->errstr;;

$row= $sth->fetchrow_arrayref() or 
            die "Unable to select row containing version " . $dbh->errstr;

# 
# DROP/CREATE PROCEDURE will give syntax error 
# for these versions
#
if ($row->[0] =~ /^4\.0/ || $row->[0] =~ /^3/) {
    plan skip_all => "Version of MySQL $row->[0] doesn't support stored procedures";
}
plan tests => 38;

ok ($dbh->do("DROP TABLE IF EXISTS $table"));

my $create = <<EOT;
CREATE TABLE $table (
        id int(4) NOT NULL default 0,
        name varchar(64) default ''
        )
EOT

ok ($dbh->do($create));

$sth = $dbh->prepare("INSERT INTO $table VALUES (?, ?)") or
    die "Unable to prepare insert " . $dbh->errstr;

ok $sth;

# Automatic type detection
my $numericVal = 1;
my $charVal = "Alligator Descartes";
ok ($sth->execute($numericVal, $charVal));

# Does the driver remember the automatically detected type?
ok ($sth->execute("3", "Jochen Wiedmann"));

$numericVal = 2;
$charVal = "Tim Bunce";
ok ($sth->execute($numericVal, $charVal));

# Now try the explicit type settings
ok ($sth->bind_param(1, " 4", SQL_INTEGER()));

# umlaut equivelant is vowel followed by 'e'
ok ($sth->bind_param(2, 'Andreas Koenig'));
ok ($sth->execute);

# Works undef -> NULL?
ok ($sth->bind_param(1, 5, SQL_INTEGER()));

ok ($sth->bind_param(2, undef));

ok ($sth->execute);

ok ($sth->bind_param(1, undef, SQL_INTEGER()));

ok ($sth->bind_param(2, undef));

ok ($sth->execute(-1, "abc"));

ok ($dbh->do("INSERT INTO $table VALUES (6, '?')"));

ok ($dbh->do('SET @old_sql_mode = @@sql_mode, @@sql_mode = \'\''));

ok ($dbh->do("INSERT INTO $table VALUES (7, \"?\")"));

ok ($dbh->do('SET @@sql_mode = @old_sql_mode'));

$sth = $dbh->prepare("SELECT * FROM $table ORDER BY id") or die "Unable to prepare " . $dbh->errstr;

ok($sth->execute);

ok ($sth->bind_columns(undef, \$id, \$name));

$ref = $sth->fetch ; 

cmp_ok $id,  '==', -1, 'id set to -1'; 

cmp_ok $name, 'eq', 'abc', 'name eq abc'; 

$ref = $sth->fetch;
cmp_ok $id, '==', 1, 'id set to 1';
cmp_ok $name, 'eq', 'Alligator Descartes', '$name set to Alligator Descartes';

$ref = $sth->fetch;
cmp_ok $id, '==', 2, 'id set to 2';
cmp_ok $name, 'eq', 'Tim Bunce', '$name set to Tim Bunce';

$ref = $sth->fetch;
cmp_ok $id, '==', 3, 'id set to 3';
cmp_ok $name, 'eq', 'Jochen Wiedmann', '$name set to Jochen Wiedmann';

$ref = $sth->fetch;
cmp_ok $id, '==', 4, 'id set to 4';
cmp_ok $name, 'eq', 'Andreas Koenig', '$name set to Andreas Koenig';

$ref = $sth->fetch;
cmp_ok $id, '==', 5, 'id set to 5';
ok !defined($name), 'name not defined';

$ref = $sth->fetch;
cmp_ok $id, '==', 6, 'id set to 6';
cmp_ok $name, 'eq', '?', "\$name set to '?'";

$ref = $sth->fetch;
cmp_ok $id, '==', 7, '$id set to 7';
cmp_ok $name, 'eq', '?', "\$name set to '?'";

ok ($dbh->do("DROP TABLE $table"));
