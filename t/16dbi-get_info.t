use strict;
use warnings;

use Test::More;
use DBI;
use DBI::Const::GetInfoType;
$|= 1;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, AutoCommit => 1});

# DBI documentation states:
# Because some DBI methods make use of get_info(), drivers are strongly
# encouraged to support at least the following very minimal set of
# information types to ensure the DBI itself works properly
# so let's test them here

# DBMS_NAME and DBMS_VERSION are not static, all we can check is they are
# there and they have some sane length
my $dbms_name = $dbh->get_info( $GetInfoType{SQL_DBMS_NAME});
cmp_ok(length($dbms_name), '>', 4, 'SQL_DBMS_NAME');

my $dbms_ver = $dbh->get_info( $GetInfoType{SQL_DBMS_VER});
cmp_ok(length($dbms_ver), '>', 4, 'SQL_DBMS_VER');

# these variables are always the same for MySQL
my %info = (
    SQL_IDENTIFIER_QUOTE_CHAR  => '`',
    SQL_CATALOG_NAME_SEPARATOR => '.',
    SQL_CATALOG_LOCATION       => 1,
);

for my $option ( keys %info ) {
    my $value = $dbh->get_info( $GetInfoType{$option});
    is($value, $info{$option}, $option);
}

$dbh->disconnect();

done_testing;
