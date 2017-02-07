use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
require "t/lib.pl";

my $INSECURE_VALUE_FROM_USER = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1, AutoCommit => 0 });

plan tests => 2;
my $sth = $dbh->prepare("select * from unknown_table where id=?");
eval { $sth->bind_param(1, $INSECURE_VALUE_FROM_USER, 3) };
like $@, qr/Binding non-numeric field 1, value '$INSECURE_VALUE_FROM_USER' as a numeric!/, "bind_param failed on incorrect numeric value";
pass "perl interpretor did not crashed";
