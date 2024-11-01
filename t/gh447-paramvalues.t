#! /bin/env perl

use strict;
use warnings;

#"set tabstop=4 softtabstop=4 shiftwidth=4 expandtab

use Data::Dumper;
use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';

my ($row, $sth, $dbh);
my ($def, $rows, $errstr, $ret_ref);
use vars qw($test_dsn $test_user $test_password);
my $table = 'dbd_mysql_gh447';

eval {$dbh = DBI->connect($test_dsn, $test_user, $test_password,
    { RaiseError => 1, AutoCommit => 1});};

if ($@) {
    plan skip_all => "no database connection";
}

# in case of exit early, ensure we clean up
END {
    if ($dbh) {
        $dbh->do("DROP TABLE IF EXISTS $table");
        $dbh->disconnect();
    }
}

# this is the starting index for the placeholder keys
# in the ParamValues attribute hashref.  gh#447 showed
# the keys begin counting with 0, but DBI requires they
# start counting at 1.
# so, if this value is 0, tests pass under DBD::mysql 4.050.
# but the value should be 1, when the issue is fixed.
my $ofs = 1;

# ------ set up
ok(defined $dbh, "Connected to database");
$dbh->do("DROP TABLE IF EXISTS $table");
$dbh->do("CREATE TABLE $table (id INT(4), name VARCHAR(64))");

# test prepare/execute statement without a placeholder

$sth = $dbh->prepare("SHOW TABLES LIKE '$table'");
is_deeply($sth->{ParamValues}, {}, "ParamValues is empty hashref before SHOW");
$sth->execute();

is_deeply($sth->{ParamValues}, {}, "ParamValues is still empty after execution");

$sth->finish;
is_deeply($sth->{ParamValues}, {}, "ParamValues empty after finish");
undef $sth;


# test prepare/execute statement with a placeholder
$sth = $dbh->prepare("INSERT INTO $table values (?, ?)");
is_deeply($sth->{ParamValues}, {0+$ofs => undef, 1+$ofs => undef},
    "ParamValues is correct hashref before INSERT")
    || print Dumper($sth->{ParamValues});

# insert rows with placeholder
my %rowdata;
my @chars = grep !/[0O1Iil]/, 0..9, 'A'..'Z', 'a'..'z';

for (my $i = 1 ; $i < 4; $i++) {
    my $word = join '', $i, '-', map { $chars[rand @chars] } 0 .. 16;
    $rowdata{$i} = $word;  # save for later
    $rows = $sth->execute($i, $word);
    is($rows, 1, "Should have inserted one row");
    is_deeply($sth->{ParamValues}, {0+$ofs => $i, 1+$ofs => $word},
        "row $i ParamValues hashref as expected");
}

$sth->finish;
is_deeply($sth->{ParamValues}, {0+$ofs => 3, 1+$ofs => $rowdata{3}},
    "ParamValues still hold last values after finish");
undef $sth;


# test prepare/execute with bind_param

$sth = $dbh->prepare("SELECT * FROM $table WHERE id = ? OR name = ?");
is_deeply($sth->{ParamValues}, {0+$ofs => undef, 1+$ofs => undef},
    "ParamValues is hashref with keys before bind_param");
$sth->bind_param(1, 1, DBI::SQL_INTEGER);
$sth->bind_param(2, $rowdata{1});
is_deeply($sth->{ParamValues}, {0+$ofs => 1, 1+$ofs => $rowdata{1}},
    "ParamValues contains bound values after bind_param");

$rows = $sth->execute;
is($rows, 1, 'execute selected 1 row');
is_deeply($sth->{ParamValues}, {0+$ofs => 1, 1+$ofs => $rowdata{1}},
    "ParamValues still contains values after execute");

# try changing one parameter only (so still param 1 => 1)
$sth->bind_param(2, $rowdata{2});
is_deeply($sth->{ParamValues}, {0+$ofs => 1, 1+$ofs => $rowdata{2}},
    "ParamValues updated with another bind_param");
$rows = $sth->execute;
is($rows, 2, 'execute selected 2 rows because changed param value');

# try execute with args (the previously bound values are overridden)
$rows = $sth->execute(3, $rowdata{3});
is($rows, 1, 'execute used exec args, overrode bound params');
is_deeply($sth->{ParamValues}, {0+$ofs => 3, 1+$ofs => $rowdata{3}},
    "ParamValues reflect execute args -- bound params overwritten");

$sth->bind_param(1, undef, DBI::SQL_INTEGER);
is_deeply($sth->{ParamValues}, {0+$ofs => undef, 1+$ofs => $rowdata{3}},
    "ParamValues includes undef param after binding");

$rows = $sth->execute(1, $rowdata{2});
is($rows, 2, 'execute used exec args, not bound values');
is_deeply($sth->{ParamValues}, {0+$ofs => 1, 1+$ofs => $rowdata{2}},
    "ParamValues changed by execution");

undef $sth;


# clean up
$dbh->do("DROP TABLE IF EXISTS $table");

# Install a handler so that a warning about unfreed resources gets caught
$SIG{__WARN__} = sub { die @_ };

$dbh->disconnect();

undef $dbh;

done_testing();

