#!perl -w
# vim: ft=perl
#
#   This checks for UTF-8 parameter support.
#

use strict;
use warnings FATAL => 'all';
use DBI;
use DBI::Const::GetInfoType;
use Carp qw(croak);
use Test::More;
use vars qw($table $test_dsn $test_user $test_password); 
use vars qw($COL_NULLABLE $COL_KEY);
use lib 't', '.';
require 'lib.pl';

my $dbh = eval { DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1 }) } or
    plan skip_all => "no database connection";

if ($dbh->{mysql_serverversion} < 50000) {
    plan skip_all => "You must have MySQL version 5.0 and greater for this test to run";
}

my $nasty_bytes = chr(0xc3).chr(0xbf); # looks like character 0xff, if you accidentally utf8 decode
utf8::downgrade($nasty_bytes);
my $nasty_utf8 = $nasty_bytes;
utf8::upgrade($nasty_utf8);

is($nasty_bytes, $nasty_utf8, "Perl's internal form does not matter");

foreach my $enable_utf8 (0, 1) { foreach my $server_prepare (0, 1) {
    my $enable_str = "mysql_enable_utf8=$enable_utf8 / mysql_server_prepare=$server_prepare";
    my $enable_hash = { mysql_enable_utf8 => $enable_utf8, mysql_server_prepare => $server_prepare, mysql_server_prepare_disable_fallback => 1 };

    $dbh = DBI->connect($test_dsn, $test_user, $test_password, $enable_hash) or die DBI->errstr;

    foreach my $name ("latin1", "utf8") {

        $dbh->do("SET NAMES $name") or die $dbh->errstr;

    foreach my $charset ("latin1", "utf8") {

        # This configuration cannot work because MySQL server expect Latin1 strings, but mysql_enable_utf8=1 cause automatic encoding to UTF-8
        next if $enable_utf8 and $name eq "latin1";

        $dbh->do("DROP TABLE IF EXISTS utf8_test");
        $dbh->do(qq{
            CREATE TABLE utf8_test (
                payload VARCHAR(20),
                id int(10)
            ) CHARACTER SET $charset
        }) or die $dbh->errstr;


        my $nasty_utf8_param = $nasty_utf8;
        utf8::encode($nasty_utf8_param) if $name eq "utf8" and not $enable_utf8; # Needs to manually UTF-8 encode when mysql_enable_utf8=0
        utf8::downgrade($nasty_utf8_param) if $name eq "latin1"; # Needs to convert Unicode string to Latin1 when MySQL server expect Latin1 strings


        $dbh->do("INSERT INTO utf8_test (id, payload) VALUES (1, ?), (2, ?)", {}, $nasty_bytes, $nasty_utf8_param);


        $dbh->do("INSERT INTO utf8_test (id, payload) VALUES (3, '$nasty_bytes')");
        $dbh->do("INSERT INTO utf8_test (id, payload) VALUES (4, '$nasty_utf8_param')");

        my $sth = $dbh->prepare("INSERT INTO utf8_test (id, payload) VALUES (?, ?)");
        $sth->execute(5, $nasty_bytes);
        $sth->execute(6, $nasty_utf8_param);

        $sth = $dbh->prepare("INSERT INTO utf8_test (id, payload) VALUES (?, ?)");
        $sth->bind_param(1, 7);
        $sth->bind_param(2, $nasty_bytes);
        $sth->execute;

        $sth = $dbh->prepare("INSERT INTO utf8_test (id, payload) VALUES (?, ?)");
        $sth->bind_param(1, 8);
        $sth->bind_param(2, $nasty_utf8_param);
        $sth->execute;

        {
            my $sql = "INSERT INTO utf8_test (id, payload) VALUES (?, ?)";
            $sth = $dbh->prepare($sql);
        }
        $sth->execute(9, $nasty_bytes);
        $sth->execute(10, $nasty_utf8_param);

        {
            my $sql = "INSERT INTO utf8_test (id, payload) VALUES (?, ?)";
            $sth = $dbh->prepare($sql);
        }
        {
            my $param = 1;
            my $val = 11;
            $sth->bind_param($param, $val);
        }
        {
            my $param = 2;
            my $val = $nasty_bytes;
            $sth->bind_param($param, $val);
        }
        $sth->execute;

        {
            my $sql = "INSERT INTO utf8_test (id, payload) VALUES (?, ?)";
            $sth = $dbh->prepare($sql);
        }
        {
            my $param = 1;
            my $val = 12;
            $sth->bind_param($param, $val);
        }
        {
            my $param = 2;
            my $val = $nasty_utf8_param;
            $sth->bind_param($param, $val);
        }
        $sth->execute;

        my @trials = (
            'do with supplied params',
            'do with interpolated string',
            'prepare then execute',
            'prepare, bind, execute',
            'prepare (free param) then execute',
            'prepare (free param), bind (free param), execute',
        );

        for (my $i = 0; $i<@trials; $i++) {
            my $bytes = $i*2+1;
            my $utf8s = $i*2+2;

            (my $out) = $dbh->selectrow_array("SELECT payload FROM utf8_test WHERE id = $bytes");
            is($out, chr(0xc3).chr(0xbf), "$trials[$i] / utf8 unset / $charset / $enable_str");

            ($out) = $dbh->selectrow_array("SELECT payload FROM utf8_test WHERE id = $utf8s");
            utf8::decode($out) if $name eq "utf8" and not $enable_utf8; # Needs to manually UTF-8 decode when mysql_enable_utf8=0
            is($out, chr(0xc3).chr(0xbf), "$trials[$i] / utf8 set / $charset / $enable_str");
        }

        $dbh->do("DROP TABLE IF EXISTS utf8_test");
    }
    }
} }

done_testing();
