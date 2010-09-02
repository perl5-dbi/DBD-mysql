#!perl -w
# vim: ft=perl
#
#   This checks for UTF-8 parameter support.
#

use strict;
use DBI;
use DBI::Const::GetInfoType;
use Carp qw(croak);
use Test::More;
use vars qw($table $test_dsn $test_user $test_password); 
use vars qw($COL_NULLABLE $COL_KEY);
use lib 't', '.';
require 'lib.pl';

my $nasty_bytes = chr(0xc3).chr(0xbf); # looks like character 0xff, if you accidentally utf8 decode
utf8::downgrade($nasty_bytes);
my $nasty_utf8 = $nasty_bytes;
utf8::upgrade($nasty_utf8);

is($nasty_bytes, $nasty_utf8, "Perl's internal form does not matter");

foreach my $enable_utf8 (0, 1) {
    my $enable_str = "mysql_enable_utf8=$enable_utf8";

    my $dbh = DBI->connect($test_dsn, $test_user, $test_password, { mysql_enable_utf8 => $enable_utf8 }) or die DBI->errstr;
    
    if ($dbh->get_info($GetInfoType{SQL_DBMS_VER}) lt "5.0") {
        plan skip_all => 
            "SKIP TEST: You must have MySQL version 5.0 and greater for this test to run";
    }


    foreach my $charset ("latin1", "utf8") {
        $dbh->do("DROP TABLE IF EXISTS utf8_test");
        $dbh->do(qq{
            CREATE TABLE utf8_test (
                payload VARCHAR(20),
                id int(10)
            ) CHARACTER SET $charset
        }) or die $dbh->errstr;


        $dbh->do("INSERT INTO utf8_test (id, payload) VALUES (1, ?), (2, ?)", {}, $nasty_bytes, $nasty_utf8);


        $dbh->do("INSERT INTO utf8_test (id, payload) VALUES (3, '$nasty_bytes')");
        $dbh->do("INSERT INTO utf8_test (id, payload) VALUES (4, '$nasty_utf8')");

        my $sth = $dbh->prepare("INSERT INTO utf8_test (id, payload) VALUES (?, ?)");
        $sth->execute(5, $nasty_bytes);
        $sth->execute(6, $nasty_utf8);

        $sth = $dbh->prepare("INSERT INTO utf8_test (id, payload) VALUES (?, ?)");
        $sth->bind_param(1, 7);
        $sth->bind_param(2, $nasty_bytes);
        $sth->execute;

        $sth = $dbh->prepare("INSERT INTO utf8_test (id, payload) VALUES (?, ?)");
        $sth->bind_param(1, 8);
        $sth->bind_param(2, $nasty_utf8);
        $sth->execute;

        my @trials = (
            'do with supplied params',
            'do with interpolated string',
            'prepare then execute',
            'prepare, bind, execute'
        );

        for (my $i = 0; $i<@trials; $i++) {
            my $bytes = $i*2+1;
            my $utf8s = $i*2+2;

            (my $out) = $dbh->selectrow_array("SELECT payload FROM utf8_test WHERE id = $bytes");
            is($out, chr(0xc3).chr(0xbf), "$trials[$i] / utf8 unset / $charset / $enable_str");

            ($out) = $dbh->selectrow_array("SELECT payload FROM utf8_test WHERE id = $utf8s");
            is($out, chr(0xc3).chr(0xbf), "$trials[$i] / utf8 set / $charset / $enable_str");
        }
    }
}

done_testing();
