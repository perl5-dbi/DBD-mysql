use strict;
use warnings;

use Test::More;
use DBI;
use Encode;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

sub skip_rt_102404 {
    skip "(Perl 5.13.1 and DBI 1.635) or DBI 1.639 is required due to bug RT 102404", $_[0] unless ($] >= 5.013001 and eval { DBI->VERSION(1.635) }) or eval { DBI->VERSION(1.639) };
}

my $dbh;
eval {
    $dbh = DBI->connect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });
};
if ($@) {
    plan skip_all => "no database connection";
}
$dbh->disconnect();

plan tests => 10 * 3;

# All in internal Perl Unicode
my $jpnErr = qr/\x{4ed8}\x{8fd1}.*\x{884c}\x{76ee}/; # Use \x{...} instead \N{U+...} due to Perl 5.12.0 bug

foreach my $mysql_enable_utf8 (0, 1, 2) {
    my %utf8_params = ();
    if ($mysql_enable_utf8 == 1) {
        $utf8_params{'mysql_enable_utf8'} = 1;
        diag "Enabled mysql_enable_utf8.";
    # XXX There are no utf8mb4 error characters
    } elsif ($mysql_enable_utf8 == 2) {
        $utf8_params{'mysql_enable_utf8mb4'} = 1;
        diag "Enabled mysql_enable_utf8mb4.";
    } else {
        diag "Disabled mysql_enable_utf8.";
    }
    $dbh = DBI->connect($test_dsn, $test_user, $test_password,
                       { RaiseError => 1, PrintError => 1, AutoCommit => 1, %utf8_params });

    eval {
        $dbh->do("SET lc_messages = 'ja_JP'");
    } or do {
        $dbh->disconnect();
        plan skip_all => "Server lc_messages ja_JP are needed for this test";
    };

    my $sth;
    my $warn;
    my $dieerr;
    my $dbierr;
    my $failed;

    $failed = 0;
    $dieerr = undef;
    $dbierr = undef;
    $dbh->{HandleError} = sub { $dbierr = $_[0]; die $_[0]; };
    eval {
        $sth = $dbh->prepare("foo");
        $sth->execute();
        1;
    } or do {
        $dieerr = $@;
        $failed = 1;
    };
    $dbh->{HandleError} = undef;

    ok($failed, 'Execution of bad statement is failing (HandleError version).');
    like(Encode::decode('UTF-8', $dbierr), $jpnErr, 'DBI error is in octets (HandleError version).'); # XXX
    like(Encode::decode('UTF-8', $DBI::errstr), $jpnErr, 'DBI::errstr is in octets (HandleError version).'); # XXX 
    like(Encode::decode('UTF-8', $dbh->errstr), $jpnErr, 'DBI handler errstr() method is in octets (HandleError version).'); # XXX

    SKIP : {
        skip_rt_102404 1;
        like(Encode::decode('UTF-8', $dieerr), $jpnErr, 'Error from eval is in octets (HandleError version).');
    }

    $failed = 0;
    $warn = undef;
    $dieerr = undef;
    $dbh->{PrintError} = 1;
    $SIG{__WARN__} = sub { $warn = $_[0] };
    eval {
        $sth = $dbh->prepare("foo");
        $sth->execute();
        1;
    } or do {
        $dieerr = $@;
        $failed = 1;
    };
    $dbh->{PrintError} = 0;
    $SIG{__WARN__} = 'DEFAULT';

    ok($failed, 'Execution of bad statement is failing (PrintError version).');
    like(Encode::decode('UTF-8', $DBI::errstr), $jpnErr, 'DBI::errstr is in octets (PrintError version).'); # XXX
    like(Encode::decode('UTF-8', $dbh->errstr), $jpnErr, 'DBI handler errstr() method is in octets (PrintError version).'); # XXX

    SKIP : {
        skip_rt_102404 2;
        like(Encode::decode('UTF-8', $warn), $jpnErr, 'Warning is in octets (PrintError version).'); # XXX
        like(Encode::decode('UTF-8', $dieerr), $jpnErr, 'Error from eval is in octets (PrintError version).'); # XXX
    }

    $dbh->disconnect();
}
done_testing;
