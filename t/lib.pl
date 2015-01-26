use strict;
use warnings;

use Test::More;
use DBI::Const::GetInfoType;
use vars qw($mdriver $dbdriver $childPid $test_dsn $test_user $test_password);

$| = 1; # flush stdout asap to keep in sync with stderr

#
#   Driver names; EDIT THIS!
#
$mdriver = 'mysql';
$dbdriver = $mdriver; # $dbdriver is usually just the same as $mdriver.
                      # The exception is DBD::pNET where we have to
                      # to separate between local driver (pNET) and
                      # the remote driver ($dbdriver)


#
#   DSN being used; do not edit this, edit "$dbdriver.dbtest" instead
#


$::COL_NULLABLE = 1;
$::COL_KEY = 2;


my $file;
if (-f ($file = "t/$dbdriver.dbtest")  ||
    -f ($file = "$dbdriver.dbtest")    ||
    -f ($file = "../tests/$dbdriver.dbtest")  ||
    -f ($file = "tests/$dbdriver.dbtest")) {
    eval { require $file; };
    if ($@) {
	print STDERR "Cannot execute $file: $@.\n";
	print "1..0\n";
	exit 0;
    }
    $::test_dsn      = $::test_dsn || $ENV{'DBI_DSN'} || 'DBI:mysql:database=test';
    $::test_user     = $::test_user|| $ENV{'DBI_USER'}  ||  '';
    $::test_password = $::test_password || $ENV{'DBI_PASS'}  ||  '';
}
if (-f ($file = "t/$mdriver.mtest")  ||
    -f ($file = "$mdriver.mtest")    ||
    -f ($file = "../tests/$mdriver.mtest")  ||
    -f ($file = "tests/$mdriver.mtest")) {
    eval { require $file; };
    if ($@) {
	print STDERR "Cannot execute $file: $@.\n";
	print "1..0\n";
	exit 0;
    }
}


#
#   Print a DBI error message
#
# TODO - This is on the chopping block
sub DbiError ($$) {
    my ($rc, $err) = @_;
    $rc ||= 0;
    $err ||= '';
    $::numTests ||= 0;
    print "Test $::numTests: DBI error $rc, $err\n";
}

sub connection_id {
    my $dbh = shift;
    return 0 unless $dbh;

    # Paul DuBois says the following is more reliable than
    # $dbh->{'mysql_thread_id'};
    my @row = $dbh->selectrow_array("SELECT CONNECTION_ID()");

    return $row[0];
}

# nice function I saw in DBD::Pg test code
sub byte_string {
    my $ret = join( "|" ,unpack( "C*" ,$_[0] ) );
    return $ret;
}

sub SQL_VARCHAR { 12 };
sub SQL_INTEGER { 4 };

=item CheckRoutinePerms()

Check if the current user of the DBH has permissions to create/drop procedures

    if (!CheckRoutinePerms($dbh)) {
        plan skip_all =>
            "Your test user does not have ALTER_ROUTINE privileges.";
    }

=cut

sub CheckRoutinePerms {
    my $dbh = shift @_;

    # check for necessary privs
    local $dbh->{PrintError} = 0;
    eval { $dbh->do('DROP PROCEDURE IF EXISTS testproc') };
    return if $@ =~ qr/alter routine command denied to user/;

    return 1;
};

=item MinimumVersion()

Check to see if the database where the test run against is
of a certain minimum version

    if (!MinimumVersion($dbh, '5.0')) {
        plan skip_all =>
            "You must have MySQL version 5.0 and greater for this test to run";
    }

=cut

sub MinimumVersion {
    my $dbh = shift @_;
    my $version = shift @_;

    my ($major, $minor) = split (/\./, $version);

    if ( $dbh->get_info($GetInfoType{SQL_DBMS_VER}) =~ /(^\d+)\.(\d+)\./ ) {

        # major version higher than requested
        return 1 if $1 > $major;

        # major version too low
        return if $1 < $major;

        # check minor version
        return 1 if $2 >= $minor;
    }

    return;
}

1;
