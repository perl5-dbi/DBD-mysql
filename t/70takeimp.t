#!/usr/bin/perl
#
#   $Id$
#
#   This is a skeleton test. For writing new tests, take this file
#   and modify/extend it.
#

use strict;
use vars qw($test_dsn $test_user $test_password $mdriver $dbdriver);
use DBI ();

#
#   Include lib.pl
#
$mdriver = "";
my $file;

# this test is only for DBI 1.49 and greater
#if ($DBI::VERSION < 1.49)
my $test_broken= 1;
if ($test_broken)
{
  #print "1..0 # Skip DBI Version is < 1.49 \n";
  print "1..0 # Skip test feature not implemented\n";
  exit(0);
}

foreach $file ("lib.pl", "t/lib.pl") {
    do $file; if ($@) { print STDERR "Error while executing lib.pl: $@\n";
			   exit 10;
		      }
    if ($mdriver ne '') {
	last;
    }
}

my $drh = DBI->install_driver($mdriver);

sub ServerError() {
    print STDERR ("Cannot connect: ", $DBI::errstr, "\n",
	"\tEither your server is not up and running or you have no\n",
	"\tpermissions for acessing the DSN $test_dsn.\n",
	"\tThis test requires a running server and write permissions.\n",
	"\tPlease make sure your server is running and you have\n",
	"\tpermissions, then retry.\n");
    exit 10;
}


#
#   Main loop; leave this untouched, put tests into the loop
#
use vars qw($state);
while (Testing()) {
    #
    #   Connect to the database
    my $dbh;
    Test($state or $dbh = DBI->connect($test_dsn, $test_user, $test_password))
	or ServerError();

    my $id = connection_id($dbh);
    print "Initial connection: $id\n";

    my $drh = $dbh->{Driver};

    my $imp_data;
    Test($state or $imp_data = $dbh->take_imp_data)
        or ErrMsg("didn't get imp_data");

    Test($state or length($imp_data) >= 80)
        or ErrMsg('test that our imp_data is greater than or equal to 80, this is reasonable');

    Test($state or $drh->{Kids} == 0)
        or ErrMsg('our Driver should have 0 Kid(s) after calling take_imp_data');

    {
        my $warn;
        local $SIG{__WARN__} = sub { ++$warn if $_[0] =~ /after take_imp_data/ };

        my $drh = $dbh->{Driver};
        Test($state or !defined $drh)
            or ErrMsg('... our Driver should be undefined');

        my $trace_level = $dbh->{TraceLevel};
        Test($state or !defined $trace_level)
            or ErrMsg('our TraceLevel should be undefined');

        Test($state or !defined $dbh->disconnect)
            or ErrMsg('disconnect should return undef');

        Test($state or !defined $dbh->quote(42))
            or ErrMsg('quote should return undefined');

        Test($state or $warn == 4)
            or ErrMsg('we should have gotten 4 warnings');
    }

    # XXX: how can we test that the connection wasn't actually dropped?

    #use Data::Dumper;
    #print "GOT $imp_data\n";
    warn "re-CONNECT\n";

    my $dbh2 = DBI->connect($test_dsn, $test_user, $test_password, { dbi_imp_data => $imp_data });
    #my $dbh2 = DBI->connect($test_dsn, $test_user, $test_password);

    # XXX: how can we test that the same connection is used?
    my $id2 = connection_id($dbh2);
    print "Overridden connection: $id2\n";

    Test($state or $id == $id2)
      or ErrMsg("the same connection: $id => $id2\n");

    my $drh2;
    Test($state or $drh2 = $dbh2->{Driver})
      or ErrMsg("can't get the driver\n");

    Test($state or $dbh2->isa("DBI::db"))
         or ErrMsg('isa test');
    # need a way to test dbi_imp_data has been used

    Test($state or $drh2->{Kids} == 1)
      or ErrMsg("our Driver should have 1 Kid(s) again: having " .  $drh2->{Kids} . "\n");

    Test($state or $drh2->{ActiveKids} == 1)
      or ErrMsg("our Driver should have 1 ActiveKid again: having " .  $drh2->{ActiveKids} . "\n");

    read_write_test($dbh2);

    # must cut the connection data again
    Test($state or $imp_data = $dbh2->take_imp_data)
        or ErrMsg("didn't get imp_data");

    #
    #   Finally disconnect.
    #
    #Test($state or $dbh2->disconnect())
    #or DbiError($dbh2->err, $dbh2->errstr);

}

sub read_write_test {
    my $dbh = shift;

    # now the actual test:

    #   Find a possible new table name
    #
    my $table= 't1';
    Test($state or $dbh->do("DROP TABLE IF EXISTS $table"))
	or DbiError($dbh->err, $dbh->errstr);

    #
    #   Create a new table
    #
    my $def;
    if (!$state) {
	($def = TableDefinition($table,
				["id",   "INTEGER",  4, 0],
				["name", "CHAR",    64, 0]));
	print "Creating table:\n$def\n";
    }
    Test($state or $dbh->do($def))
	or DbiError($dbh->err, $dbh->errstr);

    #
    #   ... and drop it.
    #
    Test($state or $dbh->do("DROP TABLE $table"))
	   or DbiError($dbh->err, $dbh->errstr);

}

sub connection_id {
    my $dbh = shift;
    return 0 unless $dbh;

    # Paul DuBois says the following is more reliable than
    # $dbh->{'mysql_thread_id'};
    my @row = $dbh->selectrow_array("SELECT CONNECTION_ID()");

    return $row[0];
}
