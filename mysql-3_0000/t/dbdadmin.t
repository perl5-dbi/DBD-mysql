#!/usr/local/bin/perl
#
#   Test suite for the admin functions of DBD::mSQL and DBD::mysql.
#


#
#   Make -w happy
#
$test_dsn = $test_host = $test_user = $test_password = $verbose = '';
$| = 1;


#
#   Include lib.pl
#
$DBI::errstr = ''; # Make -w happy
require DBI;
$mdriver = "";
foreach $file ("lib.pl", "t/lib.pl", "DBD-mysql/t/lib.pl") {
    do $file; if ($@) { print STDERR "Error while executing lib.pl: $@\n";
			   exit 10;
		      }
    if ($mdriver ne '') {
	last;
    }
}


sub ServerError() {
    print STDERR ("Cannot connect: ", $DBI::errstr, "\n",
	"\tEither your server is not up and running or you have no\n",
	"\tpermissions for acessing the DSN $test_dsn.\n",
	"\tThis test requires a running server and write permissions.\n",
	"\tPlease make sure your server is running and you have\n",
	"\tpermissions, then retry.\n");
    exit 10;
}


sub InDsnList($@) {
    my($dsn, @dsnList) = @_;
    my($d);
    foreach $d (@dsnList) {
	if ($d =~ /^dbi:[^:]+:$dsn\b/i) {
	    return 1;
	}
    }
    0;
}

sub PathFor(@) {
    eval { require File::Spec; };
    my $haveFileSpec = $@ ? 0 : 1;
    foreach my $f (@_) {
	foreach my $dir ($haveFileSpec ?
		       File::Spec->path() :
			 split(/\:/, $ENV{PATH})) {
	    my $p = $haveFileSpec ?
	      File::Spec->catfile($dir, $f) : "$dir/$f";
	    return $p if -x $p;
	}
    }
    return undef;
}

#
#   Main loop; leave this untouched, put tests after creating
#   the new table.
#
while (Testing()) {
    # Check if the server is awake.
    $dbh = undef;
    Test($state or ($dbh = DBI->connect($test_dsn, $test_user,
					$test_password)))
	or ServerError();

    Test($state or (@dsn = DBI->data_sources(
         $mdriver, {host => $test_host, port => $test_port, user=> $test_user, password=>$test_password})) >= 0);
    Test($state or (@dsn = DBI->data_sources($mdriver)) >= 0);
    if (!$state  &&  $verbose) {
	my $d;
	print "List of $mdriver data sources:\n";
	foreach $d (@dsn) {
	    print "    $d\n";
	}
	print "List ends.\n";
    }

    my $drh;
    Test($state or ($drh = DBI->install_driver($mdriver)))
	or print STDERR ("Cannot obtain drh: " . $DBI::errstr);

    #
    #   Check the ping method.
    #
    Test($state or $dbh->ping())
	or ErrMsgF("Ping failed: %s.\n", $dbh->errstr);


    if ($mdriver eq 'mSQL'  or $mdriver eq 'mysql') {
	my($testdsn) = "testaa";
	my($testdsn1, $testdsn2);
	my($accessDenied) = 0;
	my($warning);
	my($warningSub) = sub { $warning = shift };

	if (!$state) {
	    while (InDsnList($testdsn, @dsn)) {
		++$testdsn;
	    }
	    $testdsn1 = $testdsn;
	    ++$testdsn1;
	    while (InDsnList($testdsn1, @dsn)) {
		++$testdsn1;
	    }
	    $testdsn2 = $testdsn1;
	    ++$testdsn2;
	    while (InDsnList($testdsn2, @dsn)) {
		++$testdsn2;
	    }
 
	    if (!($result = $drh->func('createdb', $testdsn, $test_host,
				$test_user, $test_password, 'admin'))
		and  ($drh->errstr =~ /(access|permission) denied/i)) {
		$accessDenied = 1;
		$result = 1;
	    }
	}

	Test($state or $result)
	    or print STDERR ("Error while executing createdb: "
			     . $drh->errstr);

	Test($state or $accessDenied
	     or InDsnList($testdsn, DBI->data_sources($mdriver))
	     or InDsnList($testdsn, DBI->data_sources($mdriver,
						      {"host" => $test_host,
						       "port" => $test_port,
                                                       "user" => $test_user,
                                                       "password" => $test_password})))
	    or print STDERR ("New DB not in DSN list\n");

	Test($state or $accessDenied
	     or $drh->func('dropdb', $testdsn, $test_host,
			   $test_user, $test_password, 'admin'))
	    or print STDERR ("Error while executing _DropDB: "
			     . $drh->errstr);

	Test($state or $accessDenied
	     or (!InDsnList($testdsn, DBI->data_sources($mdriver)) and
		 !InDsnList($testdsn,
			    DBI->data_sources($mdriver,
					      {"host" => $test_host,
					       "port" => $test_port,
                                               "user" => $test_user,
                                               "password" => $test_password}))))
	    or print STDERR ("New DB not removed from DSN list\n");

	my($mayShutdown) = $ENV{'DB_SHUTDOWN_ALLOWED'};

	Test($state or $accessDenied
	     or $drh->func('createdb', $testdsn1, $test_host,
			   $test_user, $test_password, 'admin'))
	    or printf STDERR ("\$drh->admin('createdb') failed: %s\n",
			      $drh->errstr);
	Test($state or $accessDenied
	     or InDsnList($testdsn1, DBI->data_sources($mdriver))
	     or InDsnList($testdsn1,
			  DBI->data_sources($mdriver,
					    {"host" => $test_host,
					     "port" => $test_port,
                                             "user" => $test_user,
                                             "password" => $test_password})))
	    or printf STDERR ("DSN $testdsn1 not in DSN list.\n");
	Test($state or $accessDenied
	     or $drh->func('dropdb', $testdsn1, $test_host,
			   $test_user, $test_password, 'admin'))
	    or printf STDERR ("\$drh->admin('dropdb') failed: %s\n",
			      $drh->errstr);
	Test($state or $accessDenied
	     or (!InDsnList($testdsn1, DBI->data_sources($mdriver)) and
		 !InDsnList($testdsn1,
			    DBI->data_sources($mdriver,
					      {"host" => $test_host,
					       "port" => $test_port,
                                               "user" => $test_user,
                                               "password" => $test_password}))))
	    or printf STDERR ("DSN $testdsn1 not removed from DSN list.\n");
	Test($state or $accessDenied
	     or $drh->func('createdb', $testdsn2, $test_host,
			   $test_user, $test_password, 'admin'))
	    or printf STDERR ("\$drh->admin('createdb') failed: %s\n",
			      $drh->errstr);
	Test($state or $accessDenied
	     or InDsnList($testdsn2, DBI->data_sources($mdriver))
	     or InDsnList($testdsn2,
			  DBI->data_sources($mdriver,
					    {"host" => $test_host,
					     "port" => $test_port,
                                             "user" => $test_user,
                                             "password" => $test_password})))
	    or printf STDERR ("DSN $testdsn2 not in DSN list.\n");
	Test($state or $accessDenied
	     or $drh->func('dropdb', $testdsn2, $test_host,
			   $test_user, $test_password, 'admin'))
	    or printf STDERR ("\$drh->admin('dropdb') failed: %s\n",
			      $drh->errstr);
	Test($state or $accessDenied
	     or (!InDsnList($testdsn2, DBI->data_sources($mdriver)) and
		 !InDsnList($testdsn2,
			    DBI->data_sources($mdriver,
					      {"host" => $test_host,
					       "port" => $test_port,
                                               "user" => $test_user,
                                               "password" => $test_password}))))
	    or printf STDERR ("DSN $testdsn2 not removed from DSN list.\n");

	if ($mdriver eq 'mysql') {
	    #
	    #   Try to do a shutdown.
	    #
	    Test($state  or  !$mayShutdown  or  $accessDenied
		 or  $dbh->func("shutdown", "admin"))
		or ErrMsgF("Cannot shutdown database: %s.\n", $dbh->errstr);
	    if (!$state) {
		sleep 10;
	    }

	    #
	    #   Pinging should fail now.
	    #
	    Test($state or !$mayShutdown or $accessDenied or !$dbh->ping())
		or print STDERR ("Shutdown failed (ping succeeded)");

	    #
	    #   Restart the database
	    #
	    if (!$state  &&  $mayShutdown  &&  !$accessDenied) {
		if (fork() == 0) {
		    close STDIN;
		    close STDOUT;
		    close STDERR;
		    exec((PathFor("mysqld_safe") || "safe_mysqld") . " &");
		}
	    }
	    sleep 5;

	    #
	    #   Try DBD::mysql's automatic reconnect
	    #
	    Test($state or $dbh->ping())
		or ErrMsgF("Reconnect failed: %s.\n", $dbh->errstr);
	}

	Test($state or $dbh->disconnect);
    }
}
