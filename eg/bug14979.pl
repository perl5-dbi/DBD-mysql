#! /usr/bin/perl

use strict;
use warnings;

use DBI;

MAIN: {
	$ENV{'DBI_DSN'} ||= 'dbi:mysql:dbname=mysql:mysql_server_prepare=1';
	$ENV{'DBI_USER'} ||= 'root';
	$ENV{'DBI_PASS'} ||= '';
	my ($dbh) = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS}, {RaiseError => 1, PrintError => 0, AutoCommit => 0});
        $dbh->trace(3,"bug14979.trace");
	my ($sql) = qq[SELECT * FROM mysql.user WHERE user LIKE ?];
	my ($sth) = $dbh->prepare($sql);
	$sth->execute('foo');
	$sth->finish();
	my ($pid);
	if ($pid = fork()) {
		waitpid($pid, 0);
		unless ($? == 0) {
			die("Child failed to execute successfully\n");
		}
	} elsif (defined $pid) {
		$dbh->{'InactiveDestroy'} = 1;
		exit(0);
	} else {
		die("Failed to fork:$!\n");
	}
	$sth->execute('foo');
	$sth->finish();
	$dbh->disconnect();
}

