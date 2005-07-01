#!/usr/local/bin/perl
#
#   $Id$
#
#   This is the base test, tries to install the drivers. Should be
#   executed as the very first test.
#


#
#   Include lib.pl
#
$mdriver = "";
foreach $file ("lib.pl", "t/lib.pl") {
    do $file; if ($@) { print STDERR "Error while executing lib.pl: $@\n";
			   exit 10;
		      }
    if ($mdriver ne '') {
	last;
    }
}
if ($verbose) { print "Driver is $mdriver\n"; }

# Base DBD Driver Test

print "1..$tests\n";

require DBI;
print "ok 1\n";

import DBI;
print "ok 2\n";

$switch = DBI->internal;
(ref $switch eq 'DBI::dr') ? print "ok 3\n" : print "not ok 3\n";

# This is a special case. install_driver should not normally be used.
$drh = DBI->install_driver($mdriver);

(ref $drh eq 'DBI::dr') ? print "ok 4\n" : print "not ok 4\n";

if ($drh->{Version}) {
    print "ok 5\n";
    if ($verbose) {
	print "Driver version is ", $drh->{Version}, "\n";
    }
}

BEGIN { $tests = 5 }
exit 0;
# end.
