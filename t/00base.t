#!perl -w
#
#   $Id$
#
#   This is the base test, tries to install the drivers. Should be
#   executed as the very first test.
#

use Test::More tests => 5;

#
#   Include lib.pl
#
use vars qw($mdriver $table);
use lib 't', '.';
require 'lib.pl';

# Base DBD Driver Test
BEGIN {
    use_ok( 'DBI' );
}

$switch = DBI->internal;
cmp_ok ref $switch, 'eq', 'DBI::dr', 'Internal set';

# This is a special case. install_driver should not normally be used.
$drh= DBI->install_driver($mdriver);

ok $drh, 'Install driver';

cmp_ok ref $drh, 'eq', 'DBI::dr', 'DBI::dr set';

ok $drh->{Version}, "Version $drh->{Version}"; 
print "Driver version is ", $drh->{Version}, "\n";

