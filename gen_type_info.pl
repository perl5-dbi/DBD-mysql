#!/usr/local/bin/perl

use DBI;
use Data::Dumper;
require DBI::Const::GetInfoType;
use strict;
my $driver = "Mysql";
my $drvr = DBI->connect("dbi:mysql:rlippan","root","", {RaiseError=>1});
my $odbc = DBI->connect("dbi:ODBC:Mysql","root","", {RaiseError=>1});

#$odbc->{LongReadLen} = 100000000;	# Does not work ?!?
#$odbc->{LongTruncOk} = 1;		# Does work ?!?

my $odbc_data = $odbc->type_info_all();
my $drvr_data = $drvr->type_info_all();


my %o_cols = %{$odbc_data->[0]};
my %d_cols = %{$drvr_data->[0]};


my %o_types;
for my $row (@$odbc_data[1..$#$odbc_data]) { 
    push @{$o_types{$row->[0]}}, $row;
}


my %d_types;
for my $row (@$drvr_data[1..$#$drvr_data]) { 
    push @{+$d_types{$row->[0]}}, $row;
}

my @col_keys= keys %{{%o_cols, %d_cols}};

# Check for keys that are in one driver but not the other;
print "\n"x2;
my %skip_fields;
for (@col_keys) {
    unless ( exists $odbc_data->[0]{$_}) {
        print "Field $_ is not in ODBC \n";
	++$skip_fields{$_};
    }
    unless (exists $drvr_data->[0]{$_}) {
        print "Field $_ is not in DBD::$driver\n";
        ++$skip_fields{$_};
    }
}

print "\n"x2;
#print "Skip fields:", Data::Dumper::Dumper(\%skip_fields);


# Check for data types  that are in one driver and not the other,
# and check to make sure all values match (for those that exist in both drivers
for  (keys %{ {%o_types, %d_types} }) {
    if (!exists $o_types{$_}) { print "Type '$_' not in ODBC \n"; next};
    if (!exists $d_types{$_}) { print "Type '$_' not in $driver\n"; next};

    my $row_num = 0;
    print $_,":\n";
    for my $row (@{$o_types{$_}}) {
	print "[$row_num]\n";
        for my $col (@col_keys) {
	    next if exists $skip_fields{$col};
	    # print $row->[$o_cols{$col}];
            my $mysql = ($d_types{$_}[$row_num][$d_cols{$col}]);

            $row->[$o_cols{$col}] ne $mysql and print 
	        "\t For $col ODBC gives:", $row->[$o_cols{$col}],
	        " and $driver gives:$mysql\n";
        }
	$row_num++;
    }
}


