# -*- cperl -*-


use strict;
BEGIN { $^W = 1; }
use Config ();
use Getopt::Long();
use ExtUtils::MakeMaker();
use Data::Dumper ();

my $TESTDB = "test";

use vars qw($opt);
$opt =
  { "help" => \&Usage,
  };
Getopt::Long::GetOptions($opt, "help", "testdb=s", "testhost=s",
			 "testport=s",
			 "testuser=s", "testpassword=s",
			 "cflags=s", "libs=s", "verbose",
			 "nocatchstderr",
			 "ssl!");
my $source = {};

foreach my $key (qw/testdb testhost testuser testpassword cflags libs
                    nocatchstderr ssl/) {
  Configure($opt, $source, $key);
}

print <<"MSG";
I will use the following settings for compiling and testing:

MSG

delete $opt->{'help'};
my $keylen = 0;
foreach my $key (keys %$opt) {
  $keylen = length($key) if length($key) > $keylen;
}
my $slen = 0;
foreach my $val (values %$source) {
  $slen = length($val) if length($val) > $slen;
}
foreach my $key (sort { $a cmp $b} keys %$opt) {
  printf("  %-" . $keylen . "s (%-" . $slen . "s) = %s\n",
	 $key, $source->{$key}, $opt->{$key})
}

print <<"MSG";

To change these settings, see 'perl Makefile.PL --help' and
'perldoc INSTALL'.

MSG

sleep 5;

eval { require File::Spec };
my $fileName = $@ ?
  "t/mysql.mtest" : File::Spec->catfile("t", "mysql.mtest");
die "Failed to determine location of $fileName" unless -f $fileName;
(open(FILE, ">$fileName") &&
 (print FILE ("{ local " . Data::Dumper->Dump([$opt], ["opt"]) .
	      "\$::test_host = \$opt->{'testhost'};\n" .
	      "\$::test_port = \$opt->{'testport'};\n" .
	      "\$::test_user = \$opt->{'testuser'};\n" .
	      "\$::test_password = \$opt->{'testpassword'};\n" .
	      "\$::test_db = \$opt->{'testdb'};\n" .
	      "\$::test_dsn = \"DBI:mysql:\$::test_db\";\n" .
	      "\$::test_dsn .= \":\$::test_host\" if \$::test_host;\n" .
	      "\$::test_dsn .= \":\$::test_port\" if \$::test_port;\n" .
	      "} 1;\n"))  &&
  close(FILE))  ||  die "Failed to create $fileName: $!";


my $cflags = "-I\$(DBI_INSTARCH_DIR) $opt->{'cflags'}";
$cflags .= " -DDBD_MYSQL_WITH_SSL" if $opt->{'ssl'};
my %o = ( 'NAME' => 'DBD::mysql',
	  'INC' => $cflags,
	  'dist'         => { 'SUFFIX'       => ".gz",
			      'DIST_DEFAULT' => 'all installhtml tardist',
			      'COMPRESS'     => "gzip -9f" },
	  'clean' => { 'FILES' => '*.xsi' },
	  'OBJECT' => '$(O_FILES)',
	  'LIBS' => $opt->{'libs'},
	  'VERSION_FROM' => 'lib/DBD/mysql.pm'
	);
if ($ExtUtils::MakeMaker::VERSION >= 5.43) {
  $o{'CAPI'} = 'TRUE'
    if ($ExtUtils::MakeMaker::VERSION >= 5.43
	&&  $Config::Config{'archname'} =~ /-object\b/i);
  $o{'AUTHOR'} = 'Jochen Wiedmann <joe@ispsoft.de>';
  $o{'ABSTRACT'} =
    'A MySQL driver for the Perl5 Database Interface (DBI)';
  $o{'PREREQ_PM'} = { 'DBI' => 1.08,
		      'Data::Dumper' => 0 };
}
ExtUtils::MakeMaker::WriteMakefile(%o);
exit 0;


############################################################################
#
#   Name:    Usage
#
#   Purpose: Print Usage message and exit with error status.
#
############################################################################

sub Usage {
  print STDERR <<"USAGE";
Usage: perl $0 [options]

Possible options are:

  --cflags=<flags>       Use <flags> for running the C compiler; defaults
                         to the value of "mysql_config --cflags"
  --libs=<libs>          Use <libs> for running the linker; defaults
                         to the value of "mysql_config --libs"
  --testdb=<db>          Use the database <db> for running the test suite;
                         defaults to $TESTDB
  --testuser=<user>      Use the username <user> for running the test suite;
                         defaults to no username
  --testpassword=<pwd>   Use the password <pwd> for running the test suite;
                         defaults to no password
  --testhost=<host>      Use <host> as a database server for running the
                         test suite; defaults to localhost.
  --testport=<port>      Use <port> as the port number of the database;
                         by default the port number is choosen from the
                         mysqlclient library
  --nocatchstderr        Supress using the "myld" script that redirects
                         STDERR while running the linker.
  --help                 Print this message and exit

All options may be configured on the command line. If they are
not present on the command line, then mysql_config is called:

  mysql_config --cflags
  mysql_config --libs
  mysql_config --testdb

and so on. See the INSTALL.html file for details.
USAGE
  exit 1;
}


############################################################################
#
#   Name:    Configure
#
#   Purpose: Automatic configuration
#
#   Inputs:  $param - Name of the parameter being configured
#
#   Returns: Generated value, never undef
#
############################################################################

sub Configure {
  my($opt, $source, $param) = @_;

  if (exists($opt->{$param})) {
    $source->{$param} = "Users choice";
    return;
  }

  # First try mysql_config
  open(PIPE, "mysql_config --$param |");
  my $str = "";
  while (defined(my $line = <PIPE>)) {
    $str .= $line;
  }
  if ($str ne ""  &&  $str !~ /Options:/) {
    $str =~ s/\s+$//s;
    $str =~ s/^\s+//s;

    # Unfortunately ExtUtils::MakeMaker doesn't deal very well
    # with -L'...'
    $str =~ s/\-L\'(.*?)\'/-L$1/sg;
    $str =~ s/\-L\"(.*?)\"/-L$1/sg;

    $opt->{$param} = $str;
    $source->{$param} = "mysql_config";
    return;
  }

  # Ok, mysql_config doesn't work. We need to do our best
  if ($param eq "nocatchstderr") {
    $source->{$param} = "default";
    $opt->{$param} = 0;
  } elsif ($param eq "testdb") {
    $source->{$param} = "default";
    $opt->{$param} = $TESTDB;
  } elsif ($param eq "testhost"  ||  $param eq "testuser"  ||
	   $param eq "testport"  ||  $param eq "testpassword") {
    $source->{$param} = "default";
    $opt->{$param} = "";
  } elsif ($param eq "cflags") {
    $source->{$param} = "guessed";
    my $dir = SearchFor('include', 'mysql.h');
    if ($dir) {
      $opt->{$param} = "-I$dir";
      return;
    }
    die <<"MSG";
Failed to determine directory of mysql.h. Use

  perl Makefile.PL --cflags=-I<dir>

to set this directory. For details see the INSTALL.html file,
section "C Compiler flags" or type

  perl Makefile.PL --help
MSG
  } elsif ($param eq "libs") {
    $source->{$param} = "guessed";
    my @files = ($^O =~ /mswin32/i) ? qw(mysqlclient.lib) :
			qw(libmysqlclient.a libmysqlclient.so);
    my $dir = SearchFor('lib', @files);
    if ($dir) {
      $opt->{$param} = "-L$dir -lmysqlclient -lz -lgz";
      return;
    }
    my $f = join("|", @files);
    die <<"MSG";
Failed to determine directory of $f. Use

  perl Makefile.PL "--libs=-L<dir> -lmysqlclient"

to set this directory. For details see the INSTALL.html file,
section "Linker flags" or type

  perl Makefile.PL --help
MSG
  } elsif ($param eq "ssl") {
    $opt->{$param} = ($opt->{"libs"} =~ /ssl/) ? 1 : 0;
    $source->{$param} = "guessed";
  } else {
    die "Unknown configuration parameter: $param";
  }
}

my $haveFileSpec;
my $fineDir;
sub SearchFor {
  my($subdir, @files) = @_;
  $haveFileSpec = eval { require File::Spec } unless defined($haveFileSpec);

  my @dirs = ($^O =~ /mswin32/i) ? qw(C:) : qw(/usr/local /usr /opt);
  unshift(@dirs, $fineDir) if defined($fineDir);

  foreach my $f (@files) {
    foreach my $dir (@dirs) {
      my $try1 = $haveFileSpec ?
	File::Spec->catdir($dir, $subdir) : "$dir/$subdir";
      my $try2 = $haveFileSpec ?
	File::Spec->catdir($dir, "mysql") : "$dir/mysql";
      my $try3 = $haveFileSpec ?
	File::Spec->catdir($try1, "mysql") : "$try1/mysql";
      my $try4 = $haveFileSpec ?
	File::Spec->catdir($try2, $subdir) : "$try2/$subdir";
      foreach my $path ($try3, $try4, $try2, $try1, $dir) {
	my $file = $haveFileSpec ?
	  File::Spec->catfile($path, $f) : "$path/$f";
	if (-f $file) {
	  $fineDir = $dir;
	  return $path;
	}
      }
    }
  }
}


package MY;

sub libscan {
    my($self, $path) = @_;
    if ($path =~ /~$/) { return undef; }
    $path;
}

sub postamble {
  require DBI::DBD;
  "\n" . DBI::DBD::dbd_postamble(@_) . <<"POSTAMBLE";

installhtml: lib/DBD/mysql/INSTALL.pod
\tpod2html --infile=lib/DBD/mysql/INSTALL.pod --outfile=INSTALL.html

POSTAMBLE
};

sub dynamic_lib {
  my $self = shift;
  my $result = $self->SUPER::dynamic_lib(@_);
  if (!$::opt->{nocatchstderr}  &&  $result =~ /\$\(LD\)/) {
    $result =~ s/(\$\(LD\))/\$\(PERL\) myld \$(LD)/sg;
  }
  return $result;
}
