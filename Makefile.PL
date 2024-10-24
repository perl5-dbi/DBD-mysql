# -*- cperl -*-
# vim: ts=4

use strict;
use warnings;
use utf8;

use 5.008_001;
use Config;
use Getopt::Long;
use ExtUtils::MakeMaker;
use Data::Dumper;
use Devel::CheckLib;
use File::Path;
use File::Copy;
use File::Basename;
use File::Spec;
require DBI::DBD;

my $TESTDB = "test";

our $opt = { "help" => \&Usage, };

{
local ($::test_host, $::test_port, $::test_user, $::test_socket, $::test_password, $::test_db, $::test_mysql_config);
eval { require "./t/mysql.mtest"; 1; } || eval { require "../t/mysql.mtest"; 1; } and do {
$opt->{'testhost'} = $::test_host;
$opt->{'testport'} = $::test_port;
$opt->{'testuser'} = $::test_user;
$opt->{'testsocket'} = $::test_socket;
$opt->{'testpassword'} = $::test_password;
$opt->{'testdb'} = $::test_db;
$opt->{'mysql_config'} = $::test_mysql_config;
}
}

Getopt::Long::GetOptions(
    $opt,
    "help",
    "testdb=s",
    "testhost=s",
    "testport=s",
    "testuser=s",
    "testpassword=s",
    "testsocket=s",
    "cflags=s",
    "libs=s",
    "verbose",
    "ps-protocol",
    "bind-type-guessing",
    "nocatchstderr",
    "nofoundrows!",
    "mysql_config=s",
    ) || die Usage();

my $source = {};

  #Check for mysql_config first
  $source->{'mysql_config'} = "guessed";

  if ($opt->{'mysql_config'}) {
    $source->{'mysql_config'} = 'Users choice';
  }
  if (!$opt->{'mysql_config'} && $ENV{DBD_MYSQL_CONFIG}) {
     $opt->{'mysql_config'} = $ENV{DBD_MYSQL_CONFIG};
     $source->{'mysql_config'} = 'environment';
  }
  if ($opt->{'mysql_config'})
  {
    $opt->{'mysql_config'} = Win32::GetShortPathName($opt->{'mysql_config'})
        if $^O eq 'MSWin32';
    if (! defined `$opt->{'mysql_config'}`)
    {
      print <<"MSG";

Specified mysql configuration script '$opt->{'mysql_config'}' doesn't exist.
Please check path/permissions. Will try to use default mysql_config
script found through PATH.
MSG
      $opt->{'mysql_config'}= "mysql_config";
    }
  }
  else
  {
    if (! defined `mysql_config`)
    {
      print <<MSG;

Cannot find the file 'mysql_config'! Your execution PATH doesn't seem
not contain the path to mysql_config. Resorting to guessed values!
MSG
    }
    $opt->{'mysql_config'} = "mysql_config";
  }

for my $key (qw/testdb testhost testuser testpassword testsocket testport
                    cflags libs nocatchstderr nofoundrows
                    ps-protocol bind-type-guessing version/)
{
  Configure($opt, $source, $key);
}

#if we have a testport but no host, assume localhost
#but use 127.0.0.1 instead of 'localhost' to use TCP/IP
if ( $opt->{testport} && !$opt->{testhost} ) {
  $opt->{testhost} = '127.0.0.1';
  $source->{testhost} = 'guessed';
}

#We have to rename/move Makefile.PL in mysqlEmb directory
#since MakeMaker will find it and will try to execute it.
if (-f "mysqlEmb/Makefile.PL")
{
  move ("mysqlEmb/Makefile.PL", "mysqlEmb/Makefile.PL.old");
}

print <<"MSG";
I will use the following settings for compiling and testing:

MSG

delete $opt->{'help'};
my $keylen = 0;
for my $key (keys %$opt)
{
  $keylen = length($key) if length($key) > $keylen;
}
my $slen = 0;
for my $val (values %$source)
{
  $slen = length($val) if length($val) > $slen;
}

for my $key (sort { $a cmp $b} keys %$opt)
{
  printf("  %-" . $keylen . "s (%-" . $slen . "s) = %s\n",
	 $key, $source->{$key}, $opt->{$key})
}

print <<"MSG";

To change these settings, see 'perl Makefile.PL --help' and
'perldoc DBD::mysql::INSTALL'.

MSG

print "Checking if libs are available for compiling...\n";

assert_lib(
  LIBS => $opt->{libs},
);

print "Looks good.\n\n";

sleep 1;

my $dsn= '';
if (exists $opt->{'ps-protocol'}) {
  $dsn = "\$::test_dsn .= \";mysql_server_prepare=1\";\n";
}
elsif (exists $opt->{'bind-type-guessing'}) {
    $dsn= "\$::test_dsn .= \";mysql_bind_type_guessing=1\";\n";
}
my $fileName = $@ ?
  "t/mysql.mtest" : File::Spec->catfile("t", "mysql.mtest");
(open(FILE, ">$fileName") &&
 (print FILE ("{ local " . Data::Dumper->Dump([$opt], ["opt"]) .
	      "\$::test_host = \$opt->{'testhost'};\n" .
	      "\$::test_port = \$opt->{'testport'};\n" .
	      "\$::test_user = \$opt->{'testuser'};\n" .
	      "\$::test_socket = \$opt->{'testsocket'};\n" .
	      "\$::test_password = \$opt->{'testpassword'};\n" .
	      "\$::test_db = \$opt->{'testdb'};\n" .
	      "\$::test_dsn = \"DBI:mysql:\$::test_db\";\n" .
	      "\$::test_dsn .= \";mysql_socket=\$::test_socket\" if \$::test_socket;\n" .
	      "\$::test_dsn .= \":\$::test_host\" if \$::test_host;\n" .
	      "\$::test_dsn .= \":\$::test_port\" if \$::test_port;\n".
	      "\$::test_mysql_config = \$opt->{'mysql_config'};\n" .
              $dsn .
	      "} 1;\n"))  &&
  close(FILE))  ||  die "Failed to create $fileName: $!";


my $cflags = "-I\$(DBI_INSTARCH_DIR) $opt->{'cflags'}";
if ($^O eq 'VMS') {
  $cflags = "\$(DBI_INSTARCH_DIR),$opt->{'cflags'}";
}
$cflags .= " -DDBD_MYSQL_NO_CLIENT_FOUND_ROWS" if $opt->{'nofoundrows'};
$cflags .= " -g ";
my %o = ( 'NAME' => 'DBD::mysql',
	      'INC' => $cflags,
	      'dist'=> { 'SUFFIX'       => ".gz",
			         'DIST_DEFAULT' => 'all tardist',
			         'COMPRESS'     => "gzip -9f" },
	      'clean'     => { 'FILES'   => '*.xsi' },
	      'realclean' => { 'FILES'   => 't/mysql.mtest' },
          'C'         => ["dbdimp.c", "mysql.c", "socket.c"],
          'XS'        => {'mysql.xs' => 'mysql.c'},
	      'OBJECT' => '$(O_FILES)',
	      'LIBS'   => $opt->{'libs'},
	      $opt->{'ldflags'} ? ('LDFLAGS' => $opt->{'ldflags'}) : (),
	      'VERSION_FROM' => 'lib/DBD/mysql.pm'
);

if (eval $ExtUtils::MakeMaker::VERSION >= 5.43) {
  $o{'CAPI'} = 'TRUE'
    if (eval $ExtUtils::MakeMaker::VERSION >= 5.43
	&&  $Config::Config{'archname'} =~ /-object\b/i);
  $o{'AUTHOR'} = 'Patrick Galbraith <patg@patg.net>';
  $o{'ABSTRACT'} =
    'A MySQL driver for the Perl5 Database Interface (DBI)';
  $o{'PREREQ_PM'} = { 'DBI' => 1.609 };
  %o=(%o,
    LICENSE => 'perl',
    MIN_PERL_VERSION => '5.008001',
    META_MERGE => {
      'meta-spec' => { version => 2 },
      dynamic_config => 0,
      resources => {
        repository => {
          type => 'git',
          url  => 'https://github.com/perl5-dbi/DBD-mysql.git',
          web  => 'https://github.com/perl5-dbi/DBD-mysql',
        },
        bugtracker    => { web => 'https://github.com/perl5-dbi/DBD-mysql/issues' },
        x_MailingList => 'mailto:dbi-dev@perl.org',
        license       => ['http://dev.perl.org/licenses/'],
        homepage      => 'http://dbi.perl.org/',
        x_IRC         => 'irc://irc.perl.org/#dbi',
      },
      x_contributors => [
        # a list of our awesome contributors generated from git
        # using the command:
        # git shortlog -se | cut  -f2- | sed "s/^/        '/;s/$/',/"

        'Alceu Rodrigues de Freitas Junior <arfreitas@cpan.org>',
        'Alexandr Ciornii <alexchorny@gmail.com>',
        'Alexey Molchanov <alexey.molchanov@portaone.com>',
        'Amiri Barksdale at Home <amiri@roosterpirates.com>',
        'Andrew Miller <ikari7789@yahoo.com>',
        'Aran Deltac <bluefeet@gmail.com>',
        'Bernt M. Johnsen <bernt.johnsen@oracle.com>',
        'Chase Whitener <chase.whitener@infotechfl.com>',
        'Chip Salzenberg <chip@topsy.com>',
        'Chris Hammond <chrishammond@ymail.com>',
        'Chris Weyl <cweyl@alumni.drew.edu>',
        'Christian Walde <walde.christian@googlemail.com>',
        'Dagfinn Ilmari Mannsåker <ilmari@ilmari.org>',
        'Daisuke Murase <typester@cpan.org>',
        'Damyan Ivanov <dmn@debian.org>',
        'Dan Book <grinnz@gmail.com>',
        'Daniël van Eeden <daniel.vaneeden@booking.com>',
        'Dave Lambley <dave@lambley.me.uk>',
        'David Farrell <davidnmfarrell@gmail.com>',
        'David Steinbrunner <dsteinbrunner@pobox.com>',
        'Giovanni Bechis <giovanni@bigio.snb.it>',
        'Graham Ollis <plicease@cpan.org>',
        'H.Merijn Brand - Tux <h.m.brand@xs4all.nl>',
        'Hanno <hanno@gentoo.org>',
        'James McCoy <jamessan@jamessan.com>',
        'Jim Winstead <jimw@trainedmonkey.com>',
        'Juergen Weigert <jw@suse.com>',
        'Kenny Gryp <kenny.gryp@percona.com>',
        'Lu Shengliang <lushl9301@gmail.com>',
        'Masahiro Chiba <chiba@everqueue.com>',
        'Matthew Horsfall (alh) <WolfSage@gmail.com>',
        'Michiel Beijen <michiel.beijen@gmail.com>',
        'Mike Pomraning <mjp@pilcrow.madison.wi.us>',
        'Mohammad S Anwar <mohammad.anwar@yahoo.com>',
        'Pali <pali@cpan.org>',
        'Patrick Galbraith <patg@patg.net>',
        'Perlover <perlover@perlover.com>',
        'Peter Botha <peterb@striata.com>',
        'Petr Písař <ppisar@redhat.com>',
        'Reini Urban <rurban@cpanel.net>',
        'Rob Hoelz <rhoelz@inoc.com>',
        'Rob Van Dam <rvandam00@gmail.com>',
        'Rudy Lippan <rlippan@remotelinux.com>',
        'Scimon <simon.proctor@gmail.com>',
        'Sergey Zhuravlev <zhurs@ya.ru>',
        'Sergiy Borodych <Sergiy.Borodych@gmail.com>',
        'Sharif Nassar <mrwacky42+github@gmail.com>',
        'Steffen Mueller <smueller@cpan.org>',
        'Steven Hartland <steven.hartland@multiplay.co.uk>',
        'Taro Kobayashi <9re.3000@gmail.com>',
        'Tatsuhiko Miyagawa <miyagawa@bulknews.net>',
        'Tim Mullin <tim@cpanel.net>',
        'Ville Skyttä <ville.skytta@iki.fi>',
        'Vladimir Marek <vlmarek@volny.cz>',
        'katyavoid <katyavoid@gmail.com>',
        'kmx <kmx@cpan.org>',
        'tokuhirom <tokuhirom@gmail.com>',
        'zefram <zefram@fysh.org>',
        'zentooo <ankerasoy@gmail.com>',
      ],
      prereqs => {
        test => {
          recommends => {
            'Proc::ProcessTable' => 0,
          },
          suggests => {
            'Test::Pod' => '1.00',
            'Test::DistManifest' => 0,
          },
        },
      },
    },
    TEST_REQUIRES => { 'bigint'       => 0,
                       'Test::Simple' => '0.90',
                       'Test::Deep'   => 0,
                       'Time::HiRes'  => 0,
    },
    CONFIGURE_REQUIRES => { 'DBI' => '1.609',
                            'Data::Dumper' => 0,
                            'Devel::CheckLib' => '1.09',
                            'ExtUtils::MakeMaker' => 0,
    },
  );
}

WriteMakefile1(%o);
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
                         to the value of "mysql_config --cflags" or a guessed
                         value
  --libs=<libs>          Use <libs> for running the linker; defaults
                         to the value of "mysql_config --libs" or a gussed
                         value
  --testdb=<db>          Use the database <db> for running the test suite;
                         defaults to $TESTDB
  --testuser=<user>      Use the username <user> for running the test suite;
                         defaults to no username
  --testpassword=<pwd>   Use the password <pwd> for running the test suite;
                         defaults to no password
  --testhost=<host>      Use <host> as a database server for running the
                         test suite; defaults to localhost.
  --testport=<port>      Use <port> as the port number of the database;
                         by default the port number is chosen from the
                         mysqlclient library
  --testsocket=<socket>  Use <socket> as the socket for the test database
  --mysql_config=<path>  Specify <path> for mysql_config script
  --nocatchstderr        Suppress using the "myld" script that redirects
                         STDERR while running the linker.
  --nofoundrows          Change the behavior of \$sth->rows() so that it
  			 returns the number of rows physically modified
			 instead of the rows matched
  --ps-protocol          Toggle the use of driver emulated prepared statements
                         prepare, requires MySQL server >= 4.1.3 for
                         server side prepared statements, off by default
  --bind-type-guessing   Toggle the use of driver attribute mysql_bind_type_guessing
                         This feature makes it so driver-emulated prepared statements
                         try to "guess" if a value being bound is numeric, in which
                         case, quotes will not be put around the value.
  --help                 Print this message and exit

All options may be configured on the command line. If they are
not present on the command line, then mysql_config is called (if
it can be found):

  mysql_config --cflags
  mysql_config --libs

and so on. See DBD::mysql::INSTALL for details.
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
    if ($param eq 'bind-type-guessing') {
        $source->{$param}= ($opt->{$param}) ? "User's choice" : 'default';
        return;
    }
    if ($param eq 'ps-protocol') {
        $source->{$param}= ($opt->{$param}) ? "User's choice" : 'default';
        return;
    }
    if (defined($opt->{$param}) and length($opt->{$param})) {
        $source->{$param} = "User's choice";
        return;
    }

    # First try to get options values from mysql_config
    my @mysql_config_options = qw(
      cflags include libs libs_r plugindir socket port version
    );
    if ( grep {$_ eq $param} @mysql_config_options ) {
        my $command = $opt->{'mysql_config'} . " --$param";
        eval
        {
            open(PIPE, "$command |") or die "Can't find mysql_config.";
        };

        if (!$@) {
            my $str = "";
            while (defined(my $line = <PIPE>)) { $str .= $line; }

            if ($str ne ""  &&  $str !~ /Options:/) {
                $str =~ s/\s+$//s;
                $str =~ s/^\s+//s;

                # Unfortunately ExtUtils::MakeMaker doesn't deal very well
                # with -L'...'
                $str =~ s/\-L\'(.*?)\'/-L$1/sg;
                $str =~ s/\-L\"(.*?)\"/-L$1/sg;

                # Separate libs from ldflags
                # Ignore static libs like libgnutls.a as reported by MariaDB's mysql_config
                if ($param eq 'libs') {
                    my (@libs, @ldflags);
                    for (split ' ', $str) {
                        if (/^-[Ll]/ || /^[^\-]/) { push @libs, $_ unless /\.a$/ }
                        else          { push @ldflags, $_ }
                    }
                    $str = "@libs";
                    $opt->{ldflags} = "@ldflags";
                    $source->{ldflags} = "mysql_config";
                }

                if ($param eq 'version') {
                  if ($str !~ /^[89]\./) {
                    die "DBD::mysql requires MySQL 8.x or newer for building. Version reported by $command: $str";
                  }
                }

                $opt->{$param} = $str;
                $source->{$param} = "mysql_config";
                return;
            }
        }
        else {
            print "Can't find mysql_config. Use --mysql_config option to specify where mysql_config is located\n";
        }
    }

    # Ok, mysql_config doesn't work. We need to do our best
    # First check environment variables
    if (defined($ENV{'DBD_MYSQL_'.uc($param)})) {
        $opt->{$param} = $ENV{'DBD_MYSQL_'.uc($param)};
        $source->{$param} = 'environment';
    }

    # Then try to guess
    unless ($opt->{$param}) {
      if ($param eq 'testuser') {
        my $user = $ENV{USER} || '';
        print "

PLEASE NOTE:

For 'make test' to run properly, you must ensure that the
database user '$user' can connect to your MySQL server
and has the proper privileges that these tests require such
as 'drop table', 'create table', 'drop procedure', 'create procedure'
as well as others.

mysql> CREATE USER '$user'\@'localhost' IDENTIFIED BY 's3kr1t';
mysql> GRANT ALL PRIVILEGES ON test.* TO '$user'\@'localhost';

You can also optionally set the user to run 'make test' with:

perl Makefile.PL --testuser=username

";
      $opt->{$param} = $user;
	  $source->{$param} = 'guessed';
    }
    elsif ($param eq "nocatchstderr" || $param eq "nofoundrows") {
      $source->{$param} = "default";
      $opt->{$param} = 0;
    }
    elsif ($param eq "testdb") {
      $source->{$param} = "default";
      $opt->{$param} = $TESTDB;
    }
    elsif ($param eq "testhost" || $param eq "testport"  ||
        $param eq "testpassword" || $param eq "testsocket" ) {
      $source->{$param} = "default";
      $opt->{$param} = "";
    }
    elsif ($param eq "cflags") {
      $source->{$param} = "guessed";
      my $dir = SearchFor('include', 'mysql.h');
      if ($dir) {
        $opt->{$param} = "-I$dir";
        return;
      }
      die <<"MSG";
Failed to determine directory of mysql.h. Use

  perl Makefile.PL --cflags=-I<dir>

to set this directory. For details see DBD::mysql::INSTALL,
section "C Compiler flags" or type

  perl Makefile.PL --help
MSG
    }
    else {
      die "Unknown configuration parameter: $param";
    }
  }
}

my $fineDir;
sub SearchFor {
  my($subdir, @files) = @_;

  my @dirs = ($^O eq 'MSWin32') ? qw(C:) : qw(/usr/local /usr /opt);
  unshift(@dirs, $fineDir) if defined($fineDir);

  for my $f (@files) {
    for my $dir (@dirs) {
      my $try1 = File::Spec->catdir($dir, $subdir);
      my $try2 = File::Spec->catdir($dir, "mysql");
      my $try3 = File::Spec->catdir($try1, "mysql");
      my $try4 = File::Spec->catdir($try2, $subdir);
      for my $path ($try3, $try4, $try2, $try1, $dir) {
	my $file = File::Spec->catfile($path, $f);
	if (-f $file) {
	  $fineDir = $dir;
	  return $path;
	}
      }
    }
  }
}

sub SearchFor2 {
  my($files, $dirs) = @_;

  for my $f (@{$files})
  {
    for my $dir (@{$dirs})
    {
      if (-f File::Spec->catfile($dir, $f))
      {
        $fineDir = $dir;
        return $dir;
      }
    }
  }
}

sub replace
{
  my ($str, $ref)=@_;

  for my $find (keys %{$ref})
  {
    $str =~ s/$find/$ref->{$find}/g;
  }
  $str;
}

sub prepare_files
{
  my ($files)= @_;
  my $line;
  my @lib;

  for my $file (keys %{$files})
  {

    if ($files->{$file}->{makedir})
    {
      mkpath $files->{$file}->{makedir} or die "Can't create dir $files->{$file}->{makedir}"
      unless (-e $files->{$file}->{makedir} && -d $files->{$file}->{makedir});
    }

    my $replace=$files->{$file}->{replace};

    if ($replace)
    {
       open(FILE, $file) or die "Can't open file $file";
       @lib= map { $replace ? replace($_, $replace) : $_; }  <FILE>;
       close(FILE);

       open(FILE, ">".$files->{$file}->{filename}) or die "Can't open file $files->{$file}->{filename}";
       print FILE @lib;
       close(FILE);
    }
    else
    {
       if(!copy($file, $files->{$file}->{filename}))
       {
         die "Unable to copy $file to $files->{$file}->{filename}\n";
       }
    }
  }
}

sub create_makefile
{
  my ($cnf)=@_;

  open(LOG, ">mysqlEmb/Makefile.conf") or die "Can't write to file mysqlEmb/Makefile.conf";
  print LOG $cnf;
  close(LOG);
}

package MY;
sub postamble { return DBI::DBD::dbd_postamble(@_); }

package main;

sub WriteMakefile1 {  #Written by Alexandr Ciornii, version 0.21. Added by eumm-upgrade.
  my %params=@_;
  my $eumm_version=$ExtUtils::MakeMaker::VERSION;
  $eumm_version=eval $eumm_version;
  die "License not specified" if not exists $params{LICENSE};
  if ($params{BUILD_REQUIRES} and $eumm_version < 6.5503) {
    #EUMM 6.5502 has problems with BUILD_REQUIRES
    $params{PREREQ_PM}={ %{$params{PREREQ_PM} || {}} , %{$params{BUILD_REQUIRES}} };
    delete $params{BUILD_REQUIRES};
  }
  if ($params{TEST_REQUIRES} and $eumm_version < 6.64) {
    #EUMM 6.64 has problems with TEST_REQUIRES
    $params{PREREQ_PM}={ %{$params{PREREQ_PM} || {}} , %{$params{TEST_REQUIRES}} };
    delete $params{TEST_REQUIRES};
  }
  delete $params{CONFIGURE_REQUIRES} if $eumm_version < 6.52;
  delete $params{MIN_PERL_VERSION} if $eumm_version < 6.48;
  delete $params{META_MERGE} if $eumm_version < 6.46;
  delete $params{META_ADD} if $eumm_version < 6.46;
  delete $params{LICENSE} if $eumm_version < 6.31;
  delete $params{AUTHOR} if $] < 5.005;
  delete $params{ABSTRACT_FROM} if $] < 5.005;
  delete $params{BINARY_LOCATION} if $] < 5.005;

  ExtUtils::MakeMaker::WriteMakefile(%params);
}
__DATA__

my %opts = ();
GetOptions(\%opts,
           'cflags',
           'libs',
           'port',
           'version',
           'help',
          ) or usage();

usage() if ($opts{help} or not %opts);

SWITCH : {
  local $\ = "\n";
  $opts{cflags} and do {
    print $cflags;
    last SWITCH;
  };
  $opts{libs} and do {
    print $libs;
    last SWITCH;
  };
  $opts{port} and do {
    print $port;
    last SWITCH;
  };
  $opts{version} and do {
    print $version;
    last SWITCH;
  };
  usage();
}

exit(0);

sub usage {
  print << "EOU";
Usage: $0 [OPTIONS]

Options:
        --cflags         [$cflags]
        --libs           [$libs]
        --port           [$port]
        --version        [$version]
EOU
    exit(1);
}
