use strict;
use warnings;

use DBD::mysql;
use Bundle::DBD::mysql;
use Test::More;

like($DBD::mysql::VERSION, qr/^\d\.\d{2,3}(|_\d\d)$/, 'version format');
like($DBD::mysql::VERSION, qr/^4\./, 'version starts with "4." (update for 5.x)');
is(
  $DBD::mysql::VERSION,
  $Bundle::DBD::mysql::VERSION,
  'VERSION strings should be the same in all .pm files in dist'
);

diag("mysql_get_client_version: ", DBD::mysql->client_version);
cmp_ok(DBD::mysql->client_version, ">", 0, "mysql_get_client_version is available as a standalone function");

done_testing;
