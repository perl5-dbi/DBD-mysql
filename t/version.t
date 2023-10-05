use strict;
use warnings;

use DBD::mysql;
use Bundle::DBD::mysql;
use Test::More;

like($DBD::mysql::VERSION, qr/^\d\.\d{2,3}(|_\d\d)$/, 'version format');
like($DBD::mysql::VERSION, qr/^5\./, 'version starts with "5." (update for 6.x)');
is(
  $DBD::mysql::VERSION,
  $Bundle::DBD::mysql::VERSION,
  'VERSION strings should be the same in all .pm files in dist'
);

done_testing;
