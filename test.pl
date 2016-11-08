use Try::Tiny;

my @threads = 1..3;
print @threads[2];


my $ssn = "384766112";
print "$ssn";
(my $maskedSsn = $ssn) =~ s/^...../XXXXX/;
print "$maskedSsn";
