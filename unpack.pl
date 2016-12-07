#! /usr/bin/perl
use strict;
use warnings;
my $delimeter = 'A';
use Data::Dumper;

# read in columns definitions
my @columnNames;
my @columnIndexes;
my $template;

# open(COLDEFS, "columndefs.txt");
open(my $fh, "<", "columndefs.txt");
while(<$fh>) {
	chomp;
	my($columnName, $index) = split(':', $_, 2);
	#print "$index\n";
	#chomp($columnName);
	push(@columnNames, $columnName);
	chomp($index);
	push(@columnIndexes, $index);
}
close($fh);

for(@columnIndexes) {
	$template .= "A$_";# . "x";
	#print "A$_" . "x";
}

#chop($template);

print "$template";

#print @columnIndexes;
#
# my $ff;
# for (0 .. $#columnIndexes) {
# 	$template .= "A" . $columnIndexes[$_];
# 	#print "A$columnIndexes[$_]\n"
# 	my $wtf = "A$columnIndexes[$_]";
# 	print "$wtf\n";
# 	$ff = $ff + $wtf;
# 	# my $data = substr($record, $runningIndex, $columnIndexes[$_]);
# 	# $data =~ s/\s+$//;
# 	#
# 	# push(@elements, $data);
# 	# $runningIndex += $columnIndexes[$_];
# }


# print @columnIndexes;
# print "\n\n\n";
# print @columnNames;

#print "$ff";

# my $header = join $delimeter, @columnNames;
# my $template = join $delimeter, @columnIndexes;
# print "$header\n\n";
# print "$template";

#print @columnIndexes;

# for(@columnIndexes) {
# 	print "$_\n";
# 	$idx .= "A$_";
# }
#print @columnIndexes;

#print "$idx";

my $line = "1234560280600000   2016-08-05 16:02:45.000000MA49.23               870                 8401.000000            AA0                    VS0001-01-01AYP  FP.FRD.C 87         0          20         41         8          FN_FIS_D      IFS0910       2015-05-052017-05-282017-05-310                   2015-05-05 DS1953-12-250                   U0                   79045443579045790840      5411                    WAL-MART #3384                          WAL-MART #3384                          HEREFORD                      HEREFORD                      79045    79045    79045790TX TX       0          1          0             N 00  *N   00        00 0         0                                      000000000243384 S P                M    000000000243384 S P                M    0                                       0          0                                       0          0                                       0          0                                       0                                                  0          2016-08-05 16:02:42.770123                                                                            0                   0.00                0.00                0.00                                P990961110   84024338401        PM00                             0    0    0       0001-01-01    0001-01-0100                     0.00                486959                                                      2016-08-05 16:02:42.7701232016-08-0520160805   1032507    0057a4ef14001ce7                                                        486959     ";

#my $indexes = join 'A', @columnIndexes;
# for(@columnIndexes) {
#
# }

#$indexes = "A$indexes";

# print "$idx\n";
#
#$template = "a19a26a1a1";

my @data = unpack($template, $line);
#my ($data1, $data2, $data3, $data4) = unpack($template, $line);

# print "|$data1|\n";
# print "|$data2|\n";
# print "|$data3|\n";
# print "|$data4|\n";
#
# #print @data;
#
print "\n\n";
# for(@data) {
# 	print "|$_|\n";
# }

$/ = " ";
chomp(@data);

#my @newarray = grep(s/\s+$//g, @data);
#print Dumper \@newarray;
#print @data;
my $line = join '|', @data;
print "\n\n\n$line";
