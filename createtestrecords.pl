#! /usr/bin/perl
use strict;
use warnings;

local $/=undef;
open(FILE, "testdata.txt");

my $file = <FILE>;
close(FILE);

for(1..4) {
	open(OUT, ">>source/data$_.txt");

	for(1..200000) {
		print OUT $file;
	}
	close(OUT);
	print "data$_.txt\n";
}

print "DONE\n";
