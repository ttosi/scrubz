#! /usr/bin/perl
use strict;
use warnings;

local $/=undef;
open(FILE, "townado.txt");

my $file = <FILE>;
close(FILE);

for(1..2) {
	open(OUT, ">>source/data$_.txt");

	for(1..200000) {
		print OUT $file;
		#print "$_\n";
	}
	close(OUT);
	print "data$_.txt\n";
}

print "DONE\n";
