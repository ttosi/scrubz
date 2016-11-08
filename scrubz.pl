#! /usr/bin/perl
use strict;
use warnings;
use threads;
use threads::shared;
use Digest::MD5 qw(md5_hex);

my $numThreads = 4; # how many files to be processed in parallel
my $recordBufferSize = 10000; # how many records to be written out at once
my $soureDir = "original";
my $outputDir = "processed";

local $| = 1; # turn on auto-flush so output is displayed immediately
my $start = time; # start the execution timer

my @files:shared = glob($soureDir . '/*'); # get list of files in the soureDir
my @threads = 1..$numThreads; # create array for the number of threads defined

# create and start the threads
foreach(@threads) {
	$_ = threads->create("Process_File");
}

# join them so the app will run until all threads are done
foreach(@threads) {
	$_->join();
}

sub Process_File {
	# @file is a shared array. Each thread will continue
	# to pop a file until they are all processed
	while(scalar(@files) != 0) {
		my $inFile = pop(@files);
		my @recordBuffer;

		(my $outFile = $inFile) =~ s/$soureDir/$outputDir/;

		open(FILE, $inFile);
		open(OUTFILE, ">>$outFile");

		# loops through the records
		while(<FILE>) {
			# process the current record and push it onto the buffer
			push(@recordBuffer, Process_Record($_));

			# only write to disk every N records
			if(scalar(@recordBuffer) == $recordBufferSize) {
				print OUTFILE @recordBuffer;
				@recordBuffer = ();
			}
		}

		# when the loop has finsihed, write out anything
		# left over in the buffer
		if(scalar(@recordBuffer) > 0) {
			print OUTFILE @recordBuffer;
		}

		close(OUTFILE);
		close(FILE);

		print(".");
	}
}

# processing logic for a single record
sub Process_Record {
	my $record = shift;

	# get the values to be transformed
	my $pan = substr($record, 5, 16); # get pan starting at col5
	#my $ssn = substr($record, 110, 128);
	#my $lastname = ...

	# do the transformations
	my $hashedPan = md5_hex($pan); # hash the pan
	#(my $maskedSsn = $ssn) =~ s/^...../XXXXX/; # mask first 6 of the ssn
	#my $fakeLastname = ...

	# replace original values
	$record =~ s/$pan/$hashedPan/;
	#$record =~ s/$ssn/$maskedSsn/;
	#$record =~ s/$lastname/$fakeLastName/;

	return $record;
}

my $duration = (time - $start) / 60;
printf "\nProcessing completed in %.2f minutes\n", $duration;
