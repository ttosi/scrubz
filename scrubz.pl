#! /usr/bin/perl
use strict;
use warnings;
use threads;
use threads::shared;
use Digest::SHA qw(sha512_hex);

my $numThreads = 4; # how many files to be processed in parallel (use number of cores)
my $recordBufferSize = 500000; # how many records to be written out at once
my $seed = "c058da7699634fb1a927ab65d031c45c";

my $soureDir = "source";
my $processedDir = "processed";

local $| = 1; # turn on auto-flush so console output is displayed immediately
my $start = time; # start the execution timer

my @files:shared = glob($soureDir . '/*.txt'); # get list of files in the soureDir
my @threads = 1..$numThreads; # create array that holds the number of threads defined

print "Processing " . scalar(@files) . " files using $numThreads threads\n";

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
		my $fileTime = time; # start the per file processing timer

		(my $outFile = $inFile) =~ s/$soureDir/$processedDir/;

		open(INFILE, $inFile);
		open(OUTFILE, ">>$outFile");

		# loop through the records
		while(<INFILE>) {
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
		close(INFILE);

		printf "%.2f mins (%.2f mins)\n", (time - $start) / 60, (time - $fileTime) / 60;
	}
}

# primary processing logic for a single record.
# this routine will replace the values *inline*.
# no memory is wasted allocating memory for a new record.
sub Process_Record {
	my $record = shift;

	# get the values to be transformed. because the file is fixed width,
	# this is a manageable way to break out the values
	my $pan = substr($record, 0, 16); # get the card number

	# do the hashing
	my $hashedPan = sha512_hex($pan . $seed);

	# replace original values with the hashed ones
	$record =~ s/$pan/$hashedPan/;

	return $record;
}

my $duration = (time - $start) / 60;
printf "Processing completed in %.2f minutes\n", $duration;
