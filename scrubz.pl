#! /usr/bin/perl
use strict;
use warnings;
use threads;
use threads::shared;
use Digest::SHA qw(sha512_hex);

my $numThreads = 8; # how many files to be processed in parallel (use number of cores)
my $recordBufferSize = 10000; # how many records to be written out at once
my $salt = "c058da7699634fb1a927ab65d031c45c5d5a2b7b2ab24bd191989cd2362884d1";
my @processingTimes:shared = ();

my $soureDir = "source";
my $processedDir = "processed";

local $| = 1; # turn on auto-flush so console output is displayed immediately
my $start = time; # start the execution timer

my @files:shared = glob($soureDir . '/*.txt'); # get list of files in the soureDir
my @threads = 1..$numThreads; # create array that holds the number of threads defined
my $numFiles = scalar(@files);

print "Processing $numFiles files using $numThreads threads\n";

# read in columns definitions
my @columnNames = ();
my @columnIndexes = ();
#my @columns;

# open(COLDEFS, "columndefs.txt");
open(my $fh, "<", "columndefs.txt");
while(<$fh>) {
	chomp;
	my($columnName, $index) = split(':', $_, 2);

	push(@columnNames, $columnName);
	push(@columnIndexes, $index);
}
close($fh);

my $header = join '|', @columnNames;

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

		# add column headers
		print OUTFILE "$header\r\n";

		# loop through the records
		while(<INFILE>) {
			# process the current record and push it onto the buffer
			my $record = Process_Record($_);
			push(@recordBuffer, $record);

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

		my $processingTime = (time - $fileTime) / 60;
		push(@processingTimes, $processingTime);

		printf "%.2f mins (%.2f mins)\n", (time - $start) / 60, $processingTime;
	}
}

# primary processing logic for a single record.
# this routine will replace the values *inline*.
# no memory is wasted allocating memory for a new record.
sub Process_Record {
	my $record = shift;
	my $outRecord;

	my $pan = substr($record, 0, 19); # get the card number
	$pan =~ s/\s+$//; # right trim
	my $hashedPan = uc sha512_hex($salt . $pan);
	$outRecord .= "$hashedPan|";

	my $runningIndex = 19;
	for (1 .. $#columnIndexes) {
		my $data = substr($record, $runningIndex, $columnIndexes[$_]);
		$data =~ s/\s+$//;

		$outRecord .= "$data|";
		$runningIndex += $columnIndexes[$_];
	}

	return $outRecord . "\n";
}

my $totalFileProcessingTime = 0;
foreach(@processingTimes) {
	$totalFileProcessingTime += $_;
}

my $duration = (time - $start) / 60;

printf "Average file processed time %.2f minutes\n", $totalFileProcessingTime / $numFiles;
printf "Processing completed in %.2f minutes\n", $duration;
