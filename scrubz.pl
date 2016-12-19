#! /usr/bin/perl
use strict;
use warnings;
use threads;
use threads::shared;
use Digest::SHA qw(sha512_hex);
use IO::Compress::Gzip qw(gzip $GzipError);
use IO::Uncompress::Gunzip;
use File::Basename;
use File::Path qw/make_path/;

#### START CONFIG ###
my $salt = "c058da7699634fb1a927ab65d031c45c5d5a2b7b2ab24bd191989cd2362884d1";
my $sourceDir = "source";
my $processedDir = "processed";
my $columnDefFile = "columndefs.txt";

my $numThreads = 8; # how many files to be processed in parallel

my $recordBufferSize = 10000; # how many records to be written out at once
my $numLinesToSplitOn = 2475000; # numLinesToSplitOn MUST BE > recordBufferSize

my $delimitFile = 0;
my $includeHeader = 0;
my $delimeter = '|';
#### END CONFIG ###

my @columnNames;
my @columnIndexes;

local $| = 8; # turn on auto-flush so console output is displayed immediately
my $start = time; # start the execution timer

my @threads = 1..$numThreads; # create array that holds the number of threads defined
my $template;

my @files:shared = glob($sourceDir . '/*'); # get list of files in the soureDir
my $numFiles = scalar(@files);

print "Processing $numFiles file(s) using $numThreads threads\n";
for(@files) {
	my $size = (-s $_) / 1024 / 1024 / 1024; #convert bytes to gb
	printf "-- $_: %d gb\n", $size;
}

my $t = localtime;
print "\nStarted at $t\n";

# read in column definitions
open(my $colDefHandle, "<", $columnDefFile);
while(<$colDefHandle>) {
	chomp;
	my($columnName, $index) = split(':', $_, 2);

	push(@columnNames, $columnName);
	push(@columnIndexes, $index);
}
close($colDefHandle);

my $header = join $delimeter, @columnNames;
my $rest = 0;
chomp($header);

# create the template used by the unpack call
if($delimitFile) {
	$template = "A" . join "A", @columnIndexes;
} else {
	$rest += $_ for @columnIndexes;
	$template = "A19A" . ($rest - 19);
}

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
		my $fileTime = time; # start the per file processing timer
		my $inHandle;
		my $outHandle;
		my @recordBuffer;

		my $path = $processedDir . "/" . basename(substr($inFile, 0, index($inFile, '.')));
		make_path($path);

		# when configured open the in and out files using Gunzip
		# and Gzip, this allows the files to be streamed directly
		# from and in to compressed files; otherwise open and/or
		# write normally
		open($inHandle, '<', $inFile);



		# write out column headers
		# if($includeHeader) {
		# 	print $outHandle "$header\n";
		# }

		###########

		#(my $outFile = $inFile) =~ s/$sourceDir/$processedDir\/$subDirectory/;

		#$outFile =~ s/\./_$fileIndex\./;
		#print "\n\n$outFile\n\n";

		##########

		# loop through the records
		my $lineCount;
		my $fileIndex = 0;

		while(<$inHandle>) {
			if($fileIndex == 0 or $lineCount >= $numLinesToSplitOn) {
				my $outFile = basename($inFile);

				$lineCount = 1;
				$fileIndex++;

				print "$fileIndex\n";

				$outFile =~ s/\./_$fileIndex\./;
				$outFile = $path . "/" . $outFile;

				print "\n\n$outFile\n\n";

				open($outHandle, '>:encoding(UTF-8)', $outFile);
				binmode($outHandle, ":utf8");
			}

			my ($pan, @data, $rest);

			# process the current record and push it onto the buffer
			if($delimitFile) {
				($pan, @data) = unpack($template, $_);
				$_ = join '|', @data;
			} else {
				($pan, $rest) = unpack($template, $_);
			}

			# hash the pan (prepend the salt)
			my $hashedPan = uc sha512_hex($salt . $pan);

			if($delimitFile) {
				push(@recordBuffer, $hashedPan . '|' . $_ . "\n");
			} else {
				push(@recordBuffer, "$hashedPan   $rest\n");
			}

			# only write to disk every N records
			if(scalar(@recordBuffer) == $recordBufferSize) {
				print $outHandle @recordBuffer;
				@recordBuffer = ();
			}

			$lineCount++;
		}

		# when the loop has finsihed, write out anything
		# left over in the buffer
		if(scalar(@recordBuffer) > 0) {
			print $outHandle @recordBuffer;
		}

		close($outHandle);
		close($inHandle);

		# $outFile =~ s/$processedDir\///;
		# printf "-- $outFile processed in %.2f mins\n", (time - $fileTime) / 60;
	}
}

printf "Run completed in %.2f minutes\n", (time - $start) / 60;
