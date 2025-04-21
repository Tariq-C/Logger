#!/opt/bcgov/bin/perl
# Package Name: Log
# Description:
#     This is an internal package that keeps track
#     of information on a log file
#     this script also does log specific tasks such as logging
#     keeping track of processes and maintaining printstreams
package Log;
   # Constructor
   sub new {
      my $class = shift;

      my @values = @_;
      # Set/Default Class Variable Values
      # Only what is necessary if a message comes before the meta-data
      my $path     = $values[0] ? $values[0] : "/var/bcgov/log/nopath.log";
      my $type     = "Basic";

      # Class Variables
      my $self = {
         "rollover"       => 1        # How many rollovers this file will keep
         ,"rInterval"     => 3600*24  # Number of seconds between rollovers
         ,"nextRoll"      => -1       # When to roll next
         ,"path"          => $path    # The path to the log file
         ,"io"            => ""       # The IO Pipe for printing to file
         ,"openStream"    => 0        # Bool of whether pipe is open or closed
         ,"type"          => $type    # The file type of this log
         ,"processes"     => {}       # A hash of processes currently running as per a file $self->{processes}{$pid} = Process_Object
         ,"printCount"    => 0        # A count of number of messages between heartbeats
         ,"exclude"       => ["io","openStream","processes","exclude","nextRoll"]
      };

      bless $self, $class;
      $self->openPipe();
      return $self;
   }

   # A subroutine for adding metadata to the object. This is to update in case we start a log and get the init values later
   # An example of this happening is during a restart where Logger may start after a script that relies on Logger
   sub add_meta {
      my $self = shift;
      my $param = shift;
      for my $key (keys %{$param}){
         $self->{$key} = $param->{$key};
      }
   }

   # Name: rollover
   # Param:    N/A
   # Return:   $status - Whether the rollover was successful or not
   # Description:
   #     rolls the file over.
   #
   sub rollover {
      my $self = shift;
      my $fileName = $self->{path};
      my $pass = 1;
      $fileName =~ /(.*)\.([^\.]+)$/;
      my ($name, $suffix) = ($1, ".".$2);
      $pass = $self->closePipe();
      my $logMax = $self->{rollover};
      for (my $i = $logMax-2; $i >= 0; $i--){
         my $cFile = $name.'.'.$i.$suffix;
         my $nFile = $name.'.'.($i+1).$suffix;
         if (-e $cFile){
            `mv $cFile $nFile`;
         }
      }
      my $nextFile = $name.'.0'.$suffix;
      `cp $fileName $nextFile`;
      `rm $fileName`;
      $pass = $self->openPipe();
      return 1;
   }

   # Name: preRoll
   # Param: $time - The current epoch time
   # Return: $status - Whether the log should roll or not
   # Description:
   #      Compares current time to the next desired rollover time returns whether it has passed or not
   #
   sub preRoll {
      my $self = shift;
      my $time = shift;
      my $next_roll = $self->{nextRoll};
      my $rollInterval = $self->{rInterval};
      my $rollover  = $self->{rollover};
      my $fileName = $self->{path};

      if ($fileName and -e $fileName){
         my $file_size = int((split(" ", `ls -l $fileName`))[4]);
         if ($file_size == 0) {return -2}
      }else{return -1;}

      if ($next_roll < 0) {
         if ($rollInterval % 3600 != 0) {
            $self->{nextRoll} = $time - ($time%60) + $rollInterval;
         }elsif ($rollInterval % 86400 != 0) {
            $self->{nextRoll} = $time - ($time%3600) + $rollInterval;
         }else {
            $self->{nextRoll} = $time - ($time%86400) + $rollInterval;
         }
         return 0;
      }
      if ($time >= $next_roll && $rollover > 0) {
         return 1;
      }
      if ($next_roll > $time) {
         return 0;
      }
      return 0;
   }

   # Name: postRoll
   sub postRoll {
      my $self = shift;
      $self->{nextRoll} += $self->{rInterval};
   }
   # Name: logF
   # Param:
   #     $pid        => The PID that is writing the message
   #     $timestamp  => Timestamp of the message
   #     $message    => String message to be logged
   # Return: N/A
   # Description:
   #     Logs a specified message to a specified log file with a timestamp prefix and a newline suffix
   #
   sub logF {
      my ($self, $pid, $timestamp, $message) = @_;
      my $idas = IDAS->new();
      if (!$timestamp){$timestamp = localtime();}
      chomp($message);
      if(!$self->{processes}{$pid}){
         $self->{processes}{$pid} = Process->new($pid);
      }
      if(!$self->{openStream}){
         if ($self->openPipe() == 0) {
            return 0;
         }
      }
      $self->{processes}{$pid}->logAssist();

      my $full_message = $self->format_message($pid, $timestamp,$message);
      my $fileOut    = $self->{io};
      $self->{printCount}++;
      print $fileOut ($full_message);
      return 1;
   }

   sub format_message {
      my ($self, $pid, $timestamp, $message) = @_;
      my $idas = IDAS->new();
      chomp($message);

      my @messageElements;
      push (@messageElements, $timestamp) if $self->{showts};
      push (@messageElements, $pid)       if $self->{showpid};
      push (@messageElements, $message);
      my $full_message = "";

      if ($self->{type} =~/csv/i){
         $full_message = $idas->list2csv(@messageElements);
      }else{
         $full_message = join(" : ", @messageElements);
      }

      $full_message .= "\n";
      return $full_message;
   }

   # A subroutine to handle heartbeat updates across the object
   sub heartbeat {
      my $self = shift;
      my $alive = 0;
      my $active  = 0;
      my $inactive = 0;
      my $closed   = 0;
      my $count = $self->{printCount};
      $self->{printCount} = 0;
      for $pid (keys %{$self->{processes}}) {
         my $hbStatus = $self->{processes}{$pid}->heartbeat();
         if(!$hbStatus){
            $closed++;
            delete($self->{processes}{$pid});
         }elsif($hbStatus == 2){
            $alive = 1;
            $inactive++;
         }
         else{
            $alive = 1;
            $active++;
         }
      }
      if (!$alive) {
         $self->closePipe();
      }
      return ($active, $inactive, $closed, $count);
   }

   # Name: openPipe
   # Param: N/A
   # Return:
   #     1 - on Success
   #     0 - on Failure
   # Description:
   #     Starts an output file stream and sets output to be unbuffered
   sub openPipe {
      my $self = shift;
      open(my $pipe, ">>", $self->{path}) or return 0;
      select($pipe);
      $| = 1;
      $self->{io} = $pipe;
      $self->{openStream} = 1;
      return 1;
   }

   # Name: closePipe
   # Param: N/A
   # Return: 1 on Success
   # Description:
   #     closes print stream
   sub closePipe {
      my $self = shift;
      $self->{io} = "";
      $self->{openStream} = 0;
      return 1;
   }

   sub to_Hash {
      my $self = shift;
      my %meta;
      for $value (keys %{$self}) {
         my $include = 1;
         for $exclude (@{$self->{exclude}}){
            if($exclude eq $value){
               $include = 0;
            }
         }
         $meta{$value} = $self->{$value} if $include;
      }
      return %meta;
   }

   sub get_meta {
      my $self = shift;
      my @meta;
      for $value (keys %{$self}) {
         my $include = 1;
         for $exclude (@{$self->{exclude}}){
            if($exclude eq $value){
               $include = 0;
            }
         }
         push @meta, "$value=".$self->{$value} if $include;
      }
      return join(",",@meta);
   }
1;

package Process;

   sub new {
      my $class = shift;
      my $max_HP = 3;
      my $self = {
         pid      => shift
         ,curHP   => $max_HP
         ,maxHP   => $max_HP
         ,active  => 1
      };
      bless $self, $class;
      return $self;
   }

   sub heartbeat {
      my $self = shift;
      if ($self->{active} == 0){
         $self->{curHP}--;
         if ($self->{curHP} == 0){
            return 0;
         }
         return 2;
      }else{
         $self->{curHP} = $self->{maxHP};
      }
      $self->{active} = 0;
      return 1;
   }

   sub logAssist {
      my $self    = shift;
      # Do things related to a log being printed
      $self->{active} = 1;
   }


1;

# Imports
use IO::Socket;
use JSON;
use Fcntl qw(:flock SEEK_END);
use Data::Dumper qw(Dumper);
use Getopt::Std;

my $USAGE = "Usage: $0 [-v]".
            "\n\tv : verbose | include for higher level of visibility".
            "\n\th : help    | to quickly get an update on useage of file";

getopts("hv");

if ($opt_h) {
   die $USAGE."\n";
}

# Static Variables
my $logger_name = "Logger";
my $rollover    = 7;
my $idas = IDAS->new();
my $curr_date = 0;
my $prev_date = -1;
my $curr_min = 0;
my $pre_min = -1;
my $verbose_flag = 1 ? $opt_v : 0;

# Start Heartbeat
addHeartBeat();

# Open Logger Log
open($logStream, '>>', $logger_log) or die "Can't open Logger Log\n";
# Turn off Buffer Output
select($logStream);
$| = 1;

logF($logStream, "Logger.pl Started");
logF($logStream, "Heartbeat Started");

# Start Listener
logF($logStream, "Listener Opened on Port ".$port);
my $listener = addListener($port);

# Signal Handling for storing meta-data when closed

$SIG{INT} = sub { createJson() ?
                  logF($logStream, "INT Signal Caught, Successfully Created meta JSON, Closing Now") :
                  logF($logStream, "INT Signal Caught, could not open meta JSON, Closing Now");
                  die;
                  };
$SIG{TERM} = sub { createJson() ?
                  logF($logStream, "TERM Signal Caught, Successfully Created meta JSON, Closing Now") :
                  logF($logStream, "TERM Signal Caught, could not open meta JSON, Closing Now");
                  die;
                  };
$SIG{HUP} = sub { createJson() ?
                  logF($logStream, "HUP Signal Caught, Successfully Created meta JSON, Closing Now") :
                  logF($logStream, "HUP Signal Caught, could not open meta JSON, Closing Now");
                  die;
                  };
#
# Dynamic Variables
my %logs;

#If there is a meta json to be loaded, load that json
if (-e $meta_json) {
   loadJson($meta_json) ;
}else{
   logF($logStream, "Could not find JSON File");
}


# Main Loop
while (1) {
   # Listen for message
   my $packet = "";
   $listener->recv($packet, 1024);
   @values = $idas->csv2list($packet);
   my ($path, $pid, $timestamp, $message) = @values;
   # If packet has content
   if ($path) {
      if (!$message){next;}
      # If the message is a Logger Heartbeat
      # Update all log records to see if any have expired
      # Check to see if the date has changed, if so, roll files
      if($message eq "Logger Heartbeat"){
         logF($logStream, "$message");
         for my $p (keys %logs) {
            my ($active, $inactive, $closed, $count) = $logs{$p}->heartbeat();
            if($closed){
               logF($logStream, "Closed $closed inactive processes for $p");
               if(!$active and !$inactive) {
                  logF($logStream, "No remaining processes for $p, closing file stream");
               }
            }
            if($active) {
               logF($logStream, "$active active processes for $p (Logged $count lines this heartbeat)");
            }
            if ($inactive) {
               logF ($logStream, "$inactive inactive processes for $p");
            }
         }
         # This section activates every day at midnight
         # Will use this as the basis of the dynamic rollover
         # Will just compare linux time with the time saved in the processes
         my @time = localtime();
         my $t = time();


         $curr_min = $time[1]; #this changes on the minute
         if ($curr_min != $pre_min and $pre_min != -1) {

            # Goes through all the logs and sees if they should rollover
            for my $p (keys %logs) {
               my $result = $logs{$p}->preRoll($t);
               logF($logStream, "Current Time $t | Rollover time ".$logs{$p}->{nextRoll}) if ($verbose_flag);
               if ($result > 0){
                  logF($logStream, "Starting Rollover for $p");
                  if($logs{$p}->rollover()){
                     logF($logStream, "Successfully Rolled over $p");
                  }else{
                     logF($logStream, "No New Data, Did not Roll $p");
                  }
                  $logs{$p}->postRoll($t);
               }else{
                  my $reason = " No match in Pre-roll test";
                  $reason = " Current Log File doesn't need to update" if ($result == 0);
                  $reason = " Current Log File does not exist" if ($result == -1);
                  $reason = " Current Log File has no Data" if ($result == -2);
                  logF($logStream, "Failed preRoll because $reason") if ($verbose_flag);
               }
            }
         }
         $curr_date = $time[7]; #this changes on the day change
         if ($curr_date != $prev_date and $prev_date != -1) {
            logF($logStream, "Date Change has been caught, Starting process rollover");
            $logStream = rollover($logger_log, $logStream, $rollover);
            cleanUp();
            createJson();
         }
         $prev_date = $curr_date;
         $pre_min = $curr_min;
         next;
      }

      # If we don't have record of the log
      # Create a new object connected to provided path
      if (!$logs{$path}){
         logF($logStream, "New Log Proccess Identified, tracking pid: $pid for $path");
         $logs{$path} = Log->new($path);
      }

      # If the message contains meta data
      if($message =~ /meta:\{(.*)\}/){
         logF($logStream, "Metadata Obtained for $path");
         my $metaData = $1;
         $logs{$path}->add_meta(extract_meta($metaData));
         $metaData = $logs{$path}->get_meta();
         logF($logStream, "Metadata now set to: $metaData");
         next;
      }

      if($message =~ /Current File Rollover Message/) {
         logF($logStream, "Initiating Manual Rollover");
         if($logs{$path}->rollover()){
            logF($logStream, "Successfully Rolled over $path");
         }else{
            logF($logStream, "No New Data, Did not Roll $path");
         }
         next;
      }

      # Writes Message to appropriate file
      if ($logs{$path}->logF($pid, $timestamp, $message) == 0) {
         logF($logStream, "$path could not be opened - if file already exists check ownership");
      }
   }
}

die logF($logStream, "Exited the while loop, something went wrong");

# Name: AddListener
# Param:
#  $port    => Port Number
# Return:
#  $socket  => Listener Socket
# Description:
#     Creates a listener socket to watch over a specific port via UDP
#
sub addListener {
   my $port = shift;
   my $socket = new IO::Socket::INET (
      LocalPort   => $port,
      Proto       => 'udp',
      Reuse       => 1
   );
   $socket->setsockopt(SOL_SOCKET, SO_RCVBUF, 2048*1024);
   die "Could not create listener socket: $port" unless $socket;
   return $socket;
}

# Name: addHeartBeat
# Param: N/A
# Return: N/A
# Description:
#     Forks and creates a new Speaker Socket for sending a heartbeat message every 15 minutes
#     this is to prevent the main script from stalling and waiting if nothing is running
#     allows for time based events, and updates when there are no incoming messages
#
sub addHeartBeat {
   my $pid = fork();
   if ($pid == 0) {
      $listener = 0;
      my $socket = new IO::Socket::INET (
         PeerPort => "$port",
         PeerHost => 'localhost',
         Proto    => 'udp'
      );
      die "Could not create socket: $!n" unless $socket;
      my @values = ('logger',-1,scalar(localtime()),'Logger Heartbeat');
      my $hb = $idas->list2csv(@values);
      while (1) {
         $socket->send($hb);
         sleep(20);
      }
      die "Heart Beat shouldn't reach here";
   }
}

# Name: metaData
# Param:
#     $metaData = a formatted string containing meta data to regarding log files
# Return: N/A
# Description:
#     Handles the processing of metadata on a log file
sub extract_meta {
   my $metaData = shift;
   my %param = ();
   my @data = $metaData =~ /([^,=]+=[^,]+)[,\n]?/g;
   for $meta (@data){
      $meta =~ /([^=]+)=(.*)/;
      $param{$1} = $2;
   }
   return \%param;
}

sub createJson {
   my $json = "";
   %temp = ();
   for $path (keys %logs) {
      my %logTemp = $logs{$path}->to_Hash();
      $temp{data}{$path} = \%logTemp;
   }

   $json = encode_json(\%temp);
   open (my $jfile, '>', $meta_json) or return 0;
   print $jfile ($json);
   close($jfile);
   return 1;
}

sub loadJson {
   my $meta_json_path = shift;
   open (my $jfile, '<', $meta_json_path) or return 0;
   my $json = "";
   while (<$jfile>) {
      $json .= $_;
   }
   my $jref = decode_json($json);

   for $path (keys %{$jref{data}}) {
      $logs{$path} = Log->new($path);
      $logs{$path}->add_meta($temp{data}{$path});
   }
}

# Name: rollover
# Param:
#     fileName - name of the file to rollover
#     pipe     - file print stream
#     rollover - max number of days to keep rolled information
# Return:
#     pipe     - file print stream
# Description:
#     This file checks to see whether the file has rolled over in the last day
#     Logger.log -> Logger.log.0 ::: Logger.log -> Logger.0.log
sub rollover {
   my $fileName = shift;
   my $pipe     = shift;
   my $rollover = shift;
   $fileName =~ /(.*)\.([^\.]+)$/;
   my ($name, $suffix) = ($1, $2);
   $suffix = '.'.$suffix;
   logF($pipe, "Subroutine Rollover Complete, See you tomorrow!");
   close ($pipe);
   my $logMax = $rollover;
   for (my $i = $logMax-2; $i >= 0; $i--){
      my $cFile = $name.'.'.$i.$suffix;
      my $nFile = $name.'.'.($i+1).$suffix;
      if (-e $cFile){
         `mv $cFile $nFile`;
      }
   }
   my $nextFile = $name.'.0'.$suffix;
   `cp $fileName $nextFile`;
   `rm $fileName`;
   open($pipe, ">>", $fileName);
   logF($pipe, "Rollover Complete, Welcome to a new day!");
   return $pipe;
}

# Name: logF
# Param:
#     $pid        => The PID that is writing the message
#     $message    => String message to be logged
#     $timestamp  => [optional] timestamp of the message
# Return: N/A
# Description:
#     Logs a specified message to a specified log file with a timestamp prefix and a newline suffix
#
sub logF {
   my ($pipe, $message) = @_;
   chomp($message);
   my $timestamp = localtime();
   my $full_message = $timestamp." : ".$message."\n";
   print $pipe ($full_message);
}

#For removing one off scripts
sub cleanUp {
   for $path (keys %logs) {
      if ($logs{$path}->{rollover} == 0 and $logs{$path}->{openStream} == 0) {
         delete $logs{$path};
      }
   }
}
1;
