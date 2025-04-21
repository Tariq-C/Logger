
use IO::Socket;

package Logger;
   #
   sub new {
      my $class = shift;
      my $param = shift;

      # Default Parameters
      my $name = ($0 ? $0 : "unknown");
      $name =~ /([^\/]*?)(?:\.pr?l)?$/;
      $name = $1;
      my $rollover   = defined($param->{rollover}) ? $param->{rollover} : 1;
      my $rInterval  = defined($param->{rInterval}) ? $param->{rInterval} : "1d";
      my $type       = defined($param->{type}) ? $param->{type} : "Basic";
      my $path       = defined($param->{path}) ? $param->{path} : "/var/bcgov/log/$name";
      my $preset     = defined($param->{preset}) ? $param->{preset} : "custom";

      if ($path =~ /\/[^\.]+$/){
         if ($type =~ /Basic/i) {$path .= ".log"}
         elsif($type =~ /csv/i) {$path .= ".csv"; $param->{showts} = 0;}
      }

      my $showts     = defined($param->{showts}) ? $param->{showts} : 1;
      my $showpid    = defined($param->{showpid}) ? $param->{showpid} : 0;

      my $lastmeta   = (localtime() =~ / (\d+):/)[0];

      my $self = {
         name          => $name
         ,path          => $path
         ,rollover      => $rollover
         ,rInterval     => $rInterval
         ,type          => $type
         ,showts        => $showts
         ,showpid       => $showpid
         ,count         => 0
         ,lastmeta      => $lastmeta
         ,exclude       => ["name","path","idas","listener_port","exclude","socket","count","lastmeta"]
      };
      bless $self, $class;
      if (!($preset eq "custom")){$self->presets($preset)}
      $self->_convertRollover();
      $self->_connect();
      $self->_init();
      return $self;
   }

   # Name: $self->_connect()
   # Param: N/A
   # Return: N/A
   # Description:
   #     This method creates a connection between the Logger and the Listener via the static port variable
   #
   sub _connect {
      my $self = shift;
      my $current_port = $self->{listener_port};
      my $socket = new IO::Socket::INET (
         PeerPort => "$current_port",
         PeerHost => 'localhost',
         Proto    => 'udp'
      );
      die "Could not create socket: $!n" unless $socket;
      $self->{socket} = $socket;
   }

   # Name: $self->_init()
   # Param: N/A
   # Return: N/A
   # Description:
   #     Sends an initialization message to the listener to configure meta data regarding the log file
   #
   sub _init {
      my $self = shift;
      my $message = "meta:{";
      for my $key (keys %{$self}){
         my $pass = 1;
         for my $okey (@{$self->{exclude}}){
            if ($key eq $okey) {
               $pass = 0;
            }
         }
         if ($pass == 0){
            next;
         }
         $message .= $key."=".$self->{$key}.",";
      }
      $message    .= "}";
      $self->logF($message);
   }

   # Name: $self->logF($message, @variables)
   # Param:
   #     $message = A string representation of the message being sent, can be in printf format
   #     [@variables] = a list of variables to be injected into the printf statement
   # Return: N/A
   # Description:
   #     This is a method that sends a UDP packet to the listener which formats a string via printf standards
   #
   sub logF {
      my $self    = shift;
      my $format  = shift;
      my $ts      = scalar(localtime());
      my $hour    = ($ts =~ / (\d+):/)[0];
      # This is to resend the init message every hour
      if ($self->{lastmeta} ne $hour) {
         $self->{lastmeta} = $hour;
         $self->_init();
      }
      chomp($format);
      $format =~ s/(.*[^\n])\n*$/$1/g;
      my @variables = @_;
      my $socket = $self->{socket};
      my $logm = "";
      if (@variables){
         $logm = sprintf($format, @variables);
      }else{
         $logm = sprintf($format);
      }
      $message = $self->{idas}->list2csv($self->{path}, $$, $ts, $logm);
      if ($self->{count} > 200) {
         sleep(1);
         $self->{count} = 0;
      }
      $socket->send($message);
      $self->{count}++;
   }

   # Name: $self->close()
   # Param: N/A
   # Return: N/A
   # Description:
   #     Closes the socket connection
   #
   sub close {
      my $self = shift;
      close($self->{socket})
   }

   sub metadata {
      my ($self, $var, $val) = @_;
      if (defined $self->{$var}) {
         if (defined $val) {
            $self->{$var} = $val;
         }
         return $value;
      }
      return 0;
   }

   # Converts the string format into number of seconds
   sub _convertRollover {
      my $self = shift;
      my $rollover = $self->{rInterval};
      if ($rollover =~ /^(\d+)m/i) {
         $self->{rInterval} = int($1*60);
      }elsif ($rollover =~ /^(\d+)h/i) {
         $self->{rInterval} = int($1 * 60 *60);
      }elsif ($rollover =~ /^(\d+)d/i) {
         $self->{rInterval} = int($1 * 60 * 60 * 24);
      }else{
         die "$rollover didn't match any regex";
      }
   }

   sub rollNow {
      my $self = shift;
      my $ts      = scalar(localtime());
      my $socket = $self->{socket};
      my $rollM = "Current File Rollover Message";
      my $rollMessage = $self->{idas}->list2csv($self->{path}, $$, $ts, $rollM);
      $socket->send($rollMessage);
      sleep(1);
   }

   sub presets {
      my $self = shift;
      my $preset = shift;
      my %presetHash;

      if ($preset =~ /basic/i) {
         return;
      }


      for $key (keys %presetHash) {
         $self->metadata($key, $presetHash{$key});
      }
   }
1;
