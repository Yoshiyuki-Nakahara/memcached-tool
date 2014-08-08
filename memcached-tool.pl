# ------------------------------------------------------------------------- #
#                          maintenance memcached data                       #
# ------------------------------------------------------------------------- #

use strict;
use warnings;
use utf8;

use Getopt::Long qw(:config no_ignore_case);
use Config::YAML::Tiny;
use Cache::Memcached;
use YAML::Syck;

my %program;

{
    my ( $config, $option, $argv ) = parse_program_environment();
    ( $program{ 'config' },
      $program{ 'option' },
      $program{ 'argv'   } ) = ( $config, $option, $argv );

    verbose( "[ processing main loop ] ... " );

    my $memcache_config;
    if ( defined $option->{'config'} ) {
        $memcache_config = $config;
    }
    else {
        my $host = $option->{'host'};
        my $port = $option->{'port'};
        $memcache_config = {
            'servers'   => [ "$host:$port" ],
            'namespace' => $option->{'namespace'},
            'utf8'      => 1,
        };
    }
    # my $memcache = new Cache::Memcached( $config )
    my $memcache = new Cache::Memcached( $memcache_config )
        or die "Error : memchached connect error\n";

    if ( $option->{'export-key'} ) {
        memcached_export_key( $config, $option, $argv, $memcache )
            or die "Error: failed to memcached_export_key";
    }
    elsif ( $option->{'export'} ) {
        memcached_export( $config, $option, $argv, $memcache )
            or die "Error: failed to memcached_export";
    }
    elsif ( $option->{'import'} ) {
        memcached_import( $config, $option, $argv, $memcache )
            or die "Error: failed to memcached_import";
    }
    elsif ( $option->{'stats'} ) {
        memcached_stats( $config, $option, $argv, $memcache )
            or die "Error: failed to memcached_stats";
    }

    $memcache->disconnect_all;
}


sub search
{
    my $memcache   = shift || return 0;
    my $key_regexp = shift || q{};

    verbose( "[ search memcached ] ..." );

    my $data;
    # --- 各サーバに接続してすべてのアイテムを取得 --- #
    for my $server ( @{ $memcache->{servers} } ) {
        verbose( "  server -> $server" );
        my $memd = new Cache::Memcached( $memcache );
        unless ( $memd ) {
            return 0;
        }
        $memd->set_servers( [ $server ] );

        verbose( "  --- get stats for time and uptime ---" );
        my $stats = $memd->stats( "" );
        my ( $host ) = keys %{ $stats->{hosts} };
        my $stats_time   = $stats->{hosts}->{$host}->{misc}->{time};
        my $stats_uptime = $stats->{hosts}->{$host}->{misc}->{uptime};
        my $expire0_time = $stats_time - $stats_uptime;

        verbose( "  --- get slab_ids by using chunks ---" );
        my @slab_ids = ();
        my $slabs = $memd->stats( 'slabs' );
        for my $host ( keys %{ $slabs->{hosts} } ) {
            my @slab_lines = split /\r?\n/, $slabs->{hosts}->{$host}->{slabs};
            for my $line ( @slab_lines ) {
                my ( $slab_id, $used_chunks ) =
                    $line =~ m/STAT (\d+):used_chunks (\d+)/;
                if ( $used_chunks ) {
                    push @slab_ids, $slab_id;
                }
            }
        }

        verbose( "  --- get all item(key)s and values ---" );
        my @items = ();
        for my $slab_id ( @slab_ids ) {
            my $cd_cmd = "cachedump $slab_id 100000000";
            my $cachedump = $memd->stats( $cd_cmd );
            for my $host ( keys %{ $cachedump->{hosts} } ) {
                my $cachedump_text = $cachedump->{hosts}->{$host}->{$cd_cmd};
                my @lines = split( "\n", $cachedump_text );
                for my $line ( @lines ) {
                    verbose( "", 1, "." );
                    my ( $item, $bytes, $expire_time ) =
                        $line =~ /^ITEM (.+) \[(.+) b; (.+) s\]/;
                    $item =~ s/^($memd->{namespace})// if $memd->{namespace};

                    # --- filtering by regexp --- #
                    if ( $key_regexp ) {
                        next unless $item =~ /$key_regexp/;
                    }

                    # --- get expire remaining time --- #
                    my $remaining_time = $expire_time - $expire0_time;
                    if ( $remaining_time ) {
                        $remaining_time = $remaining_time - $stats_uptime;
                    }
                    #next if $remaining_time < 0;

                    # --- get values --- #
                    if( my $value = $memd->get( $item ) ) {
                        $data->{$host}->{$item}->{value} = $value;
                        $data->{$host}->{$item}->{size}  = $bytes;
                        $data->{$host}->{$item}->{expire_time} = $expire_time;
                        $data->{$host}->{$item}->{expire_remaining} = $remaining_time;
                    }
                }
                verbose( "", 1, "\n" );
            }
        }

        $memd->disconnect_all;
    }

    return ( 1, $data );
}


sub memcached_export_key
{
    my ( $config, $option, $argv, $memcache ) = @_;

    verbose( "[ process memcached_print ] ..." );
    my $key_regex;
    if ( defined $option->{key} ) {
        $key_regex = $option->{key};
        verbose( "    key      -> $key_regex     " );
    }

    my ( $result, $data ) = search( $memcache, $key_regex );
    unless ( $result ) {
        return 0;
    }

    while ( my ( $host, $item ) = each %{$data} ) {
        while ( my ( $key, $val ) = each %{$item} ) {
            delete $data->{$host}->{$key}->{value};
        }
    }

    local $YAML::Syck::Headless        = 1;
    local $YAML::Syck::SortKeys        = 1;
    local $YAML::Syck::ImplicitUnicode = 1;
    local $YAML::Syck::ImplicitBinary  = 1;
    print YAML::Syck::Dump $data;

    return 1;
}


sub memcached_export
{
    my ( $config, $option, $argv, $memcache ) = @_;

    verbose( "[ process memcached_export ] ..." );
    my $key_regex;
    if ( defined $option->{key} ) {
        $key_regex = $option->{key};
        verbose( "    key      -> $key_regex     " );
    }

    my ( $result, $data ) = search( $memcache, $key_regex );
    unless ( $result ) {
        return 0;
    }

    local $YAML::Syck::Headless        = 1;
    local $YAML::Syck::SortKeys        = 1;
    local $YAML::Syck::ImplicitUnicode = 1;
    local $YAML::Syck::ImplicitBinary  = 1;
    print YAML::Syck::Dump $data;

    return 1;
}


sub memcached_import
{
    my ( $config, $option, $argv, $memcache ) = @_;

    verbose( "[ process memcached_import ] ..." );
    my $filename = $option->{'import'};
    verbose( "    filename -> $filename     " );

    local $YAML::Syck::Headless        = 1;
    local $YAML::Syck::SortKeys        = 1;
    local $YAML::Syck::ImplicitUnicode = 1;
    local $YAML::Syck::ImplicitBinary  = 1;
    my $data = YAML::Syck::LoadFile( $filename );

    while ( my ( $host, $item ) = each %{$data} ) {
        while ( my ( $key, $val ) = each %{$item} ) {
            verbose( "  set : $key(" . $val->{expire_time} . ") -> " . $val->{value} );
            my $result = $memcache->set( $key, $val->{value}, $val->{expire_time} );
            return 0 unless $result;
        }
    }

    return 1;
}


sub memcached_stats
{
    my ( $config, $option, $argv, $memcache ) = @_;

    verbose( "[ process memcached_stats ] ..." );

    my $data = $memcache->stats;

    local $YAML::Syck::Headless        = 1;
    local $YAML::Syck::SortKeys        = 1;
    local $YAML::Syck::ImplicitUnicode = 1;
    local $YAML::Syck::ImplicitBinary  = 1;
    print YAML::Syck::Dump $data;

    return 1;
}


sub usage
{
    my $usage = $_[0] ? "$_[0]\n" : q{};

    my $basename = $0;
    $basename =~ s{\.pl}{}xms;

    return $usage . <<"END_USAGE"

Usage:
    perl $basename.pl [options]

Options:        --help    : print usage and exit
             -v|--verbose : print message verbosely
                --config  : specify config file

             --export-key : yaml format output ( option --key is indispensable )
                 --export : yaml format output ( option --key is indispensable )
                 --import : specify yaml format input
                  --stats : print stats

                   --host : specify host (default: localhost)
                   --port : specify port (default: 11211)
              --namespace : memcache key prefix

        --key             : specify key by regex (optional)

Example:
    perl $basename.pl --export-key => export keys at localhost:11211

END_USAGE
}


sub verbose
{
    my ( $str, $level, $nl ) = @_;

    $level = 1 unless $level;
    $nl = "\n" unless $nl;

    local $| = 1;
    if ( defined $program{'option'}->{verbose} ) {
        print "$str$nl" if $program{'option'}->{verbose} >= $level;
    }
}


sub parse_program_environment
{
    my $option = parse_program_option();
    my $config = parse_program_config( $option->{config} );
    my @argv   = parse_program_argv( $config, $option );

    return ( $config, $option, \@argv );
}


sub parse_program_config
{
    my ( $filename ) = @_;
    verbose( "[ parsing program config file ] ...", 2 );

    my $config = new Config::YAML::Tiny( config => '/dev/null' );
    unless ( $filename ) {
        # try path that is changed to .conf extension .pl
        my $conf_path = __FILE__;
        $conf_path =~ s/([^\.]+?)$/conf/;
        if ( -s $conf_path ) {
            verbose( "  filename => $conf_path", 2 );
            $config->read( $conf_path );
        }
    }
    else {
        verbose( "  filename => $filename", 2 );
        if ( ! -s $filename ) {
            die "error: invaild config file path $filename";
        }
        $config->read( $filename );
    }

    return $config;
}


sub parse_program_option
{
    my $option = new Config::YAML::Tiny( config => '/dev/null' );
    GetOptions(
        $option,

        'help',               # print help and exit
        'verbose+',           # print message verbosely
        'config=s',           # specify config file

        'export-key+',        # yaml format output ( option --key is indispensable )
        'export+',            # yaml format output ( option --key is indispensable )
        'import=s',           # yaml format input
        'stats+',             # print stats

        'host=s',             # specify host (default: localhost)
        'port=i',             # specify port (default: 11211)
        'namespace=s',        # memcache key prefix

        'key=s',              # specify key by regex (optional)
    ) or die usage;

    $program{ 'option' } = $option;
    verbose( "[ parsing program config file ] ..." );

    unless ( $option->{'export-key'}
            || $option->{'export'} || $option->{'import'}
            || $option->{'stats'} ) {
        print usage() and exit;
    }

    $option->{'host'} = 'localhost' unless defined $option->{'host'};
    $option->{'port'} = '11211'     unless defined $option->{'port'};
    $option->{'namespace'} = ''     unless defined $option->{'namespace'};

    print usage() and exit if $option->get( 'help' );

    $option;
}


sub parse_program_argv
{
    my ( $config, $option ) = @_;
    verbose( "[ parsing program argv(s) ] ...", 2 );

    my @argv = @ARGV;
    for my $arg ( @argv ) {
        verbose( "  argv => $arg", 2 );
    }
    die usage() if scalar @argv != 0;

    return @argv;
}

__END__

