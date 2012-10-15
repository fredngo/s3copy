#!/usr/bin/env ruby

# s3copy
#
# original s3nuke by: Stephen Eley (sfeley@gmail.com)
# improved s3nuke by: Robert LaThanh
# modified to s3copy by: Fred Ngo
#
# A script to copy Amazon S3 buckets with many objects (millions) quickly by
# using multiple threads to retrieve and copy the individual objects.
#
# http://github.com/fredngo/s3copy
#
# Licensed under the Apache License v2.0 (http://apache.org/licenses)

require 'rubygems'
require 'optparse'
require 'logger'
require 'thread'
begin
  require 'right_aws'
rescue LoadError
  puts "Missing the RightAWS gem! Try: sudo gem install right_aws"
end

access_key = nil
secret_key = nil

thread_count = 1
max_queue = 10 * 1000
STDOUT.sync = true

bucket_from = nil
bucket_to   = nil

# Parse the command line
begin
  opts = OptionParser.new
  opts.banner = "Usage: #{$PROGRAM_NAME} [options] bucket_from bucket_to"
  opts.separator ''
  opts.separator 'Options:'
  opts.on('-a',
          '--access ACCESS',
          String,
          'Amazon Access Key (required)') {|key| access_key = key}
  opts.on('-s',
          '--secret SECRET',
          String,
          'Amazon Secret Key (required)') {|key| secret_key = key}
  opts.on('-t',
          '--threads COUNT',
          Integer,
          "Number of simultaneous threads (default #{thread_count})") do |val|
            thread_count = val
            max_queue = val * 1000
          end
  opts.on('-h', '--help', 'Show this message') do
    puts opts
    exit
  end
  opts.separator ''
  # opts.separator 'If the --access and --secret options are not used, the environment variables'
  # opts.separator 'AMAZON_ACCESS_KEY_ID and AMAZON_SECRET_ACCESS_KEY must be set.'
  opts.separator ''
  buckets = opts.parse(ARGV)

  if buckets.empty?
    puts "You must specify bucket_from and bucket_to!"
    puts opts
    exit
  end
  bucket_from = buckets[0]
  bucket_to   = buckets[1]
  
  unless access_key and secret_key
    puts "The --access and --secret options are required!"
    puts opts
    exit
  end
rescue OptionParser::ParseError
  puts "Oops... #{$!}"
  puts opts
  exit
end

# Make a connection for bucket deletion
log = Logger.new(STDOUT)
log.level = Logger::ERROR
mains3 = RightAws::S3Interface.new(access_key, secret_key, :multi_thread => true, :port => 80, :protocol => 'http', :logger => log)
begin
  mains3.list_all_my_buckets  # Confirm credentials
rescue RightAws::AwsError => e
  puts e.message
  puts opts
  exit
end

puts "START: Copying from #{bucket_from} to #{bucket_to}"

# Thread management
threads = []
queue = Queue.new
mutex_total = Mutex.new

# Tracking variables
total_listed = 0
total_copied = 0

# Key retrieval thread
threads << Thread.new do
  Thread.current[:number] = "CATALOG"
  puts "Starting catalog thread...\n"
  s3 = RightAws::S3Interface.new(access_key, secret_key, :multi_thread => true, :port => 80, :protocol => 'http', :logger => log)
  prefix = ''
  begin
    while queue.length > max_queue
      sleep 1
    end

    keys = s3.list_bucket(bucket_from, { :marker => prefix, :'max-keys' => 1000})
    
    prefix = keys.last[:key] unless keys.empty?
    keys.each do |key|
      queue.enq(key[:key])
      total_listed += 1
    end
  end until keys.empty?
  thread_count.times {queue.enq(:END_OF_BUCKET)}
end

start_time = Time.now
thread_count.times do |count|
  threads << Thread.new(count) do |number|
    Thread.current[:number] = number
    puts "Starting copying thread #{number}...\n"
    
    s3 = RightAws::S3Interface.new(access_key, secret_key, :multi_thread => true, :port => 80, :protocol => 'http', :logger => log)
    begin
      key = queue.deq
      unless key == :END_OF_BUCKET
        
        acl = s3.get_acl(bucket_from, key)[:object]
        s3.copy(bucket_from, key, bucket_to, key)
        s3.put_acl(bucket_to, key, acl)

        mutex_total.synchronize {total_copied += 1}
        if (total_copied %100 == 0)
          elapsed = Time.now - start_time
          puts "#{elapsed}: Copied #{total_copied} out of #{total_listed}"
        end
      end
    end until (key == :END_OF_BUCKET)
  end
end

threads.each do |t|
  begin
    t.join
  rescue RuntimeError => e
    puts "Failure on thread #{t[:number]}: #{e.message}"
  end
end

# Clean up any stragglers and kill the bucket
puts "END: Copied from #{bucket_from} to #{bucket_to}!"
puts
