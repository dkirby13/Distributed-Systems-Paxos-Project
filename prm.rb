require 'socket'
require 'thread'
class Log
  def initialize(filename)
    @map = Hash.new(0)
    @file = filename.to_s
    if(filename == "ruby is stupid")
      return
    end
    x =  File.open(filename.to_s).each_line do |i|
     # if i != 0
      @map[i.split(' ').at(0).to_s] = i.split(' ').at(1).to_i
    #  end
    end
  end
  def get_file
    return @file
  end
  def get_map
    return @map
  end
  def set_map(loc, val)
    @map[loc.to_s] = val.to_s
  end
  def to_message
    message = @file.to_s + ','
    @map.each do |i,j|
      message = message + i.to_s + ' ' + j.to_s + ","
    end
    message
  end
  def from_message(message)
    x = message.split(",")
 #   puts "from message"
  #  puts message
   # puts "file: " + x[0].to_s
    @file = x[0]
    @map = Hash.new(0)
    for i in 1..x.size-1
    #  puts i.to_s + ' ' + x[i].to_s #+ ' ' + x[i].split(' ').at(0) + ' ' + x[i].split(' ').at(1)
      @map[x[i].split(' ').at(0)] = x[i].split(' ').at(1)
    end
    self
  end
end

class PRM
  def initialize(id, port, ip, cli_port)
  #  sleep(5)
    @filename = ""
    @shift = 0
    @total = 3
    @ack = 0
    @ballotnum = 0
    @cli_port = cli_port
    @logs = []
    @my_id = id
    @trying = false
    @the_log = ""
    @the_position = 0
    @my_port = port
    @my_ip = ip
    @running = true
    @incase = ''
    Thread.new {
      server = TCPServer.new(@my_ip.to_s, @port.to_i)
      loop {
        if @done
          break
        end
        client = server.accept
        process(client.gets)
        client.close
        #process_queue
      }
    }
  end
  def action_completed
    begin
      s = TCPSocket.new(@my_ip.to_s, @cli_port.to_i)
      s.puts "done"
    rescue Errno::EPIPE
      retry
    end
    @done = true
    s.close
  end
  def accepted_value  
    @trying = false
    @ack = 0
    for i in 1..3
      if i != @my_id.to_i
        s = TCPSocket.new(@other_ips[i].to_s, @other_ports[i].to_i)
        s.puts "accept " + (@my_id.to_i + @shift.to_i).to_s + ' ' + @the_position.to_s + ' '+ @the_message.to_s
        s.close
      end
    end
    @shift = @shift.to_i + 1
    action_completed
    @logs << @the_log
  end
  def process(x)
    puts "processing message: " + x.to_s
    if x.include? "resume"
      @running = true
      update
      action_completed
      return
    end
    if !@running
        #nothing
    elsif x.split(' ').at(0).to_s == "my_test"
      test(x.split(' ').at(1).to_i)
      action_completed
    elsif x.split(' ').at(0).to_s == "prepare"
      #      puts "its PREPARE!"
      if x.split(' ').at(3).to_i < @logs.size
        s = TCPSocket.new(@other_ips[x.split(' ').at(2).to_i].to_s, @other_ports[x.split(' ').at(2).to_i].to_i)
        m = "out_of_date "
        for i in x.split(' ').at(3)...@logs.size
          m = m + @logs[i].to_message + "this_is_the_end"
        end
        s.puts m
        s.close
      end
      if x.split(' ').at(1).to_i > @ballotnum
        @incase = x
        if @trying
          puts "failed"
          @failed = true
          @shift = x.split(' ').at(1).to_i
        end
        @shift = x.split(' ').at(1).to_i
        @ballotnum = x.split(' ').at(1).to_i
        s = TCPSocket.new(@other_ips[x.split(' ').at(2).to_i].to_s, @other_ports[x.split(' ').at(2).to_i].to_i)
        s.puts "ack " + @my_id.to_s
        s.close
      end
    elsif x.split(' ').at(0).to_s ==  "merge"
      merge(x.split(' ').at(1).to_i - 1, x.split(' ').at(2).to_i - 1)
     # action_completed
    elsif x.split(' ').at(0).to_s ==  "ack"
      if !@trying
        return
      end
      @ack = @ack + 1
      if @ack > @total/2
        accepted_value
      end
    elsif x.split(' ').at(0).to_s == "here"
      if x.split(' ').at(1).to_i <= @logs.size
        return
      end
      x = x.split(' ')
      x.delete_at(0)
      x.delete_at(0)
 #     x.delete_at(0)
      puts x.join(' ')
      x.join(' ').split("this_is_the_end").each do |i|
        puts i.to_s
        l = Log.new("ruby is stupid")
        l.from_message(i)
        @logs << l
      end
    elsif x.split(' ').at(0).to_s == "out_of_date"
      if x.split(' ').at(1) <= @logs.size
        return
      end
      x = x.split(' ').delete_at(0).join
      x.split("this_is_the_end").each do |i|
        l = Log.new("ruby is stupid")
        l.from_message(i)
        @logs << l
      end
     replicate(@filename)
    elsif x.split(' ').at(0).to_s == "stop"
      @running = false
      action_completed
    elsif x.split(' ').at(0).to_s == "replicate"
      replicate(x.split(' ').at(1))
    elsif x.split(' ').at(0).to_s == "total"
      # puts x.split(' ').slice(1..x.split(' ').size)
      total(x.split(' ').slice(1..x.split(' ').size))
    #  action_completed
    elsif x.split(' ').at(0).to_s == "print"
      print
   #   action_completed
    elsif x.split(' ').at(0).to_s == "done"
      @done = true
      action_completed
      return
    elsif x.split(' ').at(0).to_s == "update"
      if x.split(' ').at(1).to_i == @logs.size
        if @incase != ''
          s = TCPSocket.new(@other_ips[x.split(' ').at(2).to_i].to_s, @other_ports[x.split(' ').at(2).to_i].to_i)
          s.puts @incase
          s.close
        end
        return
      end
      m = "here " + @logs.size.to_s + ' '
      for i in x.split(' ').at(1).to_i...@logs.size
        m = m + @logs[i].to_message + "this_is_the_end"
      end
      s = TCPSocket.new(@other_ips[x.split(' ').at(2).to_i].to_s, @other_ports[x.split(' ').at(2).to_i].to_i)
      s.puts m# + ' ' + @logs.size.to_s
      s.close
    elsif x.split(' ').at(0).to_s == "accept"
      puts "trying to accept"
#      puts x.split(' ').at(1).to_s + ' ' + @ballotnum.to_s
      if x.split(' ').at(2).to_i > @logs.size
        update
        return
      end
      if x.split(' ').at(2).to_i < @logs.size
        return
      end
      if x.split(' ').at(1).to_i >= @ballotnum.to_i
        @incase = ''
        @shift = x.split(' ').at(1).to_i
        @ballotnum = @shift
        puts "accepted"
        l = Log.new("ruby is stupid")
        @logs << l.from_message(x.split(' ').slice(3..x.split(' ').size).join(' '))
        for i in 1..3
          if i != @my_id.to_i
            s = TCPSocket.new(@other_ips[i].to_s, @other_ports[i].to_i)
            s.puts x
            s.close
          end
        end
        if @failed
          @ack = 1
          puts "retrying"
          @trying = true
          @failed = false
          replicate(@filename)
        end
      else
        action_completed
      end
    end
  end

  def get_other_ips(ips, ports)
    @other_ips = ips
    @other_ports = ports
  end
  def replicate(filename)
    @filename = filename
    @the_log = Log.new(filename)
    @the_position = @logs.size
    @the_message = @the_log.to_message
    @trying = true
    @ack = 1
  #  puts "the message: " + @the_message.to_s
    for i in 1..3
      if i != @my_id.to_i
       # puts "id: " + @other_ips[i].to_s + " port: " + @other_ports[i].to_s
        s = TCPSocket.new(@other_ips[i].to_s, @other_ports[i].to_i)
        @incase =  "prepare " + (@my_id.to_i + @shift.to_i).to_s + ' ' + @my_id.to_s + ' ' + @the_position.to_s
        s.puts "prepare " + (@my_id.to_i + @shift.to_i).to_s + ' ' + @my_id.to_s + ' ' + @the_position.to_s
      end
    end
  end
  def total(positions)
    total = 0
    positions.each do |i|
  #    puts "Total position: " + i.to_s
      @logs[i.to_i - 1].get_map.each do |j, k|
     #   puts k
        total = total + k.to_i
      end
    end
    s = TCPSocket.new(@my_ip.to_s, @cli_port.to_i)
    s.puts "message " + total.to_s
    s.close
   # puts total
  end
  def test(a)
    @logs[a.to_i].each do |i, j|
      puts i.to_s + ": " + j.to_s
    end
  end
  def merge(a, b)
    total = @logs[a.to_i].get_map
    @logs[b.to_i].get_map.each do |i, j|
      total[i.to_s] =  total[i.to_s].to_i + j.to_i
    end 
    x = "merge "
    Hash[ total.sort_by { |key, val| key } ].each do |i, j|
      x = x + i.to_s + ': ' + j.to_s + ","
   end
    s = TCPSocket.new(@my_ip.to_s, @cli_port.to_i)
    s.puts x
    s.close
  end
  def my_merge(a, b)
    @logs[b].get_map.each do |i, j|
      @logs[a].set_map(i.to_s, @logs[a].get_map[i.to_s].to_i + j.to_i)
    end
    @logs.delete_at(b)
  end
  def print
    x = 'message '
    @logs.each do |i|
     # puts i.class
      x = x + i.get_file.to_s + ' '
    end
    s = TCPSocket.new(@my_ip.to_s, @cli_port.to_i)
    s.puts x
    s.close
    #puts x
  end
  def update
    for i in 1..3
      if i != @my_id.to_i
        s = TCPSocket.new(@other_ips[i].to_s, @other_ports[i].to_i)
        s.puts "update" + ' ' + @logs.size.to_s + ' ' + @my_id.to_s
        s.close
      end
    end
  end
  def run
    done = false
    puts "Starting up"
    server = TCPServer.open(@my_port)
    while !done
    #  process_queue
      client = server.accept
      message = client.gets
   #   puts "PRM#" + @my_id.to_s +  " message: " + message.to_s
      process(message)
      client.close
    end
      if done
        s = TCPSocket.new(@my_ip.to_s, @cli_port.to_i)
        s.puts "done"
        s.close
      end
  end
end

the_prm = 0
ips = ['first']
prm_ports = ['first']
File.readlines(ARGV[0].to_s).each do |line|
  ips << line.split(' ').at(1).to_s
  ip = line.split(' ').at(1).to_s
  prm_ports << line.split(' ').at(line.split(' ').size - 1)
  if line.split(' ').at(0).to_i == ARGV[1].to_i
    the_prm = PRM.new(ARGV[1], line.split(' ').at(6), ip, line.split(' ').at(2).to_i)
  end
end

the_prm.get_other_ips(ips, prm_ports)
the_prm.run
