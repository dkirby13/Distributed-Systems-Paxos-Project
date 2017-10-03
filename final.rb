#!usr/local/bin/ruby/ -w
#arn:aws:s3:::dominicfirstbucket
require 'socket'
require 'thread'


class Log
  def initialize(filename)
    if(filename == "ruby is stupid")
      return
    @map = Hash.new(0)
    @file = filename
    x =  File.open(filename).gets.gsub(/[^a-z0-9\s]/, '').split(' ')
    x.for_each do|i|
      @map[i] = @map[i] + 1
    end
  end
  def get_file
    return @file
  end
  def get_map
    return @map
  end
  def to_message
    message = filename + '\n'
    @map.each do |i,j|
      message = message + i.to_s + ' ' + j.to_s + '\n'
    end
  end
  def from_message(message)
    x = message.split('\n')
    @file = x[0]
    @map = Hash.new(0)
    for i in 1..x.size
      @map[x[i].split(' ').at(0)] = x[i].split(' ').at(1)
  end
end
class CLI
  def initialize(port, ip, ports)
    @local_ports = ports
    @port = port
    @ip = ip
  end
  def send_map(file)
    i = File.open(file).gets.split('')
    num = i.size/2
    x = 0
    while i[num] != ' '
      if i[num + x] == ' '
        num = num + x
      elsif i[num - x] == ' '
        num = num - x
      else
        x = x + 1
      end
    end
    s1 = TCPSocket.new(@ip.to_s, @local_ports[1].to_i)
    s2 = TCPSocket.new(@ip.to_s, @local_ports[2].to_i)
    s1.puts file + " 0 " + " " + num.to_s 
    s2.puts file + ' ' + (num + 1_.to_s + " " + i.size_to_s
    s1.close
    s2.close
    puts "Waiting for the Mappers"
    server = TCPServer.open(@port.to_i)
    client = server.accept
    puts "Got One"
    client = server.accept
    puts "Got Both"
  end
  def send_reduce(files)
    x = ''
    files.each do |i|
      x = i + ' '
    end
    s = TCPSocket.new(@ip, @local_ports[3].to_i)
    s.puts x
    s.close
    puts "Waiting for Reducer"
    server = TCPServer.open(@port.to_i)
    client = server.accept
    puts "Its done"
  end

  def run
    error = "Please Enter a Command"
    again = "y"
    begin
      command = gets
      sleep(4)
      if command.include? "map"
        send_map(command.split(' ').at(1))
      elsif command.include? "reduce"
        send_reduce(command.split(' ').slice(1..command.size))
      else command.include? "replicate"
        s = TCPSocket.new(@ip.to_s, @local_ports[4].to_i)
        s.puts command
        s.close
      #elsif command.include? "stop"
      #  TCPSocket.new(@ip.to_s, @local_ports[4].to_i).puts command
     # elsif command.include? "resume"
      #  TCPSocket.new(@ip.to_s, @local_ports[4].to_i).puts command
     # elsif command.include? "total"
      #  TCPSocket.new(@ip.to_s, @local_ports[4].to_i).puts command
     # elsif command.include? "print"
       # TCPSocket.new(@ip.to_s, @local_ports[4].to_i).puts command
      #elsif command.include? "merge"
     #   TCPSocket.new(@ip.to_s, @local_ports[4].to_i).puts command
   #   else
    #    puts error
      end
      puts "Do you want to enter another command(y/n)"
      again = gets
    end while again.downcase == 'y'
  end
end

class Mapper
  def initialize(port, id, cli_port)
    @cli_port = cli_port
    @id = id
    @port = port
    @map = Hash.new(0)
  end
  def run
    done = false
    server = TCPServer.open(@port)
    while !done
      client = server.accept
      message = client.gets
      puts "mapper " + id.to_s + " message: " + message.to_s
      if message.include? "done"
        done = true
      else
        map(message.split(' ').at(0), message.split(' ').at(1), message.split(' ').at(2))
      end
    end
  end
  def map(filename, offset, size)
    x =  File.open(filename).gets.gsub(/[^a-z0-9\s]/, '').split('').slice(offset.to_i..size.to_i).join.split(' ')
    x.for_each do|i|
      @map[i] = @map[i] + 1
    end
    output = File.open(filename + "_I_" + id.to_s, "w")
    @map.each do |i, j|
      output << i + " " + j.to_s + '\n'
    end
    s = TCPSocket.new(@ip.to_s, @cli_port.to_i)
    s.puts "done"
    s.close
    output.close
  end
end

class Reducer
  def initialize(port, cli_port)
    @cli_port = cli_port
    @port = port
    @map = Hash.new(0)
  end
  def run
    done = false
    server = TCPServer.open(@port)
    while !done
      client = server.accept
      message = client.gets
      puts "reducer " + " message: " + message.to_s
      if message.include? "done"
        done = true
      else
       reduce(message.split(' '))
      end
    end
   end
  def reduce(files)
    files.each do |i|
      File.open(i).readlines.each do |line|
        @map[line.split(' ').at(0)] = @map[line.split(' ').at(0)].to_i + line.split(' ').at(1).to_i
      end
    end
    output = File.open(files[0].to_s + "_reduced", "w")
    @map.each do |i, j|
      output << i.to_s + " " + j.to_s + "\n"
    end
    s = TCPSocket.new(@ip.to_s, @cli_port.to_i)
    s.puts "done"
    s.close
    output.close
  end
end
class PRM
  def initialize(id, port, ip, cli_port)
    @filename = ""
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
    @queue = Queue.new
    Thread.new {
      server = TCPServer.new(@port.to_i)
      loop {
        client = server.accept
        @queue << client.gets
        client.close
      }
    }
  end
  def accepted_value
    for i in 1..3
      s = TCPSocket.new(@other_ips[i].to_s. @other_ports[i].to_i)
      s.puts "accept " + @the_message + " " + @the_position.to_s + ' ' + @my_id.to_s
    end
  end
  def process_queue
    if @queue.size == 0
      return
    end
    x = @queue.pop
    if x.include "prepare"
      if x.split(' ').at(1).to_i >= @ballotnum
        if @trying
          @failed = true
        end
        @ballotnum = x.split(' ').at(1).to_i
        s = TCPSocket.new(@other_ips[@ballotnum].to_s, @other_ports[@ballotnum].to_i)
        s.puts "ack"
        s.close
      elsif x.include? "ack"
        @ack = @ack + 1
        if @ack > @total/2
          accepted_value
        end
      elsif x.include? "here"
        for i in 0...x.split(' ').size
          l = Log.new("ruby is stupid")
          l.from_message(x.split(' ').at(i))
          @logs << l
        end
      end
      elsif x.include? "update"
        m = ''
        for i in x.split(' ').at(1)..@logs.size
          m = m + @logs[i].to_message + ' '
        end
        s = TCPSocket.new(@other_ips[x.split(' ').at(2).to_i].to_s, @other_ports[x.split(' ').at(2).to_i].to_i)
        s.puts m + " here"
        s.close
      elsif x.include? "accept"
        if x.split(' ').at(3).to_i >= @ballotnum
          l = Log.new("ruby is stupid")
          @logs[x.split(' ').at(2).to_i] = l.from_message(x.split(' ').at(1))
          if @failed
            replicate(@filename)
            @trying = true
            @failed = false
          end
        end
    else
        l = Log.new("ruby is stupid")
      @logs[x.split(' ').at(2).to_i] = l.from_message(x.split(' ').at(1))
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
    @the_message = the_log.to_message
    for i in 1..3
      if i != @my_id
        s = TCPSocket.new(@other_ips[i].to_s, @other_ports[i].to_i)
        s.puts "prepare " + @my_id.to_s 
      end
    end
  end
  def total(positions)
    total = 0
    positions.each do |i|
      total = total + @logs[i.to_i].get_map.size
    end
    puts total
  end

  def print
    x = ''
    @logs.each do |i|
      x = x + i.get_file.to_s + ' '
    end
    puts x
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
    server = TCPServer.open(@my_port)
    while !done
      process_queue
      client = server.accept
      message = client.gets
      puts "PRM#" + @my_id.to_s +  " message: " + message.to_s
      if message.include "resume"
        @running = true
        update
      end
      if !@running
        #nothin
      elsif message.include "stop"
        @running = false
      elsif message.include "replicate"
        replicate(message.partition(' ').at(1))
      elsif message.include "total"
        total(message.split(' ').delete(0))
      elsif message.include "print"
        print
      else
        @done = true
      end
      if !@done
        s = TCPSocket.new(@my_ip.to_s, @cli_port.to_i)
        s.puts "done"
        s.close
      end
    end
  end
end

config = ARGV[0]
ips = ['first']
prm_ports = ['first']
File.readlines(ARGV[0].to_s).each do |line|
  ips << line.split(' ').at(0).to_s
  prm_ports << line.split(' ').at(line.split(' ').size)
  if line.split(' ').at(0).to_i == ARGV[1].to_i
    ports = line.split(' ').slice(2, lines.split(' ').size)
    ip = line.split(' ').at(1).to_s
    the_cli = CLI.new(line.split(' ').at(2).to_i, ip, ports)
    the_map_1 = Mapper.new(line.split(' ').at(3).to_i, ip, line.split(' ').at(2).to_i)
    the_map_2 = Mapper.new(line.split(' ').at(4), ip, line.split(' ').at(2).to_i)
    the_reducer = Reducer.new(line.split(' ').at(5), ip, line.split(' ').at(2).to_i)
    the_prm = PRM.new(ARGV[1], line.split(' ').at(6), ip, line.split(' ').at(2).to_i)
  end
end

the_prm.get_other_ips_and_ports(ips, prm_ports)

map1 = fork {the_map_1.run}
map2 = fork {the_map_2.run}
reducer = fork {the_reducer.run}
prm = fork {the_prm.run}
the_cli.run
