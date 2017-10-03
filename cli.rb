require 'socket'
require 'thread'
class CLI
  def initialize(port, ip, ports)
    @local_ports = ports
    @port = port
    @ip = ip
    @server = TCPServer.new(@ip.to_s, @port.to_i)
  end
  def send_map(file)
    i = File.read(file).split('')
  #  puts i
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
    s1.puts file.to_s + " 0 " + " " + num.to_s 
    s2.puts file.to_s + ' ' + (num + 1).to_s + " " + i.size.to_s
    s1.close
    s2.close
    puts "Waiting for the Mappers"
    #  server = TCPServer.new(@ip.to_s, @port.to_i)
    client = @server.accept
    puts "One is Done"
    client = @server.accept
    puts "Two is Done"
    client.close
  end
  def send_reduce(files)
    #   puts "files: " 
    #   puts files
    x = ''
    files.each do |i|
      x = x + i + ' '
    end
    #   puts "x: " + x
    s = TCPSocket.new(@ip, @local_ports[3].to_i)
    s.puts x
    s.close
    puts "Waiting for Reducer"
    #    server = TCPServer.new(@ip.to_s, @port.to_i)
    client = @server.accept
    puts "It's done"
    client.close
  end
  def finish
    s1 = TCPSocket.new(@ip.to_s, @local_ports[1].to_i)
    s2 = TCPSocket.new(@ip.to_s, @local_ports[2].to_i)
    s3 = TCPSocket.new(@ip.to_s, @local_ports[3].to_i)
    s4 = TCPSocket.new(@ip.to_s, @local_ports[4].to_i)
    s1.puts "done"
    s2.puts "done"
    s3.puts "done"
    s4.puts "done"
    s1.close
    s2.close
    s3.close
    s4.close
  end
  def run
    error = "Please Enter a Command"
    again = "y"
    begin
      puts error
      command = STDIN.gets.chomp
      #  puts "command: " + command.to_s
      # sleep(4)
      if command.split(' ').at(0).include? "map"
        send_map(command.split(' ').at(1))
      elsif command.split(' ').at(0).include? "reduce"
        send_reduce(command.split(' ').slice(1..command.size))
      else #command.include? "replicate"
        s = TCPSocket.new(@ip.to_s, @local_ports[4].to_i)
        s.puts command
        s.close
        s = @server.accept
        m = s.gets
        if m.split(' ').at(0) == "merge"
          m = m.split(' ')
          m.delete_at(0)
          m = m.join(' ').split(",")
          m.each do |i|
            puts i.split(' ').at(0).to_s + ": " + i.split(' ').at(1).to_s
          end
        elsif m.include? "message"
          m = m.split(' ')
          m.delete_at(0)
          puts m.join(' ')
        else
          puts m
          puts "the prm is done"
        end
        #  else 
        #   puts "That is not a valid command"
        s.close
      end
      puts "Do you want to enter another command(y/n)"
      again = STDIN.gets.chomp
    end while again.downcase == 'y'
    finish
  end
end

class Mapper
  def initialize(port, ip, cli_port, id)
    @id = id
    @cli_port = cli_port
    @ip = ip
    @port = port
    @map = Hash.new(0)
   # puts "ip = " + @ip.to_s  + " port = " + @port.to_s
  end
  def run
    done = false
    server = TCPServer.new(@ip.to_s, @port.to_i)
    while !done
      client = server.accept
      message = client.gets
      puts "mapper " + @id.to_s + " message: " + message.to_s
      if message.include? "done"
        done = true
        client.close
        return
      else
        map(message.split(' ').at(0), message.split(' ').at(1), message.split(' ').at(2))
        client.close
      end
      #client.close
    end
  end
  def map(filename, offset, size)
   # puts "file: " + filename.to_s + " offset: " + offset.to_s + " size: " + size.to_S
    x =  File.open(filename).read.gsub(/[^A-Za-z0-9\s]/, '').split('').slice(offset.to_i..size.to_i).join.split(' ')
    x.each do|i|
      @map[i] = @map[i] + 1
    end
    output = File.open(filename + "_I_" + @id.to_s, "w")
    @map.each do |i, j|
      output << i + " " + j.to_s + "\n"
    end
    s = TCPSocket.new(@ip.to_s, @cli_port.to_i)
    s.puts "done"
    s.close
    output.close
  end
end

class Reducer
  def initialize(port, ip, cli_port)
    @cli_port = cli_port
    @port = port
    @map = Hash.new(0)
    @ip = ip
  end
  def run
    done = false
    server = TCPServer.new(@ip.to_s, @port.to_i)
    while !done
      client = server.accept
      message = client.gets
      puts "reducer " + " message: " + message.to_s
      if message.include? "done"
        done = true
        client.close
        return
      else
        reduce(message.split(' '))
        client.close
      end
    end
  end
  def reduce(files)
    files.each do |i|
      puts i
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
the_map_1 = 0
the_map_2 = 0
the_reducer = 0
the_cli = 0

config = ARGV[0]
File.readlines(ARGV[0].to_s).each do |line|
  #  puts line
  if line.split(' ').at(0).to_i == ARGV[1].to_i
    # puts "winning"
    ports = line.split(' ').slice(2, line.split(' ').size)
    ip = line.split(' ').at(1).to_s
    the_cli = CLI.new(line.split(' ').at(2).to_i, ip, ports)
    the_map_1 = Mapper.new(line.split(' ').at(3).to_i, ip, line.split(' ').at(2).to_i, 1)
    the_map_2 = Mapper.new(line.split(' ').at(4), ip, line.split(' ').at(2).to_i, 2)
    the_reducer = Reducer.new(line.split(' ').at(5), ip, line.split(' ').at(2).to_i)
    #  the_prm = PRM.new(ARGV[1], line.split(' ').at(6), ip, line.split(' ').at(2).to_i)
  end
end
map1 = fork {the_map_1.run}
map2 = fork {the_map_2.run}
reducer = fork {the_reducer.run}
#prm = fork {the_prm.run}
the_cli.run
