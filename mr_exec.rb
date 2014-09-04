#!/usr/bin/env ruby 

require 'docopt'
require 'pp'
load 'mr.rb'

doc = <<DOCOPT
Usage:
#{__FILE__} <scenario_dir> <sample_file> [ -n <number_of_ouputs> | -o <output_file> ]
#{__FILE__} -h | --help 
DOCOPT

begin

  opt = Docopt::docopt(doc)

  num = Integer(opt["<number_of_ouputs>"] || 10 )
  output = opt["<output_file>"] || "output.csv" 

  mr = MapReduce.new(opt["<scenario_dir>"],opt["<sample_file>"])
  mr.run

  File.open(output, 'w') do |file|
     i=0; 
     mr.reduce.result.each do |result| 
       i+=1; 
       file.write("#{result[:scenario]}, #{result[:index]}, #{result[:value]}\n"); 
       break if i>num 
     end
  end

rescue Docopt::Exit => e
  puts e.message
end


