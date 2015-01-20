require 'parallel'
require 'csv'
require './serializer.rb'

class MapReduce

  attr_accessor :sample, :maps, :reduce

  def initialize(dir = "data", sample_file="data/sample.csv", mode = :asd)
    @maps = []
    @dir = dir
    @scenarios = []
    @mode = mode
    pattern = (@mode == :bat ? '.bat' : 'scenario')
    Dir.foreach(dir) do |file|
      @scenarios << file if file.match(pattern)
    end
    @sample_file = sample_file
  end

  def run
    @sample = Load.file(@sample_file)
      start = Time.now
    @maps = Parallel.map(@scenarios, :in_processes=>1) do |scenario|
      puts scenario
      job = (@mode == :bat ? MapFromBinary.new(@dir, scenario, @sample, L1.new) : Map.new(@dir, scenario, @sample, L1.new))
      job.run;
      puts "total_delta: #{job.total_delta}"
      job
    end
      puts "calc: #{(Time.now-start)} s"
    @reduce = Reduce.new(@maps)
    @reduce.run
  end

  def self.dump(dir = "data")
    scenarios = []
    Dir.foreach(dir) do |file|
      scenarios << file if file.match('scenario')
    end
    Parallel.map(scenarios) { |scenario|
      data = Load.file(File.join(dir, scenario))
      Serializer.serialize(data, File.join(dir, "#{scenario}.bat"))
      "#{scenario}.bat"
    }
  end

end

class Map


  class << self
    @@total_delta = 0
  end


  def total_delta
    @@total_delta
  end

  attr_accessor :result, :scenario

  def initialize(scenario_dir, scenario, sample, measure = L1.new)
    @sample = sample
    @measure = measure
    @scenario = scenario
    start = Time.now
    @data = Load.file(File.join(scenario_dir, scenario))
    delta = Time.now-start
    puts "#{delta} s"
    @@total_delta += delta
  end

  def run
    range = @data.length - @sample.length
    best_value = Float::MAX
    best_index = -1
    for start_point in 0..range do
      value = 0
      for column in 0..@sample[0].length - 1 do
        value += @measure.compare(@data, @sample, start_point, column)
      end
      if (value < best_value)
        best_value = value
        best_index = start_point
      end
    end
    puts "compare_count: #{@measure.compare_count}"
    puts "compare_time: #{@measure.compare_time}"
    @result = {:index => best_index, :value => best_value, :data => @data}
  end

end

class MapFromBinary < Map


  def initialize(scenario_dir, scenario, sample, measure = L1.new)
    @sample = sample
    @measure = measure
    @scenario = scenario
    start = Time.now
    @data = Serializer.deserialize(File.join(scenario_dir, scenario))
    delta = Time.now-start
    puts "#{delta} s"
    @@total_delta += delta
  end

end

class Reduce

  attr_accessor :result

  def initialize(maps)
    @maps = maps
  end

  def run
    @result = @maps.sort_by { |job| job.result[:value] }.map { |job| {:scenario => job.scenario, :value => job.result[:value], :index => job.result[:index]} }
  end

end

class L1

  class << self
    @@compare_count = 0
    @@compare_time = 0
  end

  def compare_count
    @@compare_count
  end

  def compare_time
    @@compare_time
  end

  def compare(data, sample, start_point, column)
    start = Time.now
    l = 0;
    for i in 0..sample.length.to_i - 1 do
      l += (data[start_point + i][column] - sample[i][column]).abs
      @@compare_count += 1
    end
    @@compare_time += Time.now - start
    l
  end

end

class Load

  def self.file(name)
    matrix = []
    i = 0
    CSV.foreach(name) do |row|
      if (i>0)
        matrix << row.map { |cell| cell.to_f }
      else
        i+=1
      end
    end
    matrix
  end

  def self.dump(name)
    matrix = []
    i = 0
    CSV.foreach(name) do |row|
      if (i>0)
        matrix << row.map { |cell| cell.to_f }
      else
        i+=1
      end
    end
    matrix
  end

end
 



