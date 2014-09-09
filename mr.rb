require 'parallel'
require 'csv'

class MapReduce
    
    attr_accessor :sample, :maps, :reduce
    
    def initialize(dir = "data", sample_file="data/sample.csv") 
	@maps = []
	@dir = dir
        @scenarios = []
	Dir.foreach(dir) do |file| 
	    @scenarios << file if file.match('scenario') 
	end
        @sample_file = sample_file
    end
    
    def run
	@sample = Load.file(@sample_file)
	@maps = Parallel.map(@scenarios) { |scenario| job = Map.new(@dir, scenario, @sample, L1.new); job.run; job }
	@reduce = Reduce.new(@maps)
        @reduce.run
    end
    
end     


class Map
    
    attr_accessor :result, :scenario

    def initialize(scenario_dir, scenario, sample, measure = L1.new)
	@sample = sample
	@measure = measure
	@scenario = scenario
	@data = Load.file(File.join(scenario_dir, scenario))
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
	@result = {:index => best_index, :value => best_value, :data => @data}
    end
    
end

class Reduce 
    
    attr_accessor :result

    def initialize(maps)
	@maps = maps
    end
    
    def run 
	@result = @maps.sort_by{ |job| job.result[:value] }.map { |job| { :scenario=> job.scenario, :value => job.result[:value], :index => job.result[:index]} } 
    end
    
end

class L1 
   
    def compare(data, sample, start_point, column)
	l = 0;
	for i in 0..sample.length.to_i - 1 do
	    l += (data[start_point + i][column] - sample[i][column]).abs 
	end
	l
    end
    
end

class Load

  def self.file(name)
    matrix = []
    i = 0
    CSV.foreach(name) do |row|
      if (i>0)
        matrix <<  row.map { |cell| cell.to_f }
      else 
        i+=1
      end
    end
    matrix
  end

end
 
