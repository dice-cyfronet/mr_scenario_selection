require 'parallel'
require 'csv'

class MapReduce
    
    attr_accessor :sample, :mapped, :reduced
    
    def initialize(dir = "data", sample_file="sample.csv") 
	@maps = []
	@scenarios = []
	@sample_file = File.join(dir,sample_file)	
	Dir.foreach(dir) do |file| 
	    @scenarios << File.join(dir,file) if file.match('scenario') 
	end
    end
    
    def run
	@sample = Load.file(@sample_file)
	@mapped = Hash.new
	mapped = Parallel.map(@scenarios) { |scenario| [ scenario, Map.new(scenario, @sample).run] }
	mapped.each { |result| @mapped[result.first] = result.last }
	@reduced = Reduce.new(@mapped).run
    end
    
end     


class Map
    
    def initialize(file_name, sample, measure = L1.new)
	@sample = sample
	@measure = measure
	@file_name = file_name
	@data = Load.file(file_name)
    end
    
    def run
	range = @data.length - @sample.length
	best_value = Float::MAX
	best_index = -1
	for start_point in 0..range do
	    for column in 0..@sample[0].length - 1 do
		value = @measure.compare(@data, @sample, start_point, column)
		if (value < best_value)
		    best_value = value
		    best_index = start_point
		end
	    end
	end    
	{:index => best_index, :value => best_value, :data => @data}
    end
    
end

class Reduce 
    
    def initialize(data)
	@data = data
    end
    
    def run 
	@data.sort_by{ |entity| entity.last[:value] }.map { |entity| { entity.first => { :value => entity.last[:value], :index => entity.last[:index]}} } 
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
   
    def self.file(name = "scenario.csv")
	 matrix = []; 
	 CSV.foreach(name) { |row| matrix <<  row.map{ |cell| cell.to_f} }
	 matrix
    end
    
end
