class Serializer 
    def serialize(object, output_file)
	File.open(output_file, "w") do |file|
	file.print Marshal::dump(object)
	end
    end

    def deserialize(input_file)
	File.open(input_file, "r") do |f|
	Marshal::load(f.read)
	end
    end
end


