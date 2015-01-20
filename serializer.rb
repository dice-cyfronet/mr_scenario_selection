require 'msgpack'

class Serializer
  def self.serialize(object, output_file)
    File.open(output_file, "wb") do |file|
      # file.print Marshal::dump(object)
      file.print object.to_msgpack
    end
  end

  def self.deserialize(input_file)
    File.open(input_file, "rb") do |f|
      # Marshal::load(f.read)
      MessagePack.unpack(f.read)
    end
  end
end


