#!/usr/bin/env ruby

require 'docopt'
require 'csv'
require 'faraday'
require 'json'
require 'tempfile'
require_relative 'mr.rb'

doc = <<DOCOPT
Usage:
#{__FILE__} <profile_id> <experiment_id> <dap_token>
#{__FILE__} -h | --help
DOCOPT

data_dir = "/home/bwilk/mr_scenario_selection/scenarios"
mr_exec_path = "/home/bwilk/mr_scenario_selection/mr_exec.rb"

begin
  opt = Docopt::docopt(doc)
  profile_id = opt["<profile_id>"]
  experiment_id = opt["<experiment_id>"]
  dap_token = opt["<dap_token>"]

  input_file = Tempfile.new('input')
  output_file_path = input_file.path.sub("input", "output")

  connection = Faraday.new(url: "https://dap.moc.ismop.edu.pl", ssl: {verify: false}) do |faraday|
    faraday.request :url_encoded
#    faraday.response :logger
    faraday.adapter Faraday.default_adapter
    faraday.headers['PRIVATE-TOKEN'] = dap_token
    faraday.headers['Content-Type'] = 'application/json'
  end

  response = connection.get do |req|
    req.url "/api/v1/profiles/#{profile_id}"
  end

  sensor_ids = JSON.parse(response.body)['profile']['sensor_ids'].sort

  response = connection.get do |req|
    req.url "/api/v1/measurements/"
  end

  meas = JSON.parse(response.body)['measurements']

  input = {}
  sensor_ids.each do |sensor_id|
    input[sensor_id] = (meas.select { |m| m['sensor_id'] == sensor_id}).sort {|x,y| x['timestamp'] <=> y['timestamp']}
  end

  input_file.write(sensor_ids.join(", ") + "\n")
  begin
    (input[sensor_ids[0]].length).times do |i|
      line_vals = sensor_ids.collect { |id| input[id][i]['value'] }
      input_file.write(line_vals.join(", ") + "\n")
    end
  end
  input_file.close
#save to input file

  mr = MapReduce.new(data_dir,input_file.path)
  mr.run
  
#read output, save it to dap

  input_file.unlink

  output = []
  mr.reduce.result.each do |result|
  
    result = {
        :similarity => result[:value],
        :profile_id => profile_id.to_i,
        :scenario_id => result[:scenario].scan(/\d+/).first,
        :experiment_id => experiment_id
    }

    response = connection.post do |req|
      req.url "/api/v1/results"
      req.body = {:result => result }.to_json
    end

    raise "Error while uploading results. Error code #{response.status}" unless response.status == 200

    output.push({
                    'similarity' => result[:similarity],
                    'profile_id' => result[:profile_id],
                    'experiment_id' => result[:experiment_id],
                    'scenario_id' => result[:scenario_id]
                })
  end

  File.delete(output_file_path)

  puts output

rescue Docopt::Exit => e
  puts e.message
end

