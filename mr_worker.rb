#!/usr/bin/env ruby                                                                                                                                                                                                                                                            

require 'docopt'
require 'csv'
require 'faraday'
require 'json'

doc =<<DOCOPT
Usage:                                                                                                                                                                                                                                                                         
#{__FILE__} <section_id> <experiment_id> <dap_token> <dap_location> <date_from> <date_to>
#{__FILE__} -h | --help                                                                                                                                                                                                                                                        
DOCOPT

ranking = "/home/yaq/scenario_ranking/scenario_ranking"
scenario_location = "/home/yaq/scenarios/"
work_dir = "/home/yaq/scenario_ranking/work/"
input_file_location = "#{work_dir}sample.csv"
output_file_location = "#{work_dir}output.txt"

#dirs

begin
  opt = Docopt::docopt(doc)

  section_id = opt["<section_id>"]
  experiment_id = opt["<experiment_id>"]
  dap_location = opt["<dap_location>"]
  dap_token = opt["<dap_token>"]
  date_from = opt["<date_from>"]
  date_to = opt["<date_to>"]
  
  output_limit = 10

  connection = Faraday.new(url: dap_location, ssl: {verify: false}) do |faraday|
    faraday.request :url_encoded
#    faraday.response :logger
    faraday.adapter Faraday.default_adapter
    faraday.headers['PRIVATE-TOKEN'] = dap_token
    faraday.headers['Content-Type'] = 'application/json'
  end

  response = connection.get do |req|
    req.url 'api/v1/sections/'
    req.params['id'] = section_id
  end

  sensor_ids = JSON.parse(response.body)['sections'][0]['sensor_ids'].sort

  response = connection.get do |req|
    req.url "/api/v1/measurements/"
    req.params['time_from'] = date_from
    req.params['time_to'] = date_to
    req.params['sensor_id'] = sensor_ids
    # req.options.timeout = 10
  end

  meas = JSON.parse(response.body)['measurements']

  input = {}
  sensor_ids.each do |sensor_id|
    input[sensor_id] = (meas.select { |m| m['sensor_id'] == sensor_id }).sort { |x, y| x['timestamp'] <=> y['timestamp'] }
  end

  File.open(input_file_location, "w") do |input_file|
    input_file.write(sensor_ids.join(", ") + "\n")
    begin
      (input[sensor_ids[0]].length).times do |i|
        line_vals = sensor_ids.collect { |id| input[id][i]['value'] }
        input_file.write(line_vals.join(", ") + "\n")
      end
    end
  end

  #execute scenario_ranking

  IO.popen("#{ranking} #{scenario_location} #{work_dir}", "r+") do |pipe|
    output = pipe.read

    pipe.close
    $?.to_i
  end

  ranks = []
  output_file = File.open(output_file_location, "r") do |file|
    lines = file.readlines.first(10)
    ranks = lines
  end

  output = []

  ranks.each do |rank|
    rank_s = rank.split
    result = {
        :similarity => rank_s[1],
        :section_id => section_id.to_i,
        :threat_assessment_id => rank_s[0].to_i + 1,
        :experiment_id => experiment_id.to_i
    }

    response = connection.post do |req|
      req.url "/api/v1/results"
      req.body = {:result => result}.to_json
    end

    raise "Error while uploading results. Error code #{response.status}" unless response.status == 200

    output.push({
                    'similarity' => result[:similarity],
                    'section_id' => result[:section_id],
                    'threat_assessment_id' => result[:threat_assessment_id],
                    'scenario_id' => result[:scenario_id]+1
                })
  end

  puts output

rescue Docopt::Exit => e
  puts e.message
end
