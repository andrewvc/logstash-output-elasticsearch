require_relative "../../../spec/es_spec_helper"
require "logstash/outputs/elasticsearch"


describe "Versioned delete", :integration => true, :version_greater_than_equal_to_2x => true do
  require "logstash/outputs/elasticsearch"

  before :each do
    @es = get_client
    # Delete all templates first.
    # Clean ES of data before we start.
    @es.indices.delete_template(:name => "*")
    # This can fail if there are no indexes, ignore failure.
    @es.indices.delete(:index => "*") rescue nil
    @es.indices.refresh
  end

  context "when delete only" do

    subject(:my_output) { 
      settings = {
	"manage_template" => true,
	"index" => "logstash-delete",
	"template_overwrite" => true,
	"hosts" => get_host_port(),
        'document_id' => "%{my_id}",
       	"version" => "%{my_version}",
       	"version_type" => "external",
       	"action" => "%{my_action}"
      }
      out = LogStash::Outputs::ElasticSearch.new(settings)
      out.register
      out
    }

    it "should ignore non-monotonic external version updates" do
      id = "ev2"
      my_output.multi_receive([LogStash::Event.new("my_id" => id, "my_action" => "index", "message" => "foo", "my_version" => 99)])
      r = @es.get(:index => 'logstash-delete', :type => 'logs', :id => id, :refresh => true)
      expect(r['_version']).to eq(99)
      expect(r['_source']['message']).to eq('foo')

      my_output.multi_receive([LogStash::Event.new("my_id" => id, "my_action" => "delete", "message" => "foo", "my_version" => 98)])
      r2 = @es.get(:index => 'logstash-delete', :type => 'logs', :id => id, :refresh => true)
      expect(r2['_version']).to eq(99)
      expect(r2['_source']['message']).to eq('foo')
    end

    it "should commit monotonic external version updates" do
      id = "ev3"
      my_output.multi_receive([LogStash::Event.new("my_id" => id, "my_action" => "index", "message" => "foo", "my_version" => 99)])
      r = @es.get(:index => 'logstash-delete', :type => 'logs', :id => id, :refresh => true)
      expect(r['_version']).to eq(99)
      expect(r['_source']['message']).to eq('foo')

      my_output.multi_receive([LogStash::Event.new("my_id" => id, "my_action" => "delete", "message" => "foo", "my_version" => 100)])
      expect { @es.get(:index => 'logstash-delete', :type => 'logs', :id => id, :refresh => true) }.to raise_error(Elasticsearch::Transport::Transport::Errors::NotFound)
    end
  end
end
