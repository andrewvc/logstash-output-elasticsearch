require_relative "../../../spec/es_spec_helper"
require "logstash/outputs/elasticsearch"


describe "Versioned indexing", :integration => true, :version_greater_than_equal_to_2x => true do
  require "logstash/outputs/elasticsearch"

  def get_es_output( options={} )
    settings = {
      "manage_template" => true,
      "index" => "logstash-index",
      "template_overwrite" => true,
      "hosts" => get_host_port(),
      "action" => "index",
      "script_lang" => "groovy"
    }
    LogStash::Outputs::ElasticSearch.new(settings.merge!(options))
  end

  before :each do
    @es = get_client
    # Delete all templates first.
    # Clean ES of data before we start.
    @es.indices.delete_template(:name => "*")
    # This can fail if there are no indexes, ignore failure.
    @es.indices.delete(:index => "*") rescue nil
    @es.indices.refresh
  end

  context "when index only" do

    subject(:unversioned_output) { 
      settings = {
	"manage_template" => true,
	"index" => "logstash-index",
	"template_overwrite" => true,
	"hosts" => get_host_port(),
	"action" => "index",
	"script_lang" => "groovy",
        "document_id" => "%{my_id}"
      }
      out = LogStash::Outputs::ElasticSearch.new(settings)
      out.register
      out
    }

    it "should default to ES version" do
      unversioned_output.multi_receive([LogStash::Event.new("my_id" => "123", "message" => "foo")])
      r = @es.get(:index => 'logstash-index', :type => 'logs', :id => "123", :refresh => true)
      insist { r["_version"] } == 1
      insist { r["_source"]["message"] } == 'foo'
      unversioned_output.multi_receive([LogStash::Event.new("my_id" => "123", "message" => "foobar")])
      r2 = @es.get(:index => 'logstash-index', :type => 'logs', :id => "123", :refresh => true)
      insist { r2["_version"] } == 2
      insist { r2["_source"]["message"] } == 'foobar'
    end

    subject(:versioned_output) { 
      settings = {
	"manage_template" => true,
	"index" => "logstash-index",
	"template_overwrite" => true,
	"hosts" => get_host_port(),
	"action" => "index",
	"script_lang" => "groovy",
        "document_id" => "%{my_id}",
       	"version" => "%{my_version}",
       	"version_type" => "external",
      }
      out = LogStash::Outputs::ElasticSearch.new(settings)
      out.register
      out
    }

    it "should respect the external version" do
      id = "ev1"
      versioned_output.multi_receive([LogStash::Event.new("my_id" => id, "my_version" => "99", "message" => "foo")])
      r = @es.get(:index => 'logstash-index', :type => 'logs', :id => id, :refresh => true)
      insist { r["_version"] } == 99
      insist { r["_source"]["message"] } == 'foo'
    end

    it "should ignore non-monotonic external version updates" do
      id = "ev2"
      versioned_output.multi_receive([LogStash::Event.new("my_id" => id, "my_version" => "99", "message" => "foo")])
      r = @es.get(:index => 'logstash-index', :type => 'logs', :id => id, :refresh => true)
      insist { r["_version"] } == 99
      insist { r["_source"]["message"] } == 'foo'

      versioned_output.multi_receive([LogStash::Event.new("my_id" => id, "my_version" => "98", "message" => "foo")])
      r2 = @es.get(:index => 'logstash-index', :type => 'logs', :id => id, :refresh => true)
      insist { r2["_version"] } == 99
      insist { r2["_source"]["message"] } == 'foo'
    end

    it "should commit monotonic external version updates" do
      id = "ev3"
      versioned_output.multi_receive([LogStash::Event.new("my_id" => id, "my_version" => "99", "message" => "foo")])
      r = @es.get(:index => 'logstash-index', :type => 'logs', :id => id, :refresh => true)
      insist { r["_version"] } == 99
      insist { r["_source"]["message"] } == 'foo'

      versioned_output.multi_receive([LogStash::Event.new("my_id" => id, "my_version" => "100", "message" => "foo")])
      r2 = @es.get(:index => 'logstash-index', :type => 'logs', :id => id, :refresh => true)
      insist { r2["_version"] } == 100
      insist { r2["_source"]["message"] } == 'foo'
    end
  end
end
