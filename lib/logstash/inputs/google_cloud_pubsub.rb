# encoding: utf-8
require 'gcloud'

require 'faraday'
module Faraday
  class Adapter
    class NetHttp < Faraday::Adapter
      def ssl_verify_mode(ssl)
        OpenSSL::SSL::VERIFY_NONE
      end
    end
  end
end

require "logstash/inputs/base"
require "logstash/namespace"

# Stream events from files from a Google Cloud PubSub subscription
class LogStash::Inputs::GoogleCloudPubSub < LogStash::Inputs::Base
  config_name "google_cloud_pubsub"

  config :project

  # Path to JSON file containing the Service Account credentials (not needed when running inside GCE)
  config :keyfile

  # The name of the PubSub topic
  config :topic, :validate => :string, :required => true

  # Autocreate the topic if it doesn't exist
  config :autocreate_topic, :validate => :boolean, :default => true

  # The name of the project that the topic is in (if it's not the current project)
  config :topic_project, :validate => :string

  # The name of the PubSub subscription. The subscription should already exist otherwise
  config :subscription, :validate => :string, :required => true, :default => 'logstash'

  # Autocreate the subscription if it doesn't exist
  config :autocreate_subscription, :validate => :boolean, :default => true

  # Maximum number of messages to pull in a single call
  config :batch_size, :validate => :number, :default => 10

  default :codec, "plain"

  public
  def register
    @logger.info("Registering Google Cloud PubSub input", :project => @project, :keyfile => @keyfile, :topic => @topic,
                 :subscription => @subscription)

    @pubsub = Gcloud.new(project=@project, keyfile=@keyfile).pubsub
    _topic = @pubsub.topic(@topic, { :autocreate => @autocreate_topic, :project => @topic_project })

    raise "Topic #{@topic} not found" if not _topic
    @logger.debug("Topic: ", :topic => _topic)

    @sub = _topic.subscription(@subscription)

    if (not @sub or not @sub.exists?)
      if @autocreate_subscription
        @logger.debug("Creating Subscription: ", :subscription => @subscription)
        @sub = _topic.subscribe(@subscription)
      else
        raise "Subscription #{@subscription} not found"
      end
    end

    @logger.debug("Subscription: ", :subscription => @sub)

  end # def register

  public
  def run(queue)
    while !stop do
      msgs = @sub.wait_for_messages max: @batch_size
      successful_messages = []

      msgs.each do |msg|
        begin
          @codec.decode(msg.data) do |event|
            msg.attributes.each_pair{|k, v| event[k] = v}
            decorate(event)
            queue << event
          end
          successful_messages.append(msg)
        rescue StandardError
          @logger.warn("Error processing message", :message => msg)
        end
      end

      @sub.acknowledge successful_messages if successful_messages.length > 0
    end
  end # def run

end # class LogStash::Inputs::GCS
