# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# This example filter will replace the contents of the default
# message field with whatever you specify in the configuration.
#
# Setting the config_name here is required. This is how you
# configure this filter from your Logstash config.
#
# filter {
#   event {
#     group => {
#        "key" => "name of event"
#     }
#   }
# }
#
class LogStash::Filters::Event < LogStash::Filters::Base
  config_name "event"

  # syntax: `group => { "key" => "name of event" }`
  config :group, :validate => :array, :hash => {}

  # syntax: `ignore => [ "name of event" ]`
  # The ignore, when ignore output field
  config :group_ignore, :validate => :array, :default => []

  # The flush interval, when the metrics event is created. Must be a multiple of 1s.
  config :flush_interval, :validate => :number, :default => 5

  # The clear interval, when all counter are reset.
  #
  # If set to -1, the default value, the metrics will never be cleared.
  # Otherwise, should be a multiple of 1s.
  config :clear_interval, :validate => :number, :default => -1

  public
  def register
    require "metriks"
    require "socket"
    require "atomic"
    require "thread_safe"

    @last_flush = Atomic.new(0) # how many seconds ago the metrics where flushed.
    @last_clear = Atomic.new(0) # how many seconds ago the metrics where cleared.
    @random_key_prefix = SecureRandom.hex
    @host = Socket.gethostname.force_encoding(Encoding::UTF_8)
    @metric_groups = ThreadSafe::Cache.new { |h,k| h[k] = Metriks.meter k }
  end

  # def register

  public
  def filter(event)
    key_event = create_key event
    # puts key_event.inspect
    @metric_groups[key_event].mark
  end

  # def filter

  def flush(options = {})
    # Add 1 seconds to @last_flush and @last_clear counters
    # since this method is called every 1 seconds.
    @last_flush.update { |v| v + 5 }
    @last_clear.update { |v| v + 5 }

    # Do nothing if there's nothing to do ;)
    return unless should_flush?

    event = LogStash::Event.new
    event.set("message", @host)
    results = []
    @metric_groups.each_pair do |name, metric|
      flush_rates name, metric, results
      metric.clear if should_clear?
    end

    event.set("events", results)


    # Reset counter since metrics were flushed
    @last_flush.value = 0

    if should_clear?
      #Reset counter since metrics were cleared
      @last_clear.value = 0
      @metric_groups.clear
    end

    filter_matched(event)
    return [event]
  end

  def periodic_flush
    true
  end

  private

  def create_key(event)
    key_events = []
    @group.each do |k,v|
      key_events << "#{k}:#{event.get(event.sprintf(v))}"
    end

    key_events.join(",")
  end

  def flush_rates(name, metric, results)
    hashMap = {
      event: metric.count
    }

    name.split(",", -1).each do |kv|
      output = kv.split(":", -1)
      hashMap[output[0]] = output[1] || ""
    end

    results << hashMap
  end

  def should_flush?
    @last_flush.value >= @flush_interval && !@metric_groups.empty?
  end

  def should_clear?
    @clear_interval > 0 && @last_clear.value >= @clear_interval
  end

end # class LogStash::Filters::Event
