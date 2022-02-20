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
#     group => [ "message" ]
#   }
# }
#
class LogStash::Filters::Event < LogStash::Filters::Base
  config_name "event"

  # syntax: `group => [ "name of event", "name of event" ]`
  config :group, :validate => :array, :default => []

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
    # Same as logstash-input-file
    @host = Socket.gethostname.force_encoding(Encoding::UTF_8)
    @metric_groups = ThreadSafe::Cache.new { |h,k| h[k] = Metriks.meter k }
  end

  # def register

  public
  def filter(event)
    key_event = create_key event
    event.set("eq", @metric_groups.keys.include?(key_event))
    event.set("key_event", key_event)
    event.set("keys", @metric_groups.keys)

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
    @metric_groups.each_pair do |name, metric|
      flush_rates event, name, metric
      metric.clear if should_clear?
    end

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
    @group.each do |g|
      key_events << event.get(event.sprintf(g))
    end

    key_events.join(",")
  end

  def flush_rates(event, name, metric)
    event.set("event", metric.count)

    keys = name.split(",", -1)
    @group.each_with_index do |g, index|
      next if @group_ignore.length() > 0 && @group_ignore.include?(g)
      event.set(g, keys[index])
    end

  end

  def should_flush?
    @last_flush.value >= @flush_interval && !@metric_groups.empty?
  end

  def should_clear?
    @clear_interval > 0 && @last_clear.value >= @clear_interval
  end

end # class LogStash::Filters::Event
