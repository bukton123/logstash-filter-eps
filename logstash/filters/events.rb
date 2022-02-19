# encoding: utf-8
require "securerandom"
require "logstash/filters/base"
require "logstash/namespace"

# The event filter is useful for aggregating metrics.
#
# For example, if you have a field `response` that is
# a http response code, and you want to count each
# kind of response, you can do this:
# [source,ruby]
#     filter {
#       metrics {
#         meter => [ "http_%{response}" ]
#         add_tag => "metric"
#       }
#     }
#
# Metrics are flushed every 1 seconds by default or according to
# `flush_interval`. Metrics appear as
# new events in the event stream and go through any filters
# that occur after as well as outputs.
#
# In general, you will want to add a tag to your metrics and have an output
# explicitly look for that tag.
#
# The event that is flushed will include every 'meter' and 'timer'
# metric in the following way:
#
# ==== `meter` values
#
# For a `meter => "thing"` you will receive the following fields:
#
# * "[thing][count]" - the total count of events
#
# The default lengths of the event rate window (1, 5, and 15 minutes)
# can be configured with the `rates` option.
#
# ==== Example: Computing event rate
#
# For a simple example, let's track how many events per second are running
# through logstash:
# [source,ruby]
# ----
#     input {
#       generator {
#         type => "generated"
#       }
#     }
#
#     filter {
#       if [type] == "generated" {
#         metrics {
#           meter => "events"
#           add_tag => "metric"
#         }
#       }
#     }
#
#     output {
#       # only emit events with the 'metric' tag
#       if "metric" in [tags] {
#         stdout {
#           codec => line {
#             format => "rate: %{[events][rate_1m]}"
#           }
#         }
#       }
#     }
# ----
#
# Running the above:
# [source,ruby]
#     % bin/logstash -f example.conf
#     rate: 23721.983566819246
#     rate: 24811.395722536377
#     rate: 25875.892745934525
#     rate: 26836.42375967113
#
# We see the output includes our events' 1-minute rate.
#
# In the real world, you would emit this to graphite or another metrics store,
# like so:
# [source,ruby]
#     output {
#       graphite {
#         metrics => [ "events.rate_1m", "%{[events][rate_1m]}" ]
#       }
#     }
class LogStash::Filters::Events < LogStash::Filters::Base
  config_name "events"

  # syntax: `group => [ "name of metric group", "name of metric group" ]`
  config :group, :validate => :array, :default => []

  # The flush interval, when the metrics event is created. Must be a multiple of 1s.
  config :flush_interval, :validate => :number, :default => 1

  # The clear interval, when all counter are reset.
  #
  # If set to -1, the default value, the metrics will never be cleared.
  # Otherwise, should be a multiple of 1s.
  config :clear_interval, :validate => :number, :default => -1

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
    @metric_groups = ThreadSafe::Cache.new {}
  end

  # def register

  def filter(event)
    key_event = create_key event
    @metric_groups[key_event].mark
  end

  # def filter

  def flush(options = {})
    # Add 1 seconds to @last_flush and @last_clear counters
    # since this method is called every 1 seconds.
    @last_flush.update { |v| v + 1 }
    @last_clear.update { |v| v + 1 }

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

  # this is a temporary fix to enable periodic flushes without using the plugin config:
  #   config :periodic_flush, :validate => :boolean, :default => true
  # because this is not optional here and should not be configurable.
  # this is until we refactor the periodic_flush mechanism per
  # https://github.com/elasticsearch/logstash/issues/1839
  def periodic_flush
    true
  end

  private

  def create_key(event)
    key_events = []
    @groups.each do |g|
      key_events.append(event.sprintf(g))
    end

    key_events.join(",")
  end

  def flush_rates(event, name, metric)
    event.set("event", metric.count)
    # event.set("keys", name.split(",", -1))
    keys = name.split(",", -1)
    @groups.each do |g, index|
      event.set(g, keys[index])
    end

  end

  def should_flush?
    @last_flush.value >= @flush_interval && (!@metric_groups.empty?)
  end

  def should_clear?
    @clear_interval > 0 && @last_clear.value >= @clear_interval
  end
end

# class LogStash::Filters::Events