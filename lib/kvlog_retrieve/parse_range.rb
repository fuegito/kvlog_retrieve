# encoding: UTF-8
# frozen_string_literal: true

class KVLogRetrieve
  class ParseRange
    attr_reader :start_str, :stop_str

    def initialize(options = {})
      @start_str = options[:start]
      @stop_str = options[:stop]
      self.start = @start_str
      self.stop = @stop_str
    end

    def stop
      @stop.nil? ? @start : @stop
    end

    def stop=(datetime)
      if datetime.is_a? DateTime
        @stop = datetime
      else
        return if datetime.blank?
        if datetime == "this_week"
          @stop = DateTime.now.end_of_week
        elsif datetime == "this_month"
          @stop = DateTime.now.end_of_month
        elsif datetime == "today"
          @stop = DateTime.now.end_of_day
        elsif datetime == "yesterday"
          @stop = DateTime.now.prev_day.end_of_day
        else
          @stop = DateTime.parse(datetime).change(offset: DateTime.now.zone)
        end
      end
    end

    def start
      @start.nil? ? @stop : @start
    end

    def start=(datetime)
      if datetime.is_a?(DateTime) || datetime.is_a?(Date) || datetime.is_a?(Time)
        @start = datetime.to_datetime
      else
        raise(ArgumentError, "provide: :start => [YYYYMMDD HH:MM:SS|last_month|last_week|this_month|this_week|yesterday|today]") if datetime.nil?
        if datetime == "this_week"
          @start = DateTime.now.beginning_of_week
          @stop = DateTime.now.end_of_week
        elsif datetime == "this_month"
          @start = DateTime.now.beginning_of_month
          @stop = DateTime.now.end_of_month
        elsif datetime == "last_week"
          @start = DateTime.now.prev_week.beginning_of_week
          @stop = DateTime.now.prev_week.end_of_week
        elsif datetime == "last_month"
          @start = DateTime.now.prev_month.beginning_of_month
          @stop = DateTime.now.prev_month.end_of_month
        elsif datetime == "today"
          @start = DateTime.now.beginning_of_day
          @stop = DateTime.now.end_of_day
        elsif datetime == "yesterday"
          @start = DateTime.now.prev_day.beginning_of_day
          @stop = DateTime.now.prev_day.end_of_day
        else
          @start = DateTime.parse(datetime).change(offset: DateTime.now.zone)
        end
      end
    end

    def range
      start.nil? ? nil : start..stop 
    end
  end
end