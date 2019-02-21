# encoding: UTF-8
# frozen_string_literal: true
require "kvlog_retrieve/version"
require "kvlog_retrieve/parse_range"

require "set"
require "open3"
require "date"
require "active_support"
require "active_support/core_ext/date_time/calculations"
require "active_support/core_ext/date_time/conversions"
require "active_support/core_ext/range/conversions"
require "active_support/core_ext/range/include_range"
require "active_support/core_ext/range/overlaps"
require "active_support/core_ext/date/calculations"
require "active_support/core_ext/time/calculations"

class KVLogRetrieve
  ExecutableNotFoundError = Class.new(StandardError)

  class << self
    def executable(executable = "bzcat", args = "-f")
      path = %w[/usr/bin /usr/local/bin /bin /opt/bin /usr/sbin /usr/local/sbin /sbin]
      Array(path).flatten.compact.each do |p|
        executable_path = File.join(p, executable)
        return "#{executable_path} #{args} " if File.executable?(executable_path)
      end
      raise ExecutableNotFoundError
    end

    def get_cmd(file)
      if file.end_with?(".bz2")
        ["bzcat", "-f"]
      elsif file.end_with?(".gz")
        ["gunzip", "-c -f"]
      else
        ["cat", ""]
      end
    end

    def file_list(glob, regex = Regexp.new(/.*/), range = DateTime.now.prev_year..DateTime.now)
      hash = Hash.new { |h,k| h[k] = Set.new }
      # puts "# ---> " + glob.inspect
      Dir.glob(Array(glob)).each do |f|
        # puts "## ---> " + f.inspect
        if m = regex.match(f)
          # puts "### ---> " + m.inspect
          date = m[:date]
          time = m[:time]
                   .yield_self { |t| t.nil? ? "00:00:00" : t.gsub("_", ":") }
                   .yield_self { |t| t.size == 2 ? "#{t}:00:00" : t }
          dt = DateTime.parse("#{date} #{time} #{range.first.zone}")
          hash[dt] << f
        end
      end
      good = hash.sort.to_h.keys.select do |k|
        # TODO: the following isn't exactly a perfect solution
        # it seems like older files make it to the list as well.
        # No problem in outcome due to date filtering later but waste of resource.
        if k == k.beginning_of_day
          (k >= range.first.beginning_of_day) && (k <= range.last.beginning_of_day)
        else
          (k >= range.first) && (k <= range.last)
        end
      end
      unless good.first == range.first
        if good.empty?
          good.unshift(hash.keys.sort.last)
        else
          good.unshift(hash.keys.select { |k| (k < good.first) }.sort.last)
        end
      end
      good.map { |dt| hash[dt].to_a }.flatten
    end

    def kvline2hash(line)
      line = " " + line.chomp # prefix wg. regex
      line.scan(/(?:"((?:\\.|[^"])*)"|([^\s]*))=\s*(?:"((?:\\.|[^"])*)"|([^\s]*))/).map(&:compact).to_h
    end
  end

  attr_reader :range, :log_begin, :log_end, :logsource_globs, :re_date, :logger

  def initialize(log_begin:, log_end:, logsource_globs:, re_date:, logger: false)
    @log_begin = log_begin
    @log_end = log_end
    @logsource_globs = Array(logsource_globs).flatten
    @re_date = re_date
    @pr = ParseRange.new(start: @log_begin, stop: @log_end)
    @range = @pr.range
    @log_begin = @pr.start
    @log_end = @pr.stop
    @logger = logger
  end

  def get_files
    self.class.file_list(logsource_globs, re_date, range)
  end

  def get_time_regex
    regex = Array.new
    from = log_begin
    to = log_end
    while(from < to)
      regex << from.strftime("date=\"?%Y-%m-%d\"?\\s+time=\"?%H:%M")
      from +=  1.0 / 1440
    end
    tmp = regex.group_by { |a| a.split(":")[0] + ":" + a.split(":")[1][0] }.transform_values { |v| v.map { |m| m[-1] } }.map do |k,v|
      Regexp.new("#{k}[#{v.join}]:..\"?")
    end
    Regexp.union(tmp)
  end

  def read_kv(incl: [], excl: [])
    incl = (Array(get_time_regex) + Array(incl)).flatten
    read(incl: incl, excl: excl) do |line|
      yield self.class.kvline2hash(line)
    end
  end

  def read(incl: [], excl: [])
    incl = Array(incl).flatten
    excl = Array(excl).flatten
    #puts filter.inspect
    seen_files = Set.new
    get_files.each do |file|
      catch :next_file do
        if seen_files.include?(file)
          logger.info("skipping file: " + file + " already scanned") if logger
          throw :next_file
        end
        seen_files << file
        if File.zero?(file)
          logger.info("skipping empty file: " + file) if logger
          throw :next_file
        end
        logger.info("processing file: " + file) if logger
        cmd = "#{self.class.executable(*self.class.get_cmd(file))} #{file}"
        Open3.popen3(cmd) do |stdin, stdout, stderr, thread|
          stdout.each do |line|
            catch :next_line do
              begin
                incl.each do |re|
                  throw :next_line unless re.match(line)
                end
                excl.each do |re|
                  throw :next_line if re.match(line)
                end
                yield line.chomp
              rescue ArgumentError => error
                logger.info("ArgumentError -> " + error.to_s) if logger
                if error.to_s == "invalid byte sequence in UTF-8"
                  logger.info('problem processing line: "' + line.chomp + '"') if logger
                  line = line.encode("UTF-8", "binary", invalid: :replace, undef: :replace, replace: "")
                  logger.info('line cleaned and logged: "' + line.chomp + '"') if logger
                  retry
                else
                  logger.info('Error processing line: "' + line.chomp + '"') if logger
                  throw :next_line
                end
              rescue StandardError => error
                logger.info("StandardError -> " + error.inspect) if logger
                logger.info('Error processing line: "' + line.chomp + '"') if logger
                throw :next_line
              end
            end
          end
        end
      end
    end
  end
end
