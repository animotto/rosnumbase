# frozen_string_literal: true

require 'net/http'

module Rosnumbase
  ##
  # Data downloader
  class Downloader
    SOURCE_PAGE = 'http://opendata.digital.gov.ru/registry/numeric/downloads/'
    SOURCES = {
      ABC3XX: %r{http://opendata\.digital\.gov\.ru/downloads/ABC-3xx\.csv\?\d+},
      ABC4XX: %r{http://opendata\.digital\.gov\.ru/downloads/ABC-4xx\.csv\?\d+},
      ABC8XX: %r{http://opendata\.digital\.gov\.ru/downloads/ABC-8xx\.csv\?\d+},
      DEF9XX: %r{http://opendata\.digital\.gov\.ru/downloads/DEF-9xx\.csv\?\d+}
    }.freeze

    RANGE_PIECES = 15

    def initialize(sources_prev: {})
      @callbacks = {}
      @sources = {}
      @sources_prev = sources_prev
    end

    ##
    # Callbacks
    def method_missing(method, **_args, &block)
      return unless method.start_with?('on_')

      @callbacks[method] = block
    end

    ##
    # Callbacks respond
    def respond_to_missing?(method)
      @callbacks.key?(method)
    end

    ##
    # Parses source page
    def parse_source
      uri = URI(SOURCE_PAGE)
      client = Net::HTTP.new(uri.host, uri.port)
      client.use_ssl = uri.instance_of?(URI::HTTPS)
      request = Net::HTTP::Get.new(uri.path)
      begin
        response = client.request(request)
      rescue StandardError => e
        raise DownloaderError, e
      end
      raise DownloaderError, "HTTP error (#{response.code})" unless response.is_a?(Net::HTTPSuccess)

      SOURCES.each do |name, regexp|
        match = regexp.match(response.body)
        next unless match

        @sources[name] = match[0]
      end
      @sources
    end

    ##
    # Downloads data from sources
    def download
      data = {}
      @sources.each do |source_name, source_uri|
        callback(:on_request, source_name)

        if @sources_prev[source_name] == source_uri
          callback(:on_no_updates, source_name)
          next
        end

        uri = URI(source_uri)
        client = Net::HTTP.new(uri.host, uri.port)
        client.use_ssl = uri.instance_of?(URI::HTTPS)

        request = Net::HTTP::Head.new(uri.path)
        response = http_request(client, request, source_name)
        next unless response

        content_length = response['Content-Length'].to_i
        unless response.is_a?(Net::HTTPSuccess)
          callback(:on_http_error, source_name, **{ code: response.code })
          next
        end

        data_range = download_range(client, request, content_length, source_name)
        next unless data_range

        data[source_name] = {}
        data[source_name][:uri] = source_uri
        data[source_name][:data] = data_range
        data[source_name][:data].force_encoding('UTF-8')
        callback(:on_success, source_name, **data[source_name])
      end
      data
    end

    private

    ##
    # Calls callback
    def callback(method, source, **args)
      return unless @callbacks.key?(method)

      @callbacks[method].call(source, **args)
    end

    ##
    # Does HTTP request
    def http_request(client, request, source_name)
      client.request(request)
    rescue StandardError => e
      callback(:on_request_error, source_name, **{ error: e })
      false
    end

    ##
    # Downloads with ranges
    def download_range(client, uri, length, source_name)
      data = String.new
      request = Net::HTTP::Get.new(uri.path)
      piece_length = length / RANGE_PIECES
      (length / piece_length.to_f).ceil.times do |i|
        range_start = i * piece_length
        range_end = range_start + piece_length - 1
        range_end = length if range_end > length
        request['Range'] = "bytes=#{range_start}-#{range_end}"
        response = http_request(client, request, source_name)
        unless response.is_a?(Net::HTTPSuccess)
          callback(:on_http_error, source_name, **{ code: response.code })
          return false
        end
        callback(:on_http_range, source_name, **{ data: response.body })
        return false unless response

        data << response.body
      end
      data
    end
  end

  ##
  # Downloader error
  class DownloaderError < StandardError; end
end
