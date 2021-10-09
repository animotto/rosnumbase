# frozen_string_literal: true

require 'sqlite3'

module Rosnumbase
  ##
  # Database
  class Database
    DEFAULT_FILENAME = File.join(Dir.home, '.rosnumbase.db')

    def initialize(filename: DEFAULT_FILENAME)
      @db = SQLite3::Database.new(filename)
      @db.results_as_hash = true
    end

    ##
    # Creates tables
    def init_db
      query = <<~SQL
        CREATE TABLE IF NOT EXISTS `registry` (
          `id` INTEGER PRIMARY KEY AUTOINCREMENT,
          `source` TEXT,
          `code` INTEGER,
          `from` INTEGER,
          `to` INTEGER,
          `capacity` INTEGER,
          `operator` TEXT,
          `region` TEXT
        );
        CREATE TABLE IF NOT EXISTS `sources` (
          `source` TEXT PRIMARY KEY,
          `uri` TEXT
        );
        CREATE INDEX IF NOT EXISTS `registry_code`
        ON `registry` (`code`);
        CREATE INDEX IF NOT EXISTS `registry_operator`
        ON `registry` (`operator`);
        CREATE INDEX IF NOT EXISTS `registry_region`
        ON `registry` (`region`);
      SQL
      @db.execute_batch(query)
    end

    ##
    # Starts transaction
    def transaction
      @db.transaction
    end

    ##
    # Commits transaction
    def commit
      @db.commit
    end

    ##
    # Returns true if tables exist
    def tables_exist?
      !@db.table_info('registry').empty? && !@db.table_info('sources').empty?
    end

    ##
    # Adds a record to the registry
    def add_record(source, code, from, to, capacity, operator, region)
      query = <<~SQL
        INSERT INTO `registry` (
          `source`,
          `code`,
          `from`,
          `to`,
          `capacity`,
          `operator`,
          `region`
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?
        )
      SQL
      @db.execute(
        query,
        source.to_s,
        code.to_i,
        from.to_i,
        to.to_i,
        capacity.to_i,
        operator,
        region
      )
    end

    ##
    # Flushes all records for the source
    def flush_records(source)
      query = <<~SQL
        DELETE FROM `registry`
        WHERE `source` = ?
      SQL
      @db.execute(query, source.to_s)
    end

    ##
    # Finds a number
    def find_record(code, number)
      query = <<~SQL
        SELECT * FROM `registry`
        WHERE `code` = ? AND `from` <= ? AND `to` >= ?
      SQL
      result = @db.execute(query, code, number, number)
      data = {}
      unless result.empty?
        data = {
          source: result[0]['source'],
          code: result[0]['code'],
          from: result[0]['from'],
          to: result[0]['to'],
          capacity: result[0]['capacity'],
          operator: result[0]['operator'],
          region: result[0]['region']
        }
      end
      data
    end

    ##
    # Adds or updates source
    def add_source(source, uri)
      query = <<~SQL
        INSERT INTO `sources` VALUES (
          ?, ?
        ) ON CONFLICT(`source`)
        DO UPDATE SET `uri` = ?
      SQL
      @db.execute(query, source.to_s, uri, uri)
    end

    ##
    # Returns an array of sources
    def sources
      query = <<~SQL
        SELECT * FROM `sources`
      SQL
      result = @db.execute(query)
      sources = {}
      result.each { |row| sources[row['source'].to_sym] = row['uri'] }
      sources
    end

    ##
    # Returns an array of operators
    def operators
      query = <<~SQL
        SELECT DISTINCT `operator`
        FROM `registry`
        ORDER BY `operator` ASC
      SQL
      result = @db.execute(query)
      result.map { |row| row['operator'] }
    end

    ##
    # Returns an array of regions
    def regions
      query = <<~SQL
        SELECT DISTINCT `region`
        FROM `registry`
        ORDER BY `region` ASC
      SQL
      result = @db.execute(query)
      result.map { |row| row['region'] }
    end
  end
end
