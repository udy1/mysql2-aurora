# frozen_string_literal: true

require 'mysql2/aurora/version'
require 'mysql2'

module Mysql2
  # mysql2 aurora module
  # @note This module patch Mysql2::Client
  module Aurora
    ORIGINAL_CLIENT_CLASS = ::Mysql2.send(:remove_const, :Client)

    # Implement client patch
    class Client
      READ_ONLY_ERRORS = %w[--read-only --super-read-only].freeze
      AURORA_CONNECTION_ERRORS = [
        'client is not connected',
        'Lost connection to MySQL server',
        "Can't connect to MySQL",
        'Server shutdown in progress'
      ].freeze

      attr_reader :client

      # Initialize class
      # @note [Override] with reconnect options
      # @param [Hash] opts Options
      # @option opts [Integer] aurora_max_retry Max retry count, when failover. (Default: 5)
      # @option opts [Bool] aurora_disconnect_on_readonly, when readonly exception hit terminate the connection (Default: false)
      def initialize(opts)
        @opts = Mysql2::Util.key_hash_as_symbols(opts)
        @max_retry = (@opts.delete(:aurora_max_retry) || 0).to_i
        @disconnect_on_readonly = @opts.delete(:aurora_disconnect_on_readonly) || false
        reconnect!
      end

      # Execute query with reconnect
      # @note [Override] with reconnect.
      def query(*args)
        try_count = 0

        begin
          client.query(*args)
        rescue Mysql2::Error => e

          # Disconnect and raise exception if read_only error
          if read_only_error?(e.message) && @disconnect_on_readonly
            warn '[mysql2-aurora] Database connection error, Aurora failover event likely occured'
            disconnect!
            raise e
          end

          # Reconnect if connection error
          if aurora_connection_error?(e.message) && (try_count <= @max_retry)
            try_count += 1
            retry_interval_seconds = [1.5 * (try_count - 1), 10].min

            warn "[mysql2-aurora] Database is readonly. Retry after #{retry_interval_seconds}seconds"
            sleep retry_interval_seconds
            reconnect!
            retry
          end

          raise e
        end
      end

      # Reconnect to database and Set `@client`
      # @note If client is not connected, Connect to database.
      def reconnect!
        query_options = (@client&.query_options&.dup || {})

        disconnect!

        @client = Mysql2::Aurora::ORIGINAL_CLIENT_CLASS.new(@opts)
        @client.query_options.merge!(query_options)
      end

      # Close connection to database server
      def disconnect!
        @client&.close
      rescue StandardError
        nil
      end

      # Check if exception message contains read only specific errors
      # @param [String] message Exception message
      # @return [Boolean]
      def read_only_error?(message)
        return false if message.nil?

        READ_ONLY_ERRORS.any? { |matching_str| message.include?(matching_str) }
      end

      # Check if exception message contains connection errors
      # @param [String] message Exception message
      # @return [Boolean]
      def aurora_connection_error?(message)
        return false if message.nil?

        AURORA_CONNECTION_ERRORS.any? { |matching_str| message.include?(matching_str) }
      end

      # Delegate method call to client.
      # @param [String] name  Method name
      # @param [Array]  args  Method arguments
      # @param [Proc]   block Method block
      def method_missing(name, *args, &block) # rubocop:disable Style/MethodMissingSuper, Style/MissingRespondToMissing
        client.public_send(name, *args, &block)
      end

      # Delegate method call to Mysql2::Client.
      # @param [String] name  Method name
      # @param [Array]  args  Method arguments
      # @param [Proc]   block Method block
      def self.method_missing(name, *args, &block) # rubocop:disable Style/MethodMissingSuper, Style/MissingRespondToMissing
        Mysql2::Aurora::ORIGINAL_CLIENT_CLASS.public_send(name, *args, &block)
      end

      # Delegate const reference to class.
      # @param [Symbol] name Const name
      def self.const_missing(name)
        Mysql2::Aurora::ORIGINAL_CLIENT_CLASS.const_get(name)
      end
    end

    # Swap Mysql2::Client
    Mysql2.const_set(:Client, Mysql2::Aurora::Client)
  end
end
