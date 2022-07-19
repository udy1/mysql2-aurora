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
      READ_ONLY_ERROR = '--read-only'

      attr_reader :client

      # Initialize class
      # @note [Override] with reconnect options
      # @param [Hash] opts Options
      # @option opts [Bool] aurora_reconnect_on_readonly, when readonly exception hit terminate the connection (Default: false)
      def initialize(opts)
        @opts = Mysql2::Util.key_hash_as_symbols(opts)
        @reconnect_on_readonly = @opts.delete(:aurora_reconnect_on_readonly) || false
        reconnect!
      end

      # Execute query with reconnect
      # @note [Override] with reconnect.
      # Reconnect and raise exception if read_only error
      def query(*args)
        client.query(*args)
      rescue Mysql2::Error => e
        if read_only_error?(e.message) && @reconnect_on_readonly
          warn '[mysql2-aurora] Database read-only error, Aurora failover event likely occured'
          reconnect!
        end

        raise e
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
        message&.include?(READ_ONLY_ERROR) || false
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
