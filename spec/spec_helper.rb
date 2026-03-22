$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)

require 'simplecov'

SimpleCov.start do
  add_filter 'spec'
end
SimpleCov.minimum_coverage(ENV['SNOWFLAKE_CONN_STR'].to_s.empty? ? 0 : 100)

require 'sequel-snowflake'
