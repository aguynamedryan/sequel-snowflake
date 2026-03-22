require 'spec_helper'

describe 'Shared Snowflake adapter' do
  let(:db) { Sequel.mock(host: :snowflake) }

  describe 'mock connection' do
    it 'returns :snowflake as database_type' do
      expect(db.database_type).to eq(:snowflake)
    end
  end

  describe Sequel::Snowflake::DatabaseMethods do
    it 'returns the maximum varchar size as default_string_column_size' do
      expect(db.default_string_column_size).to eq(16777216)
    end
  end

  describe Sequel::Snowflake::DatasetMethods do
    let(:ds) { db[:test_table] }

    it 'generates GROUP BY CUBE SQL' do
      sql = ds.group(:a, :b).group_cube.sql
      expect(sql).to include('GROUP BY CUBE')
    end

    it 'generates GROUP BY ROLLUP SQL' do
      sql = ds.group(:a, :b).group_rollup.sql
      expect(sql).to include('GROUP BY ROLLUP')
    end

    it 'generates GROUPING SETS SQL' do
      sql = ds.group([:a], [:b]).grouping_sets.sql
      expect(sql).to include('GROUPING SETS')
    end

    it 'supports MERGE' do
      expect(ds.supports_merge?).to be true
    end

    it 'uses :values multi-insert strategy' do
      expect(ds.send(:multi_insert_sql_strategy)).to eq(:values)
    end

    describe '#explain' do
      it 'generates EXPLAIN SQL' do
        db.fetch = [{ step: 1, description: 'scan' }]
        ds.explain
        expect(db.sqls.last).to match(/^EXPLAIN SELECT/)
      end

      it 'generates EXPLAIN USING TABULAR SQL' do
        db.fetch = [{ step: 1 }]
        ds.explain(tabular: true)
        expect(db.sqls.last).to match(/^EXPLAIN USING TABULAR SELECT/)
      end

      it 'generates EXPLAIN USING JSON SQL' do
        db.fetch = [{ step: 1 }]
        ds.explain(json: true)
        expect(db.sqls.last).to match(/^EXPLAIN USING JSON SELECT/)
      end

      it 'generates EXPLAIN USING TEXT SQL' do
        db.fetch = [{ step: 1 }]
        ds.explain(text: true)
        expect(db.sqls.last).to match(/^EXPLAIN USING TEXT SELECT/)
      end
    end
  end
end
