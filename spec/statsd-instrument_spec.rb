require File.join(File.dirname(__FILE__), 'helpers')
require 'sensu/extensions/statsd'
require 'statsd-instrument'
# Using datadog protocol
StatsD.backend = StatsD::Instrument::Backends::UDPBackend.new("127.0.0.1:8125", :datadog)

describe 'Sensu::Extension::StatsD' do
  include Helpers

  before do
    @extension = Sensu::Extension::StatsD.new
    @extension.settings = {
      client: {
        name: 'foo'
      },
      statsd: {
        flush_interval: 1
      }
    }
    @extension.logger = Sensu::Logger.get(log_level: :fatal)
  end

  it 'can run' do
    async_wrapper do
      @extension.safe_run do |output, status|
        expect(output).to eq('')
        expect(status).to eq(0)
        async_done
      end
    end
  end

  it 'can create graphite plaintext metrics' do
    async_wrapper do
      timer(1) do
        StatsD.increment('test1.count', 10)
        StatsD.gauge('test1.value', 20)
        StatsD.measure('test1.time', 30)
        timer(2) do
          @extension.safe_run do |output, status|
            expect(output).to match(/foo\.statsd\.gauges\.test1\.value 20\.0/)
            expect(output).to match(/foo\.statsd\.counters\.test1\.count 10/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.lower 30\.0/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.mean 30\.0/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.upper 30\.0/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.upper_90 30\.0/)
            expect(status).to eq(0)
            async_done
          end
        end
      end
    end
  end

  it 'does not support relative gauges' do
    async_wrapper do
      timer(1) do
        StatsD.gauge('tcp', +2)
        StatsD.gauge('tcp', -2)
        StatsD.gauge('tcp', -3)
        timer(2) do
          @extension.safe_run do |output, status|
            expect(output).to match(/foo\.statsd\.gauges\.tcp -3\.0/)
            expect(status).to eq(0)
            async_done
          end
        end
      end
    end
  end

  it 'can support tags' do
    async_wrapper do
      timer(1) do
        StatsD.increment('test1.count', 10, tags: { t1: 10, t2: 'value1' } )
        StatsD.gauge('test1.value', 20, tags: { t3: 10, t4: 'value2' } )
        StatsD.measure('test1.time', 30, tags: { t5: 10, t6: 'value3' } )
        timer(2) do
          @extension.safe_run do |output, status|
            expect(output).to match(/foo\.statsd\.gauges\.test1\.value 20\.0 \d+ t3:10,t4:value2/)
            expect(output).to match(/foo\.statsd\.counters\.test1\.count 10 \d+ t1:10,t2:value1/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.lower 30\.0 \d+ t5:10,t6:value3/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.mean 30\.0 \d+ t5:10,t6:value3/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.upper 30\.0 \d+ t5:10,t6:value3/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.upper_90 30\.0 \d+ t5:10,t6:value3/)
            expect(status).to eq(0)
            async_done
          end
        end
      end
    end
  end

  it 'can be grouped by tags' do
    async_wrapper do
      timer(1) do
        StatsD.increment('test1.count', 10, tags: { t1: 10, t2: 'value1' } )
        StatsD.gauge('test1.value', 20, tags: { t3: 10, t4: 'value2' } )
        StatsD.measure('test1.time', 30, tags: { t5: 10, t6: 'value3' } )
        StatsD.increment('test1.count', 10, tags: { t1: 10, t2: 'value1' } )
        StatsD.gauge('test1.value', 30, tags: { t3: 10, t4: 'value2' } )
        StatsD.measure('test1.time', 40, tags: { t5: 10, t6: 'value3' } )
        timer(2) do
          @extension.safe_run do |output, status|
            expect(output).to match(/foo\.statsd\.gauges\.test1\.value 30\.0 \d+ t3:10,t4:value2/)
            expect(output).to match(/foo\.statsd\.counters\.test1\.count 20 \d+ t1:10,t2:value1/)
            expect(output).to_not match(/foo\.statsd\.gauges\.test1\.value 20\.0 \d+ t3:10,t4:value2/)
            expect(output).to_not match(/foo\.statsd\.counters\.test1\.count 10 \d+ t1:10,t2:value1/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.lower 30\.0 \d+ t5:10,t6:value3/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.mean 35\.0 \d+ t5:10,t6:value3/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.upper 40\.0 \d+ t5:10,t6:value3/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.upper_90 40\.0 \d+ t5:10,t6:value3/)
            expect(output.split("\n").size).to eq(6)
            expect(status).to eq(0)
            async_done
          end
        end
      end
    end
  end

  it 'should be seperate if metrics are differ by tags' do
    async_wrapper do
      timer(1) do
        StatsD.increment('test1.count', 10, tags: { t1: 10, t2: 'value1' } )
        StatsD.gauge('test1.value', 20, tags: { t3: 10, t4: 'value2' } )
        StatsD.measure('test1.time', 30, tags: { t5: 10, t6: 'value3' } )
        StatsD.increment('test1.count', 10, tags: { t1: 10, t2: 'value1' } )
        StatsD.gauge('test1.value', 30, tags: { t3: 10, t4: 'value2' } )
        StatsD.measure('test1.time', 40, tags: { t5: 10, t6: 'value3' } )
        StatsD.increment('test1.count', 110, tags: { t1: 10, t2: 'value1', extratag: 1 } )
        StatsD.gauge('test1.value', 130, tags: { t3: 10, t4: 'value2', extratag: 1 } )
        StatsD.measure('test1.time', 140, tags: { t5: 10, t6: 'value3', extratag: 1 } )
        timer(2) do
          @extension.safe_run do |output, status|
            expect(output).to match(/foo\.statsd\.gauges\.test1\.value 30\.0 \d+ t3:10,t4:value2\n/)
            expect(output).to match(/foo\.statsd\.counters\.test1\.count 20 \d+ t1:10,t2:value1\n/)
            expect(output).to_not match(/foo\.statsd\.gauges\.test1\.value 20\.0 \d+ t3:10,t4:value2\n/)
            expect(output).to_not match(/foo\.statsd\.counters\.test1\.count 10 \d+ t1:10,t2:value1\n/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.lower 30\.0 \d+ t5:10,t6:value3\n/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.mean 35\.0 \d+ t5:10,t6:value3\n/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.upper 40\.0 \d+ t5:10,t6:value3\n/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.upper_90 40\.0 \d+ t5:10,t6:value3\n/)
            expect(output).to match(/foo\.statsd\.gauges\.test1\.value 130\.0 \d+ t3:10,t4:value2,extratag:1\n/)
            expect(output).to match(/foo\.statsd\.counters\.test1\.count 110 \d+ t1:10,t2:value1,extratag:1\n/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.lower 140\.0 \d+ t5:10,t6:value3,extratag:1\n/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.mean 140\.0 \d+ t5:10,t6:value3,extratag:1\n/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.upper 140\.0 \d+ t5:10,t6:value3,extratag:1\n/)
            expect(output).to match(/foo\.statsd\.timers\.test1\.time\.upper_90 140\.0 \d+ t5:10,t6:value3,extratag:1\n/)
            expect(output.split("\n").size).to eq(12)
            expect(status).to eq(0)
            async_done
          end
        end
      end
    end
  end

  it 'can support tags with other params' do
    async_wrapper do
      timer(1) do
        StatsD.increment('test1.count', 10, sample_rate: 0.9, tags: { t1: 1, t2: 2 })
        timer(2) do
          @extension.safe_run do |output, status|
            expect(output).to match(/foo\.statsd\.counters\.test1\.count 11 \d+ t1:1,t2:2/)
            expect(status).to eq(0)
            async_done
          end
        end
      end
    end
  end

end
