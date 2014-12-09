/**
 * This file is part of Graylog2.
 *
 * Graylog2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog2.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog2.buffers;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.InstrumentedThreadFactory;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.graylog2.Configuration;
import org.graylog2.buffers.processors.OutputBufferProcessor;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.buffers.Buffer;
import org.graylog2.plugin.buffers.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.codahale.metrics.MetricRegistry.name;

@Singleton
public class OutputBuffer extends Buffer {
    private static final Logger LOG = LoggerFactory.getLogger(OutputBuffer.class);

    private final ExecutorService executor;

    private final Configuration configuration;

    private final Meter incomingMessages;

    private final OutputBufferProcessor.Factory outputBufferProcessorFactory;

    @Inject
    public OutputBuffer(OutputBufferProcessor.Factory outputBufferProcessorFactory,
                        MetricRegistry metricRegistry,
                        Configuration configuration) {
        this.outputBufferProcessorFactory = outputBufferProcessorFactory;
        this.configuration = configuration;
        this.executor = executorService(metricRegistry);

        incomingMessages = metricRegistry.meter(name(OutputBuffer.class, "incomingMessages"));
    }

    private ExecutorService executorService(final MetricRegistry metricRegistry) {
        return new InstrumentedExecutorService(Executors.newCachedThreadPool(
                threadFactory(metricRegistry)), metricRegistry);
    }

    private ThreadFactory threadFactory(MetricRegistry metricRegistry) {
        return new InstrumentedThreadFactory(
                new ThreadFactoryBuilder().setNameFormat("outputbufferprocessor-%d").build(),
                metricRegistry);
    }

    public void initialize() {
        Disruptor<MessageEvent> disruptor = new Disruptor<>(
                MessageEvent.EVENT_FACTORY,
                configuration.getRingSize(),
                executor,
                ProducerType.MULTI,
                configuration.getProcessorWaitStrategy()
        );

        LOG.info("Initialized OutputBuffer with ring size <{}> "
                        + "and wait strategy <{}>.", configuration.getRingSize(),
                configuration.getProcessorWaitStrategy().getClass().getSimpleName());

        int outputBufferProcessorCount = configuration.getOutputBufferProcessors();

        OutputBufferProcessor[] processors = new OutputBufferProcessor[outputBufferProcessorCount];

        for (int i = 0; i < outputBufferProcessorCount; i++) {
            processors[i] = outputBufferProcessorFactory.create(i, outputBufferProcessorCount);
        }

        disruptor.handleEventsWith(processors);

        ringBuffer = disruptor.start();
    }

    public void insertBlocking(Message message) {
        insert(message);
    }

    @Override
    protected void afterInsert(int n) {
        incomingMessages.mark(n);
    }
}
