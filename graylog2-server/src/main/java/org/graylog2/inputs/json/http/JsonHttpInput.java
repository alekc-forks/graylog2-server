/**
 * This file is part of Graylog.
 *
 * Graylog is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog2.inputs.json.http;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.graylog2.inputs.codecs.UniversalJsonCodec;
import org.graylog2.inputs.transports.HttpTransport;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.MessageInput;

import javax.inject.Inject;

public class JsonHttpInput extends MessageInput {

    private static final String NAME = "JSON over HTTP";

    @AssistedInject
    public JsonHttpInput(MetricRegistry metricRegistry,
                         @Assisted Configuration configuration,
                         HttpTransport.Factory httpTransportFactory,
                         UniversalJsonCodec.Factory jsonCodecFactory, LocalMetricRegistry localRegistry, JsonHttpInput.Config config, JsonHttpInput.Descriptor descriptor, ServerStatus serverStatus) {
        super(metricRegistry, configuration, httpTransportFactory.create(configuration),
                localRegistry,
                jsonCodecFactory.create(configuration), config, descriptor, serverStatus);
    }

    public interface Factory extends MessageInput.Factory<JsonHttpInput> {
        @Override
        JsonHttpInput create(Configuration configuration);

        @Override
        JsonHttpInput.Config getConfig();

        @Override
        JsonHttpInput.Descriptor getDescriptor();
    }

    public static class Descriptor extends MessageInput.Descriptor {
        @Inject
        public Descriptor() {
            super(NAME, false, "");
        }
    }

    public static class Config extends MessageInput.Config {
        @Inject
        public Config(HttpTransport.Factory transport, UniversalJsonCodec.Factory codec) {
            super(transport.getConfig(), codec.getConfig());
        }
    }
}
