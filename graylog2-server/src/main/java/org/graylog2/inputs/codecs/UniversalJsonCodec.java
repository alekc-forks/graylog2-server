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
package org.graylog2.inputs.codecs;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.assistedinject.Assisted;
import org.apache.commons.lang3.StringUtils;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.Tools;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.BooleanField;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.annotations.Codec;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.AbstractCodec;
import org.graylog2.plugin.inputs.codecs.MultiMessageCodec;
import org.graylog2.plugin.journal.RawMessage;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

@Codec(name = "json", displayName = "JSON")
public class UniversalJsonCodec extends AbstractCodec implements MultiMessageCodec {
    private static final Logger log = LoggerFactory.getLogger(UniversalJsonCodec.class);
    private static final String CONF_ENABLE_MULTILINE_SPLIT = "enable_multiline_split";
    private static final String CONF_ENABLE_CUSTOM_TIME_FORMAT = "enable_custom_timeformat";
    private static final String CONF_CUSTOM_TIME_FORMAT = "custom_timeformat";
    private static final String CONF_DECODE_DEPTH = "decode_depth";

    private final ObjectMapper objectMapper;
    private final boolean splitMessages;
    private final boolean enableCustomTimeFormat;
    private final String customTimeFormat;
    private final int decodeDepth;

    @Inject
    public UniversalJsonCodec(@Assisted Configuration configuration) {
        super(configuration);
        this.objectMapper = new ObjectMapper().enable(
                JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS,
                JsonParser.Feature.ALLOW_TRAILING_COMMA);
        this.splitMessages = configuration.getBoolean(CONF_ENABLE_MULTILINE_SPLIT, false);
        this.enableCustomTimeFormat = configuration.getBoolean(CONF_ENABLE_CUSTOM_TIME_FORMAT, false);
        this.customTimeFormat = configuration.getString(CONF_CUSTOM_TIME_FORMAT, "ISO_OFFSET_DATE_TIME");
        this.decodeDepth = configuration.getInt(CONF_DECODE_DEPTH, 0);
    }

    private DateTime timestampValue(final JsonNode json, final DateTime defaultTimeStamp) {
        final JsonNode value = json.path(Message.FIELD_TIMESTAMP);

        //first of all, check if we have enabled custom override for timestamp field
        if (enableCustomTimeFormat && !value.isMissingNode()) {
            final Date date;
            final DateTimeFormatter fmt;
            switch (this.customTimeFormat){
                case "ISO_LOCAL_DATE_TIME":
                    fmt = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
                    break;
                case "ISO_OFFSET_DATE_TIME":
                    fmt = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
                    break;
                case "ISO_ZONED_DATE_TIME":
                    fmt = DateTimeFormatter.ISO_ZONED_DATE_TIME;
                    break;
                case "ISO_DATE_TIME":
                    fmt = DateTimeFormatter.ISO_DATE_TIME;
                    break;
                case "ISO_INSTANT":
                    fmt = DateTimeFormatter.ISO_INSTANT;
                    break;
                case "RFC_1123_DATE_TIME":
                    fmt = DateTimeFormatter.RFC_1123_DATE_TIME;
                    break;
                default:
                    fmt = DateTimeFormatter.ofPattern(this.customTimeFormat, Locale.ENGLISH);
                    break;
            }
            try {
                LocalDateTime ldt = LocalDateTime.parse(value.asText(), fmt);
                return new DateTime(ldt);
            } catch (DateTimeParseException e) {
                log.debug("cannot parse timestamp",e);
                return defaultTimeStamp;
            }
        }

        double doubleTime = -1.0;
        if (value.isNumber()) {
            doubleTime = value.asDouble(-1.0);
        } else if (value.isTextual()) {
            try {
                doubleTime = Double.parseDouble(value.asText());
            } catch (NumberFormatException e) {
                log.debug("Unable to parse timestamp", e);
                doubleTime = -1.0;
            }
        }
        if (doubleTime > 0) {
            return Tools.dateTimeFromDouble(doubleTime);
        }
        return defaultTimeStamp;
    }

    @Nullable
    @Override
    public Message decode(@Nonnull final RawMessage rawMessage) {
        final String rawJson = new String(rawMessage.getPayload(), StandardCharsets.UTF_8);
        return decodeOne(rawJson, rawMessage);
    }

    @Nullable
    @Override
    public Collection<Message> decodeMessages(@Nonnull RawMessage rawMessage) {
        ArrayList<Message> messages = new ArrayList<>();
        final String rawJson = new String(rawMessage.getPayload(), StandardCharsets.UTF_8);

        log.info("Incoming packet" + rawJson);

        //we don't want to split, so lets attempt to decode just one single message
        if (!splitMessages) {
            messages.add(decodeOne(rawJson, rawMessage));
            return messages;
        }

        //split by \n
        for (String jsonChunk : rawJson.split("\\r?\\n")) {
            try {
                messages.add(decodeOne(jsonChunk, rawMessage));
            } catch (Exception e) {
                log.error("couldn't debug json chunk" + jsonChunk);
            } //suppress individual errors for chunks. Check what we have in resulting array  later
        }
        if (messages.isEmpty()) {
            throw new IllegalStateException("could not find any valid json in the packet");
        }
        return messages;
    }

    protected String messageValue(JsonNode jsonNode) {
        //by default return short message
        final JsonNode shortMessageNode = jsonNode.path("short_message");
        if (!shortMessageNode.isMissingNode() && shortMessageNode.isTextual()) {
            return shortMessageNode.asText();
        }

        //if short_message is not set, try to give priority to MESSAGE
        final JsonNode messageNode = jsonNode.path("message");
        if (!messageNode.isMissingNode() && messageNode.isTextual()) {
            return messageNode.asText();
        }

        // no valid message field is defined, give back a default value in hope that user
        // will fix it through the use of pipelines
        return "UNKNOWN";
    }

    protected String sourceValue(JsonNode jsonNode) {
        //if source is already set, use that as a host
        final JsonNode source = jsonNode.path("source");
        if (!source.isMissingNode() && source.isTextual()) {
            return source.asText();
        }

        //try to use host as the source
        final JsonNode host = jsonNode.path("host");
        if (!host.isMissingNode() && host.isTextual()) {
            return host.asText();
        }

        // no valid source field is defined, give back a default value in hope that user
        // will fix it through the use of pipelines
        return "UNKNOWN";
    }

    protected Message decodeOne(String json, @Nonnull final RawMessage rawMessage) {
        final JsonNode jsonNode;

        try {
            jsonNode = objectMapper.readTree(json);
            if (jsonNode == null) {
                throw new IOException("null result");
            }
        } catch (final Exception e) {
            log.error("Could not parse JSON, first 400 characters: " +
                    StringUtils.abbreviate(json, 403), e);
            throw new IllegalStateException("JSON is null/could not be parsed (invalid JSON)", e);
        }

        // Timestamp.
        final Message message = new Message(
                messageValue(jsonNode),
                sourceValue(jsonNode),
                timestampValue(jsonNode, rawMessage.getTimestamp())
        );

        // Add additional data if there is some.
        final Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        decodeJsonLevel(message, fields, "", 0);
        return message;
    }

    @SuppressWarnings("DuplicatedCode")
    private void decodeJsonLevel(Message message, final Iterator<Map.Entry<String, JsonNode>> fields, String parentKey, int level) {
        while (fields.hasNext()) {
            final Map.Entry<String, JsonNode> entry = fields.next();

            // if parentKey is not empty, then it means that we are not in the first level, prepend the key with previous
            // values
            String key = parentKey.isEmpty() ? entry.getKey() : parentKey.concat(entry.getKey());

            // We already set short_message and host as message and source. Do not add as fields again.
            if ("short_message".equals(key) || "host".equals(key)) {
                continue;
            }

            // Skip standard or already set fields.
            if (message.getField(key) != null || Message.RESERVED_FIELDS.contains(key) && !Message.RESERVED_SETTABLE_FIELDS.contains(key)) {
                continue;
            }

            // Convert JSON containers to Strings, and pick a suitable number representation.
            final JsonNode value = entry.getValue();

            final Object fieldValue;
            if (value.isContainerNode()) {
                // if we have reached the end of the admissible depth, return string, otherwise go deeper
                if (level >= decodeDepth) {
                    fieldValue = value.toString();
                } else {
                    decodeJsonLevel(message, value.fields(), key.concat("_"), level + 1);
                    continue;
                }
            } else if (value.isFloatingPointNumber()) {
                fieldValue = value.asDouble();
            } else if (value.isIntegralNumber()) {
                fieldValue = value.asLong();
            } else if (value.isNull()) {
                log.debug("Field [{}] is NULL. Skipping.", key);
                continue;
            } else if (value.isTextual()) {
                fieldValue = value.asText();
            } else {
                log.debug("Field [{}] has unknown value type. Skipping.", key);
                continue;
            }
            message.addField(key, fieldValue);
        }
    }

    @FactoryClass
    public interface Factory extends AbstractCodec.Factory<UniversalJsonCodec> {
        @Override
        UniversalJsonCodec create(Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }

    @ConfigClass
    public static class Config extends AbstractCodec.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            final ConfigurationRequest requestedConfiguration = super.getRequestedConfiguration();
            final int positionIndex = 5;
            requestedConfiguration.addField(new BooleanField(
                    CONF_ENABLE_MULTILINE_SPLIT,
                    "Split messages",
                    false,
                    "Incoming text will be split on a new line, every chunk decoded as a separate message",
                    positionIndex + 1));

            requestedConfiguration.addField(new BooleanField(
                    CONF_ENABLE_CUSTOM_TIME_FORMAT,
                    "Enable custom timestamp format",
                    false,
                    "If the timestamp is not unixtime, then enable this option to override it",
                    positionIndex + 2));

            requestedConfiguration.addField(new TextField(
                    CONF_CUSTOM_TIME_FORMAT,
                    "Custom timestamp format",
                    "ISO_OFFSET_DATE_TIME",
                    "Custom timestamp format. Must be either a name of formatter containing date and time (see https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html) or a valid Java timestamp format.",
                    ConfigurationField.Optional.NOT_OPTIONAL,
                    positionIndex + 3));

            requestedConfiguration.addField(new NumberField(
                    CONF_DECODE_DEPTH,
                    "Decode Depth",
                    0,
                    "How deep should the message be decoded. Every level will add all parent key names to the resulting key",
                    ConfigurationField.Optional.NOT_OPTIONAL,
                    positionIndex + 4));

            return requestedConfiguration;
        }
    }

    public static class Descriptor extends AbstractCodec.Descriptor {
        @Inject
        public Descriptor() {
            super(UniversalJsonCodec.class.getAnnotation(Codec.class).displayName());
        }
    }
}
