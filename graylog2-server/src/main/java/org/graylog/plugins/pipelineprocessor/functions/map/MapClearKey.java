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
package org.graylog.plugins.pipelineprocessor.functions.map;

import org.graylog.plugins.pipelineprocessor.EvaluationContext;
import org.graylog.plugins.pipelineprocessor.ast.functions.AbstractFunction;
import org.graylog.plugins.pipelineprocessor.ast.functions.FunctionArgs;
import org.graylog.plugins.pipelineprocessor.ast.functions.FunctionDescriptor;
import org.graylog.plugins.pipelineprocessor.ast.functions.ParameterDescriptor;

import java.util.Map;

import static org.graylog.plugins.pipelineprocessor.ast.functions.ParameterDescriptor.string;
import static org.graylog.plugins.pipelineprocessor.ast.functions.ParameterDescriptor.type;

public class MapClearKey extends AbstractFunction<Void> {

    public static final String NAME = "map_clear_key";
    private static final String VALUE = "value";
    private final ParameterDescriptor<Map, Map> mapParam;
    private final ParameterDescriptor<String, String> keyParam;

    public MapClearKey() {
        mapParam = type("map", Map.class).description("The map from which you want to remove the key").build();
        keyParam = string("key").optional().description("Key name").build();
    }

    @Override
    public Void evaluate(FunctionArgs args, EvaluationContext context) {
        //noinspection unchecked
        final Map<String, Object> map = mapParam.required(args, context);
        Object key = keyParam.required(args, context);

        if (map == null) {
            return null;
        }
        map.remove(key);
        return null;
    }

    @Override
    public FunctionDescriptor<Void> descriptor() {
        return FunctionDescriptor.<Void>builder()
                .name(NAME)
                .description("Clear (remove) a key in the map.")
                .params(mapParam, keyParam)
                .returnType(Void.class)
                .build();
    }
}

