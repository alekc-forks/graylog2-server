package org.graylog.storage.elasticsearch6;

import org.graylog2.indexer.indices.IndicesAdapter;
import org.graylog2.indexer.indices.IndicesIT;
import org.graylog2.shared.bindings.providers.ObjectMapperProvider;

public class IndicesES6IT extends IndicesIT {
    @Override
    protected IndicesAdapter indicesAdapter() {
        return new IndicesAdapterES6(jestClient(),
                new ObjectMapperProvider().get(),
                new IndexingHelper());
    }
}
