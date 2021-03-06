package org.graylog.storage.elasticsearch6;

import io.searchbox.core.Search;
import io.searchbox.core.search.aggregation.CardinalityAggregation;
import io.searchbox.core.search.aggregation.ExtendedStatsAggregation;
import io.searchbox.core.search.aggregation.ValueCountAggregation;
import io.searchbox.params.Parameters;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.graylog2.Configuration;
import org.graylog2.indexer.IndexMapping;
import org.graylog2.indexer.ranges.IndexRange;
import org.graylog2.indexer.results.CountResult;
import org.graylog2.indexer.results.FieldStatsResult;
import org.graylog2.indexer.results.ResultMessage;
import org.graylog2.indexer.results.ScrollResult;
import org.graylog2.indexer.results.SearchResult;
import org.graylog2.indexer.searches.ScrollCommand;
import org.graylog2.indexer.searches.SearchesAdapter;
import org.graylog2.indexer.searches.SearchesConfig;
import org.graylog2.indexer.searches.Sorting;
import org.graylog2.plugin.indexer.searches.timeranges.TimeRange;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;

public class SearchesAdapterES6 implements SearchesAdapter {
    private static final String DEFAULT_SCROLLTIME = "1m";
    private static final String AGG_CARDINALITY = "gl2_field_cardinality";
    private static final String AGG_EXTENDED_STATS = "gl2_extended_stats";
    private static final String AGG_FILTER = "gl2_filter";
    private static final String AGG_VALUE_COUNT = "gl2_value_count";

    private static final Sorting DEFAULT_SORTING = new Sorting("doc", Sorting.Direction.ASC);
    private final Configuration configuration;
    private final MultiSearch multiSearch;
    private final Scroll scroll;
    private final SortOrderMapper sortOrderMapper;

    @Inject
    public SearchesAdapterES6(Configuration configuration, MultiSearch multiSearch, Scroll scroll, SortOrderMapper sortOrderMapper) {
        this.configuration = configuration;
        this.multiSearch = multiSearch;
        this.scroll = scroll;
        this.sortOrderMapper = sortOrderMapper;
    }

    @Override
    public CountResult count(Set<String> affectedIndices, String query, TimeRange range, String filter) {
        final String searchSource = standardSearchRequest(query, 0, -1, range, filter, null, false).toString();
        final Search search = new Search.Builder(searchSource).addIndex(affectedIndices).build();
        final io.searchbox.core.SearchResult searchResult = multiSearch.wrap(search, () -> "Unable to perform count query");

        final long tookMs = multiSearch.tookMsFromSearchResult(searchResult);
        return CountResult.create(searchResult.getTotal(), tookMs);
    }

    @Override
    public ScrollResult scroll(Set<String> indexWildcards, Sorting sorting, String filter, String query, TimeRange range, int limit, int offset, List<String> fields) {
        return scroll(ScrollCommand.builder()
                .indices(indexWildcards)
                .sorting(sorting)
                .filter(filter)
                .query(query)
                .range(range)
                .limit(limit)
                .offset(offset)
                .fields(fields)
                .build());
    }

    public ScrollResult scroll(ScrollCommand scrollCommand) {
        final String searchQuery = buildSearchRequest(scrollCommand).toString();
        final Search.Builder initialSearchBuilder = scrollBuilder(searchQuery, scrollCommand.indices());
        scrollCommand.fields().forEach(initialSearchBuilder::addSourceIncludePattern);
        return scroll.scroll(
                initialSearchBuilder.build(),
                () -> "Unable to perform scroll search",
                searchQuery,
                DEFAULT_SCROLLTIME,
                scrollCommand.fields()
        );
    }

    @Override
    public ScrollResult scroll(Set<String> indexWildcards, Sorting sorting, String filter, String query, int batchSize) {
        return scroll(ScrollCommand.builder()
                .indices(indexWildcards)
                .sorting(sorting)
                .filter(filter)
                .query(query)
                .batchSize(batchSize)
                .build());
    }

    private Search.Builder scrollBuilder(String query, Set<String> indices) {
        return new Search.Builder(query)
                .addType(IndexMapping.TYPE_MESSAGE)
                .setParameter(Parameters.SCROLL, DEFAULT_SCROLLTIME)
                .addIndex(indices);
    }

    @Override
    public SearchResult search(Set<String> indices, Set<IndexRange> indexRanges, SearchesConfig config) {
        final SearchSourceBuilder requestBuilder = searchRequest(config);
        if (indexRanges.isEmpty()) {
            return SearchResult.empty(config.query(), requestBuilder.toString());
        }
        final Search.Builder searchBuilder = new Search.Builder(requestBuilder.toString())
                .addType(IndexMapping.TYPE_MESSAGE)
                .addIndex(indices);
        final io.searchbox.core.SearchResult searchResult = multiSearch.wrap(searchBuilder.build(), () -> "Unable to perform search query");

        final List<ResultMessage> hits = searchResult.getHits(Map.class, false).stream()
                .map(hit -> ResultMessage.parseFromSource(hit.id, hit.index, (Map<String, Object>) hit.source, hit.highlight))
                .collect(Collectors.toList());


        return new SearchResult(hits, searchResult.getTotal(), indexRanges, config.query(), requestBuilder.toString(), multiSearch.tookMsFromSearchResult(searchResult));

    }

    @Override
    public FieldStatsResult fieldStats(String query, String filter, TimeRange range, Set<String> indices, String field, boolean includeCardinality, boolean includeStats, boolean includeCount) {

        final SearchSourceBuilder searchSourceBuilder;
        if (filter == null) {
            searchSourceBuilder = standardSearchRequest(query, range);
        } else {
            searchSourceBuilder = filteredSearchRequest(query, filter, range);
        }

        final FilterAggregationBuilder filterBuilder = AggregationBuilders.filter(AGG_FILTER, standardAggregationFilters(range, filter));
        if (includeCount) {
            searchSourceBuilder.aggregation(AggregationBuilders.count(AGG_VALUE_COUNT).field(field));
        }
        if (includeStats) {
            searchSourceBuilder.aggregation(AggregationBuilders.extendedStats(AGG_EXTENDED_STATS).field(field));
        }
        if (includeCardinality) {
            searchSourceBuilder.aggregation(AggregationBuilders.cardinality(AGG_CARDINALITY).field(field));
        }

        searchSourceBuilder.aggregation(filterBuilder);

        if (indices.isEmpty()) {
            return FieldStatsResult.empty(query, searchSourceBuilder.toString());
        }

        final Search searchRequest = new Search.Builder(searchSourceBuilder.toString())
                .addType(IndexMapping.TYPE_MESSAGE)
                .addIndex(indices)
                .build();
        final io.searchbox.core.SearchResult searchResponse = multiSearch.wrap(searchRequest, () -> "Unable to retrieve fields stats");
        final List<ResultMessage> hits = searchResponse.getHits(Map.class, false).stream()
                .map(hit -> ResultMessage.parseFromSource(hit.id, hit.index, (Map<String, Object>) hit.source))
                .collect(Collectors.toList());

        final ExtendedStatsAggregation extendedStatsAggregation = searchResponse.getAggregations().getExtendedStatsAggregation(AGG_EXTENDED_STATS);
        final ValueCountAggregation valueCountAggregation = searchResponse.getAggregations().getValueCountAggregation(AGG_VALUE_COUNT);
        final CardinalityAggregation cardinalityAggregation = searchResponse.getAggregations().getCardinalityAggregation(AGG_CARDINALITY);

        return new FieldStatsResult(
                valueCountAggregation,
                extendedStatsAggregation,
                cardinalityAggregation,
                hits,
                query,
                searchSourceBuilder.toString(),
                multiSearch.tookMsFromSearchResult(searchResponse)
        );
    }

    private QueryBuilder standardAggregationFilters(TimeRange range, String filter) {
        final QueryBuilder filterBuilder = standardFilters(range, filter);

        // Throw an exception here to avoid exposing an internal Elasticsearch exception later.
        if (filterBuilder == null) {
            throw new RuntimeException("Either range or filter must be set.");
        }

        return filterBuilder;
    }
    private SearchSourceBuilder searchRequest(SearchesConfig config) {
        final SearchSourceBuilder request;

        if (config.filter() == null) {
            request = standardSearchRequest(config.query(), config.limit(), config.offset(), config.range(), config.sorting());
        } else {
            request = filteredSearchRequest(config.query(), config.filter(), config.limit(), config.offset(), config.range(), config.sorting());
        }

        final List<String> fields = config.fields();
        if (fields != null) {
            // Use source filtering instead of SearchSourceBuilder#fields() here because Jest cannot handle responses
            // without a "_source" field yet. See:
            // https://github.com/searchbox-io/Jest/issues/157
            // https://github.com/searchbox-io/Jest/issues/339
            request.fetchSource(fields.toArray(new String[0]), Strings.EMPTY_ARRAY);
        }

        return request;
    }

    private SearchSourceBuilder standardSearchRequest(String query, TimeRange range) {
        return standardSearchRequest(query, 0, 0, range, null);
    }

    private SearchSourceBuilder standardSearchRequest(String query,
                                                      int limit,
                                                      int offset,
                                                      TimeRange range,
                                                      Sorting sort) {
        return standardSearchRequest(query, limit, offset, range, sort, true);
    }

    private SearchSourceBuilder standardSearchRequest(
            String query,
            int limit,
            int offset,
            TimeRange range,
            Sorting sort,
            boolean highlight) {
        return standardSearchRequest(query, limit, offset, range, null, sort, highlight);
    }

    private SearchSourceBuilder standardSearchRequest(
            String query,
            int limit,
            int offset,
            TimeRange range,
            String filter,
            Sorting sort,
            boolean highlight) {
        if (query == null || query.trim().isEmpty()) {
            query = "*";
        }

        final QueryBuilder queryBuilder;
        if ("*".equals(query.trim())) {
            queryBuilder = matchAllQuery();
        } else {
            queryBuilder = queryStringQuery(query).allowLeadingWildcard(configuration.isAllowLeadingWildcardSearches());
        }

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.boolQuery().must(queryBuilder).filter(standardFilters(range, filter)));

        if (offset >= 0) {
            searchSourceBuilder.from(offset);
        }

        if (limit > 0) {
            searchSourceBuilder.size(limit);
        }

        if (sort != null) {
            searchSourceBuilder.sort(sort.getField(), sortOrderMapper.fromSorting(sort));
        }

        if (highlight && configuration.isAllowHighlighting()) {
            final HighlightBuilder highlightBuilder = new HighlightBuilder()
                    .requireFieldMatch(false)
                    .field("*")
                    .fragmentSize(0)
                    .numOfFragments(0);
            searchSourceBuilder.highlighter(highlightBuilder);
        }

        return searchSourceBuilder;
    }


    private SearchSourceBuilder buildSearchRequest(ScrollCommand scrollCommand) {
        final String query = normalizeQuery(scrollCommand.query());

        final QueryBuilder queryBuilder = isWildcardQuery(query)
                ? matchAllQuery()
                : queryStringQuery(query).allowLeadingWildcard(configuration.isAllowLeadingWildcardSearches());

        final Optional<BoolQueryBuilder> rangeQueryBuilder = scrollCommand.range()
                .map(range -> QueryBuilders.boolQuery()
                        .must(IndexHelper.getTimestampRangeFilter(range)));
        final Optional<BoolQueryBuilder> filterQueryBuilder = scrollCommand.filter()
                .filter(filter -> !isWildcardQuery(filter))
                .map(filter -> rangeQueryBuilder.orElse(QueryBuilders.boolQuery())
                        .must(queryStringQuery(filter)));

        final BoolQueryBuilder filteredQueryBuilder = QueryBuilders.boolQuery()
                .must(queryBuilder);
        filterQueryBuilder.ifPresent(filteredQueryBuilder::filter);

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(filteredQueryBuilder);

        applyPaginationIfPresent(searchSourceBuilder, scrollCommand);

        applySortingIfPresent(searchSourceBuilder, scrollCommand);

        applyHighlighting(searchSourceBuilder, scrollCommand);

        return searchSourceBuilder;
    }

    private void applyPaginationIfPresent(SearchSourceBuilder searchSourceBuilder, ScrollCommand scrollCommand) {
        scrollCommand.offset().ifPresent(searchSourceBuilder::from);
        scrollCommand.limit().ifPresent(searchSourceBuilder::size);
    }

    private void applyHighlighting(SearchSourceBuilder searchSourceBuilder, ScrollCommand scrollCommand) {
        if (scrollCommand.highlight() && configuration.isAllowHighlighting()) {
            final HighlightBuilder highlightBuilder = new HighlightBuilder()
                    .requireFieldMatch(false)
                    .field("*")
                    .fragmentSize(0)
                    .numOfFragments(0);
            searchSourceBuilder.highlighter(highlightBuilder);
        }
    }

    private void applySortingIfPresent(SearchSourceBuilder searchSourceBuilder, ScrollCommand scrollCommand) {
        final Sorting sort = scrollCommand.sorting().orElse(DEFAULT_SORTING);
        searchSourceBuilder.sort(sort.getField(), sortOrderMapper.fromSorting(sort));
    }

    private boolean isWildcardQuery(String filter) {
        return normalizeQuery(filter).equals("*");
    }

    private String normalizeQuery(String query) {
        if (query == null || query.trim().isEmpty()) {
            return "*";
        }
        return query.trim();
    }

    private SearchSourceBuilder filteredSearchRequest(String query, String filter, TimeRange range) {
        return filteredSearchRequest(query, filter, 0, 0, range, null);
    }

    private SearchSourceBuilder filteredSearchRequest(String query, String filter, int limit, int offset, TimeRange range, Sorting sort) {
        return standardSearchRequest(query, limit, offset, range, filter, sort, true);
    }

    @Nullable
    private QueryBuilder standardFilters(TimeRange range, String filter) {
        BoolQueryBuilder bfb = null;

        if (range != null) {
            bfb = QueryBuilders.boolQuery()
                    .must(IndexHelper.getTimestampRangeFilter(range));
        }

        // Not creating a filter for a "*" value because an empty filter used to be submitted that way.
        if (!isNullOrEmpty(filter) && !"*".equals(filter)) {
            if (bfb == null) {
                bfb = QueryBuilders.boolQuery();
            }
            bfb.must(queryStringQuery(filter));
        }

        return bfb;
    }
}
