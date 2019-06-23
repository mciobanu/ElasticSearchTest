package tech.ciobi;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * <p/>
 * Created by ciobi, 2019-06-16 11:48
 */
@SuppressWarnings({"Duplicates", "SameParameterValue"})
public class TestElasticSearchPerformance {

    private static final String BOOL_FIELD_FMT = "f%02d";
    private static final int WARMUP_ITERATION_COUNT = 2;

    private static String HOST;
    private static int JAVA_API_PORT1;
    private static int JAVA_API_PORT2;

    private static int RUN_NUMBER = 1;


    // How the data looks like: a field called "rank", for sorting , and a number of boolean fields called "f00", "f01", ... (then "f100", ... , if really wanted)

    private final RestHighLevelClient client;
    private final int maxRank;
    private final int batchSize;
    private final int boolFieldsCount; // number of boolean fields, which we use in filtering
    private final long documentCount;
    private final int maxThreads; // maxThreads Some data sets fail with too many threads, so we don't run all the thread tests for them
    private final int threadCountForTermImpact;
    private final double base;
    private final long searchInThreadDuration;
    private final String indexName;

    private AtomicLong searchInThreadEnd = new AtomicLong();

    private double truePercent; // percentage of boolean fields that are true; estimated at warm-up, so it can be used on indexes that are generated differently
    private long diskSpace;

    private final double[] limits;
    private final String RANK_FIELD = "rank";

    private final AtomicInteger finishedThreadsCount = new AtomicInteger();
    private final AtomicInteger finishedQueriesCount = new AtomicInteger();
    private final AtomicInteger queriesWithResultsCount = new AtomicInteger();
    private final AtomicLong totalResultsLowerBound = new AtomicLong();
    private final AtomicLong totalQueryDuration = new AtomicLong();
    private final AtomicInteger errorCount = new AtomicInteger();

    private List<RunResult> runResults = new ArrayList<>();

    // some tests are run for various thread counts, which are taken from this list and capped by maxThreads
    private static final int[] THREAD_COUNTS = new int[]{1, 10, 20, 100, 500, 1000, 1500, 2000, 5000};

    // some tests are run for various term counts, which are taken from this list and capped by boolFieldsCount
    private static final int[] TERM_COUNTS = new int[]{1, 2, 3, 4, 6, 10, 15};

    private enum QueryType {
        ALL,  // all attributes must be found
        SOME, // at least 1 attribute must be found
        NEGATE_ALL,  // take boolFieldsCount-termCount attributes (so there are many); returns docs where they are false
        NEGATE_ALL2; // take boolFieldsCount-termCount attributes (so there are many); returns docs where they are not true (so it's the same
        // results as NEGATE_ALL, but different timing - many times one is significantly faster than the other, but not immediately obvious which beforehand)

        boolean isNegate() {
            return this == NEGATE_ALL || this == NEGATE_ALL2;
        }
    }


    private TestElasticSearchPerformance(RestHighLevelClient client, int maxRank, int batchSize, int boolFieldsCount, int documentCount, int maxThreads,
                                         int threadCountForTermImpact, double base, long searchInThreadDuration, String indexName) {

        if (threadCountForTermImpact > maxThreads) {
            throw new RuntimeException("threadCountForTermImpact cannot exceed maxThreads");
        }

        this.client = client;
        this.maxRank = maxRank;
        this.batchSize = batchSize;
        this.boolFieldsCount = boolFieldsCount;
        this.documentCount = documentCount;
        this.maxThreads = maxThreads;
        this.threadCountForTermImpact = threadCountForTermImpact;
        this.base = base;
        this.searchInThreadDuration = searchInThreadDuration;
        this.indexName = indexName;

        double firstTargetCount = documentCount * 0.9;
        double firstTargetLimit = firstTargetCount / documentCount; // always 0.9
        double lastTargetCount = 10;
        double lastTargetLimit = lastTargetCount / documentCount; // 10/documentCount
        double powFirst = Math.pow(base, 0); // always 1
        double powLast = Math.pow(base, -boolFieldsCount + 1);
        // linear transform so for powFirst we get firstTargetLimit and for powLast we get lastTargetLimit

        double a = (firstTargetLimit - lastTargetLimit) / (powFirst - powLast);
        double b = firstTargetLimit - a * powFirst;

        limits = new double[boolFieldsCount];
        for (int i = 0; i < boolFieldsCount; i++) {
            double pow = Math.pow(base, -i);
            limits[i] = a * pow + b;
        }
        log("%s", Arrays.toString(limits));
    }

    private static final long[] TIME_BUCKETS = new long[] { 1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5000, 10_000, 20_000 };
    private AtomicLong[] timeDistribution = new AtomicLong[TIME_BUCKETS.length];
    {
        for (int i = 0; i < timeDistribution.length; i++) {
            timeDistribution[i] = new AtomicLong();
        }
    }

    private class RunResult {
        long duration;
        long queriesRun;
        long queriesWithResults;
        long totalResultsLowerBound; // note that this is capped at 10000 for a query
        int threadCount;
        long totalQueryDuration;
        int minTerms;
        int maxTerms;
        int errorCount;
        QueryType queryType;
        long[] timeDistribution;

        void print(PrintStream printStream) {
            String distribs = IntStream.range(0, timeDistribution.length)
                    .boxed()
                    .map(k -> queryCountToPercent(timeDistribution[k]))
                    .collect(Collectors.joining("\t"));
            printStream.printf("%s\t%s\t%d\t%,d\t%.3f\t%.3f\t%,d\t" + // ttt0 probably switch all to %s, as these don't improve readability too much, while truncating results
                            "%s\t%d\t%d\t%d\t%d\t%,d\t%,d\t%,d\t%,d\t" +
                            "%.3f\t" +
                            "%.3f\t" +
                            "%.3f\t" +
                            "%,.0f\t" +
                            "%s\t%s\t" +
                            "%n",
                    RUN_NUMBER++,
                    indexName, boolFieldsCount, documentCount, base, truePercent, diskSpace,
                    queryType, threadCount, minTerms, maxTerms, duration, queriesRun, queriesWithResults, totalResultsLowerBound, errorCount,
                    queriesRun * 1000.0 / duration, // queries per second
                    queriesWithResults * 100.0 / queriesRun, // percentage of queries that had results
                    totalResultsLowerBound * 1.0 / queriesRun, // average results per query
                    totalQueryDuration * 1.0 / queriesRun, // average query duration
                    distribs, queryCountToPercent(queriesRun - Arrays.stream(timeDistribution).sum())
            );
        }

        private String queryCountToPercent(long queryCount) {
            if (queryCount == 0) {
                return "";
            }
            return String.valueOf(queryCount * 100.0 / queriesRun);
        }
    }

    private void testPerformance() {
        log("starting testPerformance for %s", indexName);
        if (!checkExists()) {
            generate();
        }
        warmUp();
        testThreadImpact();
        testTermImpact();
    }

    private boolean checkExists() {
        // couldn't find a replacement for this deprecated API; there is a workaround that uses HTTP directly at retrieveDiskSpace()
        try (TransportClient transportClient = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName(HOST), JAVA_API_PORT1))
                .addTransportAddress(new TransportAddress(InetAddress.getByName(HOST), JAVA_API_PORT2))) {
            IndicesStatsResponse indicesStatsResponse = transportClient.admin().indices().prepareStats(indexName).get();
            long count = indicesStatsResponse.getIndices().get(indexName).getTotal().docs.getCount();
            if (documentCount != count) {
                throw new RuntimeException(String.format("Mismatch between expected (%,d) and found (%,d) document count", documentCount, count));
            }
            return true;
        } catch (IndexNotFoundException e) {
            return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void generate() {
        log("starting generate for %s", indexName);
        for (long start = 0; start < documentCount; start += batchSize) {
            addBulk(start, Math.min(batchSize, documentCount - start));
        }
    }

    private void addBulk(long start, long count) {
        log("Adding %d documents to %s", count, indexName);
        try {
            BulkRequest bulkRequest = new BulkRequest(indexName);
            Map<String, Object> m = new TreeMap<>();
            Random random = new Random();

            for (int i = 0; i < count; i++) {
                do {
                    m.clear();
                    m.put(RANK_FIELD, 1_000_000_000 + random.nextInt(maxRank));
                    for (int j = 0; j < boolFieldsCount; j++) {
                        m.put(String.format(BOOL_FIELD_FMT, j), random.nextDouble() < limits[j]);
                    }
                } while (!m.values().contains(true));
                IndexRequest indexRequest = new IndexRequest()
                        .id("" + (i + start))
                        .source(m);
                bulkRequest.add(indexRequest);
            }
            BulkResponse res = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            log("%s", res);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private void warmUp() {
        log("Warming up %s", indexName);
        measureDensity();
        retrieveDiskSpace();
        for (int i = 0; i < WARMUP_ITERATION_COUNT; i++) {
            runQueriesOnMultipleThreads(maxThreads, 1, 5, QueryType.ALL);
        }
    }


    /**
     * Tests the impact on thread count for a small number of terms (between 1 and 5, randomly chosen)
     */
    private void testThreadImpact() {
        log("starting testThreadImpact for %s", indexName);
        for (int k : THREAD_COUNTS) {
            if (k > maxThreads) {
                break;
            }
            runResults.addAll(runQueriesOnMultipleThreads(k, 1, 5));
        }
    }

    private void testTermImpact() {
        log("starting testTermImpact for %s", indexName);
        for (int k : TERM_COUNTS) {
            if (k > boolFieldsCount) {
                break;
            }
            runResults.addAll(runQueriesOnMultipleThreads(threadCountForTermImpact, k, k));
        }
    }


    private void measureDensity() {
        try {
            log("starting measureDensity for %s", indexName);
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.sort(new FieldSortBuilder(RANK_FIELD).order(SortOrder.ASC));
            int readCount = 100;
            searchSourceBuilder.size(readCount);
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            int trueCount = 0;
            for (var h : searchResponse.getHits().getHits()) {
                Map<String, Object> m = h.getSourceAsMap();
                for (Object o : m.values()) {
                    if (o instanceof Boolean && (Boolean) o) {
                        trueCount++;
                    }
                }
            }
            truePercent = trueCount * 100.0 / readCount / boolFieldsCount;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void retrieveDiskSpace() {
        try {
            // The high level REST API doesn't seem to offer something, and not sure the way to use the low level API is right.
            /*
            // Attempt with high-level API - couldn't figure out how to use it
            ClusterHealthRequest request = new ClusterHealthRequest(indexName);
            //request.level(ClusterHealthRequest.Level.INDICES);
            request.level(ClusterHealthRequest.Level.SHARDS);
            ClusterHealthResponse health = client.cluster().health(request, RequestOptions.DEFAULT);
            //health.*/

            // Here we basically parse the response to http://localhost:9200/performance06/_stats with the low-level REST API.
            /*try (RestClient lowLevelClient = RestClient.builder(
                    new HttpHost(HOST, 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build()) {
                List<Node> nodes = lowLevelClient.getNodes();

                Request lowLevelRequest = new Request(
                        "GET",
                        "/" + indexName + "/_stats");
                Response response = lowLevelClient.performRequest(lowLevelRequest);
                String responseBody = EntityUtils.toString(response.getEntity());

                Gson gson = new GsonBuilder()
                        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                        .create();

                IndexStatsResponse indexStatsResponse = gson.fromJson(responseBody, IndexStatsResponse.class);

                diskSpace = indexStatsResponse.indices.get(indexName).total.store.sizeInBytes;
            }*/

            // So we use the old Java API, which is being deprecated - https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.1/_motivations_around_a_new_java_client.html
            try (TransportClient transportClient = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(HOST), JAVA_API_PORT1))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(HOST), JAVA_API_PORT2))) {
                IndicesStatsResponse indicesStatsResponse = transportClient.admin().indices().prepareStats(indexName).get();
                diskSpace = indicesStatsResponse.getIndices().get(indexName).getTotal().store.getSizeInBytes();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*// was used in order to parse a JSON response with the low-level API; it's easier with the Java client
    private static class IndexStatsResponse {
        private static class IndexEntry {
            private static class TotalEntry {
                private static class StoreEntry {
                    long sizeInBytes;
                }
                private static class DocsEntry {
                    long count;
                    long deleted;
                }
                StoreEntry store;
                DocsEntry docs;
            }
            TotalEntry total;
        }

        @SuppressWarnings({"unused", "MismatchedQueryAndUpdateOfCollection"})
        private Map<String, IndexEntry> indices;
    }*/


    private List<RunResult> runQueriesOnMultipleThreads(int threadCount, int minTerms, int maxTerms) {
        List<RunResult> res = new ArrayList<>();
        for (QueryType queryType : QueryType.values()) {
            res.add(runQueriesOnMultipleThreads(threadCount, minTerms, maxTerms, queryType));
        }
        return res;
    }


    private final Object sync = new Object();

    private RunResult runQueriesOnMultipleThreads(int threadCount, int minTerms, int maxTerms, QueryType queryType) {
        try {
            log("starting runQueriesOnMultipleThreads for %s, %s, %s, %s", indexName, threadCount, minTerms, maxTerms);
            finishedThreadsCount.set(0);
            finishedQueriesCount.set(0);
            queriesWithResultsCount.set(0);
            totalResultsLowerBound.set(0);
            totalQueryDuration.set(0);
            errorCount.set(0);
            searchInThreadEnd.set(System.currentTimeMillis() + searchInThreadDuration);
            for (var atomicLong : timeDistribution) {
                atomicLong.set(0);
            }

            long start = System.currentTimeMillis();
            for (int i = 0; i < threadCount; i++) {
                new Thread(() -> {
                    try {
                        searchInThread(threadCount, minTerms, maxTerms, queryType,false);
                    } catch (Exception e) {
                        e.printStackTrace();
                        errorCount.incrementAndGet();
                    }
                    finishedThreadsCount.incrementAndGet();
                    synchronized (sync) {
                        sync.notify();
                    }
                }).start();
            }
            for (; ; ) {
                synchronized (sync) {
                    sync.wait(1000); // since many threads use the same sync, it is possible that this won't get awaken and would remain locked
                    // forever without a timeout; at least this seemed to be the observed behavior, and didn't dig deeper; should review notifyAll() vs notify()
                    if (finishedThreadsCount.get() == threadCount) {
                        break;
                    }
                    log("only %s finished out of %s; continue waiting ...", finishedThreadsCount.get(), threadCount);
                }
            }
            RunResult res = new RunResult();
            res.duration = System.currentTimeMillis() - start;
            res.queriesRun = finishedQueriesCount.get();
            res.queriesWithResults = queriesWithResultsCount.get();
            res.totalResultsLowerBound = totalResultsLowerBound.get();
            res.totalQueryDuration = totalQueryDuration.get();
            res.threadCount = threadCount;
            res.minTerms = minTerms;
            res.maxTerms = maxTerms;
            res.queryType = queryType;
            res.errorCount = errorCount.get();
            res.timeDistribution = new long[TIME_BUCKETS.length];
            for (int i = 0; i < TIME_BUCKETS.length; i++) {
                res.timeDistribution[i] = timeDistribution[i].get();
            }
            log("overall total run: %s ms, query count: %s, queries with results: %s, queries per second: %s", res.duration, finishedQueriesCount.get(), queriesWithResultsCount.get(), finishedQueriesCount.get() * 1000.0 / res.duration);
            return res;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void searchInThread(int threadCount, int minTerms, int maxTerms, QueryType queryType, boolean log) {
        log("starting searchInThread for %s, %s, %s, %s", indexName, threadCount, minTerms, maxTerms);
        long start = System.currentTimeMillis();
        int run = 0;
        for (; run == 0 || System.currentTimeMillis() < searchInThreadEnd.get(); run++) {
            individualSearch(run, threadCount, minTerms, maxTerms, queryType, log);
        }
        long duration = System.currentTimeMillis() - start;
        log("total run for %s, %s, %s, %s: %s; queries per second: %s", indexName, threadCount, minTerms, maxTerms, duration, run * 1000.0 / duration);
    }


    private void individualSearch(int run, int threadCount, int minTerms, int maxTerms, QueryType queryType, boolean log) {
        try {
            long start = System.currentTimeMillis();
            log |= Math.random() < 0.0001;
            if (log) {
                log("starting individualSearch for %s, %s, %s, %s, %s", run, indexName, threadCount, minTerms, maxTerms);
            }
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            Random random = new Random();
            int termCount = minTerms + random.nextInt(maxTerms - minTerms + 1);
            Set<Integer> s = new HashSet<>();
            while (s.size() < termCount) {
                s.add(random.nextInt(boolFieldsCount));
            }
            //boolQueryBuilder.must(boolQueryBuilder); ttt0 play some more with expressions

            for (int i = 0; i < boolFieldsCount; i++) {
                if (s.contains(i) ^ !queryType.isNegate()) {
                    continue;
                }
                switch (queryType) {
                    case ALL: {
                        boolQueryBuilder.must(QueryBuilders.termQuery(String.format(BOOL_FIELD_FMT, i), true));
                        break;
                    }
                    case SOME: {
                        boolQueryBuilder.should(QueryBuilders.termQuery(String.format(BOOL_FIELD_FMT, i), true));
                        break;
                    }
                    case NEGATE_ALL: {
                        boolQueryBuilder.mustNot(QueryBuilders.termQuery(String.format(BOOL_FIELD_FMT, i), true));
                        break;
                    }
                    case NEGATE_ALL2: {
                        boolQueryBuilder.must(QueryBuilders.termQuery(String.format(BOOL_FIELD_FMT, i), false));
                        break;
                    }
                }
            }
            searchSourceBuilder.query(boolQueryBuilder);
            searchSourceBuilder.sort(new FieldSortBuilder(RANK_FIELD).order(SortOrder.ASC));
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            finishedQueriesCount.incrementAndGet();
            long duration = System.currentTimeMillis() - start;
            if (log
                //|| searchResponse.getHits().getHits().length > 0
            ) {
                log("finished individualSearch for %s, %s, %s, %s; query: %s, total hits: %s, took %s ms (measured locally: %s ms)",
                        indexName, threadCount, minTerms, maxTerms,
                        boolQueryBuilder.toString(),
                        searchResponse.getHits().getTotalHits().value,
                        searchResponse.getTook().getMillis(),
                        duration);
                for (var h : searchResponse.getHits().getHits()) {
                    log("%s", new TreeMap<>(h.getSourceAsMap()));
                }
                //log("%s", searchResponse);
            }

            totalResultsLowerBound.addAndGet(searchResponse.getHits().getTotalHits().value);
            totalQueryDuration.addAndGet(duration);
            for (int i = 0; i < TIME_BUCKETS.length; i++) {
                if (duration < TIME_BUCKETS[i]) {
                    timeDistribution[i].incrementAndGet();
                    break;
                }
            }

            if (searchResponse.getHits().getHits().length > 0) {
                queriesWithResultsCount.incrementAndGet();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private void printResults() {
        runResults.forEach(runResult -> runResult.print(System.out));
    }

    public static void main(String[] args) {

        HOST = args.length > 0 ? args[0] : "localhost";
        int port1 = args.length > 1 ? Integer.parseInt(args[1]) : 9200;
        int port2 = args.length > 2 ? Integer.parseInt(args[2]) : 9201;
        JAVA_API_PORT1 = args.length > 3 ? Integer.parseInt(args[1]) : 9300;
        JAVA_API_PORT2 = args.length > 4 ? Integer.parseInt(args[1]) : 9301;

        List<TestElasticSearchPerformance> tests = new ArrayList<>();

        long start = System.currentTimeMillis();

        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(HOST, port1, "http"),
                        new HttpHost(HOST, port2, "http")))) {

            //tests.add(new TestElasticSearchPerformance(client, 10_000, 100_000,  20, 10_000_000, 100, 500,50, 2.00, "performance02")); // space on disk: 400 MB; percentage of true values: 4.80

            //tests.add(new TestElasticSearchPerformance(client, 10_000, 100_000,  16, 100_000, 100, 500, 50, 2.00, "test_create_01")); // space on disk: 400 MB; percentage of true values: 4.80

/*
            TestElasticSearchPerformance test01 = new TestElasticSearchPerformance(client, 10_000, 100_000,  20, 10_000_000, 5000, 500, 50, 2.00, "test01"); // space on disk: 400 MB; percentage of true values: 4.80
            TestElasticSearchPerformance test02 = new TestElasticSearchPerformance(client, 10_000,  50_000,  60,  1_000_000, 2000, 500, 50, 1.09, "test02"); // space on disk: 130 MB; percentage of true values: 18.45
            TestElasticSearchPerformance test03 = new TestElasticSearchPerformance(client, 10_000,  50_000, 100, 50_000_000,  100, 100, 50, 1.05, "test03"); // space on disk: 10 GB; percentage of true values: 18.12
            TestElasticSearchPerformance test04 = new TestElasticSearchPerformance(client, 10_000,  50_000, 100, 50_000_000, 2000, 500, 50, 1.25, "test04"); // space on disk: 5.2 GB; percentage of true values: 4.49
            TestElasticSearchPerformance test05 = new TestElasticSearchPerformance(client, 10_000,  50_000, 100, 50_000_000, 2000, 500, 50, 2.00, "test05"); // space on disk: 3.9 GB; percentage of true values: 1.83
*/

/*
            tests.add(new TestElasticSearchPerformance(client, 10_000, 100_000,  20, 10_000_000, 5000, 500, 50, 2.00, "performance02"));
            tests.add(new TestElasticSearchPerformance(client, 10_000,  50_000,  60,  1_000_000, 2000, 500, 50, 1.29, "performance03"));
            tests.add(new TestElasticSearchPerformance(client, 10_000,  50_000, 100, 50_000_000,  100, 100, 50, 1.05, "performance04"));
            tests.add(new TestElasticSearchPerformance(client, 10_000,  50_000, 100, 50_000_000, 2000, 500, 50, 1.25, "performance05"));
            tests.add(new TestElasticSearchPerformance(client, 10_000,  50_000, 100, 50_000_000, 2000, 500, 50, 2.00, "performance06"));
*/

            //TestElasticSearchPerformance test01 = new TestElasticSearchPerformance(client, 10_000, 100_000,  20, 10_000_000, 5000, 500, 2.00, 60000, "test01");
            //TestElasticSearchPerformance test01 = new TestElasticSearchPerformance(client, 10_000, 100_000,  20, 10_000_000, 100, 100, 2.00, 10000, "test01");
            TestElasticSearchPerformance test01 = new TestElasticSearchPerformance(client, 10_000, 100_000,  20, 10_000_000, 100, 100, 2.00, 60000, "test01");
            tests.add(test01);

            //TestElasticSearchPerformance test02 = new TestElasticSearchPerformance(client, 10_000,  50_000,  60,  1_000_000, 2000, 500, 1.29, 60000, "test02");
            //TestElasticSearchPerformance test02 = new TestElasticSearchPerformance(client, 10_000,  50_000,  60,  1_000_000, 100, 100, 1.29, 10000, "test02");
            TestElasticSearchPerformance test02 = new TestElasticSearchPerformance(client, 10_000,  50_000,  60,  1_000_000, 100, 100, 1.29, 60000, "test02");
            tests.add(test02);

            //TestElasticSearchPerformance test03 = new TestElasticSearchPerformance(client, 10_000,  50_000, 100, 50_000_000,  100, 100, 1.05, 60000, "test03");
            //TestElasticSearchPerformance test03 = new TestElasticSearchPerformance(client, 10_000,  50_000, 100, 50_000_000,   20, 20, 1.05, 10000, "test03");
            TestElasticSearchPerformance test03 = new TestElasticSearchPerformance(client, 10_000,  50_000, 100, 50_000_000,  20,  20, 1.05, 60000, "test03");
            tests.add(test03);

            //TestElasticSearchPerformance test04 = new TestElasticSearchPerformance(client, 10_000,  50_000, 100, 50_000_000, 2000, 500, 1.25, 60000, "test04");
            //TestElasticSearchPerformance test04 = new TestElasticSearchPerformance(client, 10_000,  50_000, 100, 50_000_000, 100, 100, 1.25, 10000, "test04");
            TestElasticSearchPerformance test04 = new TestElasticSearchPerformance(client, 10_000,  50_000, 100, 50_000_000, 100, 100, 1.25, 60000, "test04");
            tests.add(test04);

            //TestElasticSearchPerformance test05 = new TestElasticSearchPerformance(client, 10_000,  30_000, 100, 50_000_000, 2000, 500, 2.00, 60000, "test05");
            //TestElasticSearchPerformance test05 = new TestElasticSearchPerformance(client, 10_000,  30_000, 100, 50_000_000, 100, 100, 2.00, 10000, "test05");
            TestElasticSearchPerformance test05 = new TestElasticSearchPerformance(client, 10_000,  30_000, 100, 50_000_000, 100, 100, 2.00, 60000, "test05");
            tests.add(test05);



            //TestElasticSearchPerformance test05 = new TestElasticSearchPerformance(client, 10_000, 30_000, 100, 50_000_000, 2000, 500, 50, 2.00, "test05");
            //TestElasticSearchPerformance test05 = new TestElasticSearchPerformance(client, 10_000, 30_000, 100, 50_000_000, 100, 100, 50, 2.00, "test05");
            //tests.add(test05);
            //new TestElasticSearchPerformance(client, 10_000,  30_000, 100, 50_000_000, 20, 500, 50, 2.00, "test05").individualNegateSearch(10, 6, 10, 10, true);

            TestElasticSearchPerformance test06 = new TestElasticSearchPerformance(client, 10_000, 30_000, 100, 50_000, 200, 200, 2.0, 60000, "test06");
            tests.add(test06);
            //test06.individualNegateSearch(10, 6, 10, 10, true);

            //TestElasticSearchPerformance test07 = new TestElasticSearchPerformance(client, 10_000, 30_000, 5    , 100, 10, 10, 2.0, 100, "test07");
            TestElasticSearchPerformance test07 = new TestElasticSearchPerformance(client, 10_000, 30_000, 5    , 100, 10, 10, 2.0, 60000, "test07");
            tests.add(test07);

            //test07.individualSearch(1, 1, 2, 2, QueryType.NEGATE_ALL, true);
            //test07.individualSearch(1, 1, 2, 2, QueryType.NEGATE_ALL2, true);

            tests.forEach(TestElasticSearchPerformance::testPerformance);


            //performance02.add(client);
            //performance05.add(client);
            //performance06.add(client);

            //---------------------- compare based on data ----------------------

            //performance02.measureDensity(client); // Percentage of true values: 4.80
            //performance02.testQueryPerformance(client, 50, 10, 1, 5);     //  1300 queries per second
            //performance02.testQueryPerformance(client, 50, 20, 1, 5);     //  2100 queries per second
            //performance02.testQueryPerformance(client, 50, 100, 1, 5);    //  5500 queries per second
            //performance02.testQueryPerformance(client, 50, 500, 1, 5);    // 11000 queries per second
            //performance02.testQueryPerformance(client, 50, 1000, 1, 5);   // 15000 queries per second
            //performance02.testQueryPerformance(client, 50, 1500, 1, 5);   // 16500 queries per second
            //performance02.testQueryPerformance(client, 50, 2000, 1, 5);   // 16500 queries per second
            //performance02.testQueryPerformance(client, 50, 5000, 1, 5);   //  4700 queries per second

            //performance03.measureDensity(client); // Percentage of true values: 18.45
            //performance03.testQueryPerformance(client, 50, 10, 1, 5);     //   850 queries per second
            //performance03.testQueryPerformance(client, 50, 20, 1, 5);     //  1250 queries per second
            //performance03.testQueryPerformance(client, 50, 100, 1, 5);    //  2150 queries per second
            //performance03.testQueryPerformance(client, 50, 500, 1, 5);    //  2800 queries per second
            //performance03.testQueryPerformance(client, 50, 1000, 1, 5);   //  2900 queries per second
            //performance03.testQueryPerformance(client, 50, 1500, 1, 5);   //  2950 queries per second
            //performance03.testQueryPerformance(client, 50, 2000, 1, 5);   //  3000 queries per second

            //performance04.measureDensity(client); // Percentage of true values: 18.12
            //performance04.testQueryPerformance(client, 50, 10, 1, 5);     //  41 queries per second
            //performance04.testQueryPerformance(client, 50, 20, 1, 5);     //  49 queries per second
            //performance04.testQueryPerformance(client, 50, 100, 1, 5);    //  58 queries per second

            //performance05.measureDensity(client); // Percentage of true values: 4.49
            //performance05.testQueryPerformance(client, 50, 10, 1, 5);     //  250 queries per second
            //performance05.testQueryPerformance(client, 50, 20, 1, 5);     //  360 queries per second
            //performance05.testQueryPerformance(client, 30, 20, 1, 5);     //  340 queries per second
            //performance05.testQueryPerformance(client, 50, 100, 1, 5);    //  500 queries per second
            //performance05.testQueryPerformance(client, 50, 500, 1, 5);    //  550 queries per second
            //performance05.testQueryPerformance(client, 50, 1000, 1, 5);   //  560 queries per second
            //performance05.testQueryPerformance(client, 30, 1500, 1, 5);   //  570 queries per second
            //performance05.testQueryPerformance(client, 30, 2000, 1, 5);   //  560 queries per second

            //performance06.measureDensity(client); // Percentage of true values: 1.83
            //performance06.testQueryPerformance(client, 50, 10, 1, 5);     //   410 queries per second
            //performance06.testQueryPerformance(client, 50, 20, 1, 5);     //   690 queries per second
            //performance06.testQueryPerformance(client, 50, 100, 1, 5);    //  1200 queries per second
            //performance06.testQueryPerformance(client, 50, 500, 1, 5);    //  1700 queries per second
            //performance06.testQueryPerformance(client, 50, 1000, 1, 5);   //  1900 queries per second
            //performance06.testQueryPerformance(client, 50, 1500, 1, 5);   //  1800 queries per second
            //performance06.testQueryPerformance(client, 50, 2000, 1, 5);   //  1900 queries per second

            //---------------------- compare number of terms ----------------------

            // note: there are timeouts and crashes with 500 threads, so just use 100
            //performance04.measureDensity(client); // Percentage of true values: 18.12
            //performance04.testQueryPerformance(client, 50, 100, 1, 1);      //    48 queries per second, 25000 queries with results (normalized by multiplication by 5)
            //performance04.testQueryPerformance(client, 50, 100, 2, 2);      //    47 queries per second, 24750 queries with results (normalized by multiplication by 5)
            //performance04.testQueryPerformance(client, 50, 100, 3, 3);      //    65 queries per second, 24325 queries with results (normalized by multiplication by 5)
            //performance04.testQueryPerformance(client, 50, 100, 4, 4);      //    85 queries per second, 23215 queries with results (normalized by multiplication by 5)
            //performance04.testQueryPerformance(client, 50, 100, 6, 6);      //   110 queries per second, 16050 queries with results (normalized by multiplication by 5)
            //performance04.testQueryPerformance(client, 50, 100, 10, 10);    //   600 queries per second,  1425 queries with results (normalized by multiplication by 5)
            //performance04.testQueryPerformance(client, 50, 100, 15, 15);    //  1000 queries per second, no results

            //performance05.measureDensity(client); // Percentage of true values: 4.49
            //performance05.testQueryPerformance(client, 50, 500, 1, 1);      //   190 queries per second, 25000 queries with results
            //performance05.testQueryPerformance(client, 50, 500, 2, 2);      //   640 queries per second, 10000 queries with results
            //performance05.testQueryPerformance(client, 50, 500, 3, 3);      //  1500 queries per second,  2700 queries with results
            //performance05.testQueryPerformance(client, 50, 500, 4, 4);      //  3300 queries per second,   610 queries with results
            //performance05.testQueryPerformance(client, 50, 500, 6, 6);      //  8000 queries per second,    28 queries with results
            //performance05.testQueryPerformance(client, 50, 500, 10, 10);    //  8400 queries per second, no results
            //performance05.testQueryPerformance(client, 50, 500, 15, 15);    //  7000 queries per second, no results

            //performance06.measureDensity(client); // Percentage of true values: 1.83
            //performance06.testQueryPerformance(client, 50, 500, 1, 1);      //   450 queries per second, 25000 queries with results
            //performance06.testQueryPerformance(client, 50, 500, 2, 2);      //  3800 queries per second,  2808 queries with results
            //performance06.testQueryPerformance(client, 50, 500, 3, 3);      //  8000 queries per second,   210 queries with results
            //performance06.testQueryPerformance(client, 50, 500, 4, 4);      //  9500 queries per second,    14 queries with results
            //performance06.testQueryPerformance(client, 50, 500, 6, 6);      //  9900 queries per second, no results
            //performance06.testQueryPerformance(client, 50, 500, 10, 10);    //  9700 queries per second, no results
            //performance06.testQueryPerformance(client, 50, 500, 15, 15);    //  8800 queries per second, no results
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            System.out.printf("\n\n=================================================================================\nTotal duration: %s\n=================================================================================%n", System.currentTimeMillis() - start);

            System.out.print("run\tindex name\tbool fields count\tdocument count\tbase\tpercentage of true\tdisk space\tquery type\tthread count\tmin terms\tmax terms\tduration\tqueries run\t" +
                    "queries with results\ttotal results lower bound\terror count\tqueries per second\tpercentage of queries that had results\taverage results per query\taverage query duration");
            for (long t : TIME_BUCKETS) {
                System.out.printf("\t<%,d", t);
            }
            System.out.printf("\t>=%,d\n", TIME_BUCKETS[TIME_BUCKETS.length - 1]);
            tests.forEach(TestElasticSearchPerformance::printResults);
        }
    }

    //ttt0 switch to Log4j
    private static void log(String s, Object... params) {
        String msg = String.format("%s %s\n", new Date(), String.format(s, params));
        System.out.print(msg);
        try {
            Files.write(Paths.get("log.txt"), msg.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
