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

import java.io.PrintStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    private final int queryCountPerThread;
    private final double base;
    private final String indexName;

    private double truePercent; // percentage of boolean fields that are true; estimated at warm-up, so it can be used on indexes that are generated differently
    private long diskSpace;

    private final double[] limits;
    private final String RANK_FIELD = "rank";

    private final AtomicInteger finishedThreadsCount = new AtomicInteger();
    private final AtomicInteger finishedQueriesCount = new AtomicInteger();
    private final AtomicInteger queriesWithResultsCount = new AtomicInteger();
    private final AtomicLong totalResultsLowerBound = new AtomicLong();
    private final AtomicLong totalQueryDuration = new AtomicLong();

    private List<RunResult> runResults = new ArrayList<>();

    // some tests are run for various thread counts, which are taken from this list and capped by maxThreads
    private static final int[] THREAD_COUNTS = new int[]{1, 10, 20, 100, 500, 1000, 1500, 2000, 5000};

    // some tests are run for various term counts, which are taken from this list
    private static final int[] TERM_COUNTS = new int[]{1, 2, 3, 4, 6, 10, 15};

    private TestElasticSearchPerformance(RestHighLevelClient client, int maxRank, int batchSize, int boolFieldsCount, int documentCount, int maxThreads, int threadCountForTermImpact, int queryCountPerThread, double base, String indexName) {

        this.client = client;
        this.maxRank = maxRank;
        this.batchSize = batchSize;
        this.boolFieldsCount = boolFieldsCount;
        this.documentCount = documentCount;
        this.maxThreads = maxThreads;
        this.threadCountForTermImpact = threadCountForTermImpact;
        this.queryCountPerThread = queryCountPerThread;
        this.base = base;
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
        System.out.println(Arrays.toString(limits));
    }

    private class RunResult {
        long duration;
        int queriesRun;
        int queriesWithResults;
        long totalResultsLowerBound; // note that this is capped at 10000 for a query
        int threadCount;
        long totalQueryDuration;
        int minTerms;
        int maxTerms;

        void print(PrintStream printStream) {
            printStream.printf("%s\t%s\t%d\t%,d\t%.3f\t%.3f\t%,d\t" +
                            "%d\t%d\t%d\t%d\t%d\t%,d\t%,d\t%,d\t" +
                            "%.3f\t" +
                            "%.3f\t" +
                            "%.3f\t" +
                            "%,d\t" +
                            "%n",
                    RUN_NUMBER++,
                    indexName, boolFieldsCount, documentCount, base, truePercent, diskSpace,
                    threadCount, queryCountPerThread, minTerms, maxTerms, duration, queriesRun, queriesWithResults, totalResultsLowerBound,
                    queriesRun * 1000.0 / duration, // queries per second
                    queriesWithResults * 100.0 / queriesRun, // percentage of queries that had results
                    totalResultsLowerBound * 1.0 / queriesRun, // average results per query
                    totalQueryDuration / queriesRun // average query duration
            );
        }
    }

    private void testPerformance() {
        System.out.printf("starting testPerformance for %s%n", indexName);
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
        System.out.printf("starting generate for %s%n", indexName);
        for (long start = 0; start < documentCount; start += batchSize) {
            addBulk(start, Math.min(batchSize, documentCount - start));
        }
    }

    private void addBulk(long start, long count) {
        System.out.printf("Adding %d documents to %s%n", count, indexName);
        try {
            BulkRequest bulkRequest = new BulkRequest(indexName);
            Map<String, Object> m = new TreeMap<>();
            Random random = new Random();

            for (int i = 0; i < count; i++) {
                m.clear();
                m.put(RANK_FIELD, 1_000_000_000 + random.nextInt(maxRank));
                for (int j = 0; j < boolFieldsCount; j++) {
                    m.put(String.format(BOOL_FIELD_FMT, j), random.nextDouble() < limits[j]);
                }
                IndexRequest indexRequest = new IndexRequest()
                        .id("" + (i + start))
                        .source(m);
                bulkRequest.add(indexRequest);
            }
            BulkResponse res = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            System.out.println(res);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private void warmUp() {
        System.out.printf("Warming up %s%n", indexName);
        measureDensity();
        retrieveDiskSpace();
        for (int i = 0; i < WARMUP_ITERATION_COUNT; i++) {
            runQueriesOnMultipleThreads(maxThreads, 1, 5);
        }
    }


    /**
     * Tests the impact on thread count for a small number of terms (between 1 and 5, randomly chosen)
     */
    private void testThreadImpact() {
        System.out.printf("starting testThreadImpact for %s%n", indexName);
        for (int k : THREAD_COUNTS) {
            if (k > maxThreads) {
                break;
            }
            runResults.add(runQueriesOnMultipleThreads(k, 1, 5));
        }
    }

    private void testTermImpact() {
        System.out.printf("starting testTermImpact for %s%n", indexName);
        for (int k : TERM_COUNTS) {
            runResults.add(runQueriesOnMultipleThreads(threadCountForTermImpact, k, k));
        }
    }



    private void measureDensity() {
        try {
            System.out.printf("starting measureDensity for %s%n", indexName);
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

            //System.out.printf("Percentage of true values: %.2f%n", trueCount * 100.0 / readCount / recordSize);
            truePercent = trueCount * 100.0 / readCount / boolFieldsCount;
            //diskSpace
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void retrieveDiskSpace() {
        try {
            // The high level REST API doesn't seem to offer something, and not sure the way to use the low level API is right.
            // We basically parse the response to http://localhost:9200/performance06/_stats with the low-level REST API.
            // So we use the old Java API, which is being deprecated - https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.1/_motivations_around_a_new_java_client.html

            /*
            // Attempt with high-level API - couldn't figure out how to use it
            ClusterHealthRequest request = new ClusterHealthRequest(indexName);
            //request.level(ClusterHealthRequest.Level.INDICES);
            request.level(ClusterHealthRequest.Level.SHARDS);
            ClusterHealthResponse health = client.cluster().health(request, RequestOptions.DEFAULT);
            //health.*/

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

            try (TransportClient transportClient = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(HOST), JAVA_API_PORT1))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(HOST), JAVA_API_PORT2))) {
                IndicesStatsResponse indicesStatsResponse = transportClient.admin().indices().prepareStats(indexName).get();
                diskSpace = indicesStatsResponse.getIndices().get(indexName).getTotal().store.getSizeInBytes();
                System.out.println();
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


    private RunResult runQueriesOnMultipleThreads(int threadCount, int minTerms, int maxTerms) {
        try {
            System.out.printf("starting runQueriesOnMultipleThreads for %s, %s, %s, %s%n", indexName, threadCount, minTerms, maxTerms);
            finishedThreadsCount.set(0);
            finishedQueriesCount.set(0);
            queriesWithResultsCount.set(0);
            totalResultsLowerBound.set(0);
            totalQueryDuration.set(0);
            long start = System.currentTimeMillis();
            for (int i = 0; i < threadCount; i++) {
                new Thread(() -> {
                    try {
                        searchInThread(threadCount, queryCountPerThread, minTerms, maxTerms, false);
                        finishedThreadsCount.incrementAndGet();
                        synchronized (finishedThreadsCount) {
                            finishedThreadsCount.notify();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).start();
            }
            for (; ; ) {
                synchronized (finishedThreadsCount) {
                    finishedThreadsCount.wait();
                    if (finishedThreadsCount.get() == threadCount) {
                        break;
                    }
                    System.out.printf("only %s finished out of %s; continue waiting ...%n", finishedThreadsCount.get(), threadCount);
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
            System.out.printf("overall total run: %s ms, query count: %s, queries with results: %s, queries per second: %s%n", res.duration, finishedQueriesCount.get(), queriesWithResultsCount.get(), queryCountPerThread * threadCount * 1000.0 / res.duration);

            return res;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private void searchInThread(int threadCount, int count, int minTerms, int maxTerms, boolean log) {
        System.out.printf("starting searchInThread for %s, %s, %s, %s, %s%n", indexName, threadCount, count, minTerms, maxTerms);
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            individualSearch(threadCount, count, minTerms, maxTerms, log);
        }
        long duration = System.currentTimeMillis() - start;
        System.out.printf("total run for %s, %s, %s, %s, %s: %s, queries per second: %s%n", indexName, threadCount, count, minTerms, maxTerms, duration, count * 1000.0 / duration);
    }


    private void individualSearch(int threadCount, int count, int minTerms, int maxTerms, boolean log) {
        try {
            long start = System.currentTimeMillis();
            log |= Math.random() < 0.001;
            if (log) {
                System.out.printf("starting individualSearch for %s, %s, %s, %s, %s%n", indexName, threadCount, count, minTerms, maxTerms);
            }
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            Random random = new Random();
            for (int i = 0; i < minTerms + random.nextInt(maxTerms - minTerms + 1); i++) {
                boolQueryBuilder.must(QueryBuilders.termQuery(String.format(BOOL_FIELD_FMT, random.nextInt(boolFieldsCount)), true));
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
                System.out.printf("finished individualSearch for %s, %s, %s, %s, %s; query: %s, total hits: %s, took %s ms (measured locally: %s ms)%n",
                        indexName, threadCount, count, minTerms, maxTerms,
                        boolQueryBuilder.toString(),
                        searchResponse.getHits().getTotalHits().value,
                        searchResponse.getTook().getMillis(),
                        duration);
                for (var h : searchResponse.getHits().getHits()) {
                    System.out.println(new TreeMap<>(h.getSourceAsMap()));
                }
                //System.out.println(searchResponse);
            }

            totalResultsLowerBound.addAndGet(searchResponse.getHits().getTotalHits().value);
            totalQueryDuration.addAndGet(duration);

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

            tests.add(new TestElasticSearchPerformance(client, 10_000, 100_000,  20, 10_000_000, 5000, 500, 50, 2.00, "test01"));
            tests.add(new TestElasticSearchPerformance(client, 10_000,  50_000,  60,  1_000_000, 2000, 500, 50, 1.29, "test02"));
            tests.add(new TestElasticSearchPerformance(client, 10_000,  50_000, 100, 50_000_000,  100, 100, 50, 1.05, "test03"));
            tests.add(new TestElasticSearchPerformance(client, 10_000,  50_000, 100, 50_000_000, 2000, 500, 50, 1.25, "test04"));
            tests.add(new TestElasticSearchPerformance(client, 10_000,  50_000, 100, 50_000_000, 2000, 500, 50, 2.00, "test05"));

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
            System.out.println("run\tindex name\tbool fields count\tdocument count\tbase\tpercentage of true\tdisk space\tthread count\tquery count per thread\tmin terms\tmax terms\tduration\tqueries run\tqueries with results\ttotal results lower bound\tqueries per second\tpercentage of queries that had results\taverage results per query\taverage query duration");
            tests.forEach(TestElasticSearchPerformance::printResults);
        }
    }
}
