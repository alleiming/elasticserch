package com.test.es;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by allei on 16/9/3.
 */
public class TestQueryApi {

    private TransportClient client;

    private String index = "twitter";
    private String type = "tweet";

    @Before
    public void before() throws UnknownHostException {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.ping_timeout", "20s")
                .build();
        client = TransportClient.builder().settings(settings).build().
                addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.109"), 9300));
    }

    @After
    public void after() {
        client.close();
    }

    /**
     * 滚动翻页查询
     */
    @Test
    public void testScroll() {
        QueryBuilder termQueryBuilder = QueryBuilders.fuzzyQuery("user", "testBulkProcessor6");
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).setScroll(new TimeValue(60000))
                .setQuery(termQueryBuilder).setSize(5).execute().actionGet();
        while (true) {
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                System.out.println(hit.getSource());
            }
            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            //Break condition: No hits are returned
            if (searchResponse.getHits().getHits().length == 0) {
                break;
            }
        }
    }

    /**
     * 批量查询
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testMSearch() throws ExecutionException, InterruptedException {
        SearchRequestBuilder b1 = client.prepareSearch().setQuery(QueryBuilders.queryStringQuery("mingli"));
        SearchRequestBuilder b2 = client.prepareSearch().setQuery(QueryBuilders.matchQuery("user", "dongxiaoyu"));

        MultiSearchResponse multiSearchResponse = client.prepareMultiSearch().add(b1).add(b2).execute().get();
        for (MultiSearchResponse.Item item : multiSearchResponse.getResponses()) {
            SearchResponse response = item.getResponse();
            for (SearchHit hit : response.getHits().getHits()) {
                System.out.println(hit.getSource());
            }
        }
    }

    /**
     * 聚合查询-group by
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testAggregation() throws ExecutionException, InterruptedException {
        SearchResponse searchResponse = client.prepareSearch().setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(AggregationBuilders.terms("user").field("user"))
                .addAggregation(AggregationBuilders.dateHistogram("postDate").field("postDate").interval(DateHistogramInterval.MINUTE))
                .execute().actionGet();
        Terms user = searchResponse.getAggregations().get("user");
        List<Terms.Bucket> buckets = user.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            System.out.println("key:" + bucket.getKey());
            System.out.println("key:" + bucket.getDocCount());
        }
        InternalHistogram postDate = searchResponse.getAggregations().get("postDate");
        List postDateBuckets = postDate.getBuckets();
        for (Object o : postDateBuckets) {
            System.out.println(ToStringBuilder.reflectionToString(o, ToStringStyle.SHORT_PREFIX_STYLE));
        }
    }
}
