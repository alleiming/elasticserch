package com.test.es;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 * Created by allei on 16/9/2.
 */
public class TestDocumentApi {

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
     * 创建索引
     * @throws IOException
     */
    @Test
    public void testIndex() throws IOException {
        String json = XContentFactory.jsonBuilder()
                .startObject()
                .field("user", "dongxiaoyu")
                .field("postDate", new Date())
                .field("message", "trying out Elasticsearch")
                .endObject().string();
        IndexResponse indexResponse = client.prepareIndex("twitter", "tweet", "2").setSource(json).get();
        System.out.println(ToStringBuilder.reflectionToString(indexResponse, ToStringStyle.SHORT_PREFIX_STYLE));
    }

    /**
     * get获取索引
     */
    @Test
    public void testGet() {
        GetResponse response = client.prepareGet("twitter", "tweet", "1").get();
        System.out.println(response.getSource().get("message"));
    }

    /**
     * 删除索引
     */
    @Test
    public void testDelete() {
        DeleteResponse response = client.prepareDelete("twitter", "tweet", "1").get();
    }


    /**
     * 删除索引
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testUpdate() throws IOException, ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("twitter");
        updateRequest.type("tweet");
        updateRequest.id("4");
        updateRequest.doc(XContentFactory.jsonBuilder()
                .startObject()
                .field("user", "zhangsan")
                .endObject());
        client.update(updateRequest).get();

        /*UpdateRequest updateRequest = new UpdateRequest("twitter", "tweet", "1")
                .script(new Script("ctx._source.message = \"update by script\"", ScriptService.ScriptType.INLINE, null, null));
        client.update(updateRequest).get();*/
        testGet();
    }

    /**
     * 批量获取索引
     */
    @Test
    public void testMget() {
        MultiGetResponse multiGetResponse = client.prepareMultiGet().add(index, type, "1", "2", "3", "4").get();
        for (MultiGetItemResponse itemResponse : multiGetResponse) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                System.out.println(json);
            }
        }
    }

    /**
     * 批量新增索引
     * @throws IOException
     */
    @Test
    public void testBulk() throws IOException {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.
                add(client.prepareIndex("twitter", "tweet", "3").setSource(
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .field("user", "zhangsan")
                                .field("postDate", new Date())
                                .field("message", "trying out Elasticsearch")
                                .endObject().string()))
                .add(client.prepareIndex("twitter", "tweet", "4").setSource(
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .field("user", "lisi")
                                .field("postDate", new Date())
                                .field("message", "trying out Elasticsearch")
                                .endObject().string()));
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        System.out.println(ToStringBuilder.reflectionToString(bulkResponse, ToStringStyle.SHORT_PREFIX_STYLE));
    }

    /**
     * 批量新增索引，带回调方法
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testBulkProcessor() throws IOException, InterruptedException {
        BulkProcessor bulkProcessor = BulkProcessor.builder(client,
                new BulkProcessor.Listener() {
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                        System.out.println(request.numberOfActions());
                    }

                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        System.out.println(response.hasFailures());
                    }

                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        System.out.println(failure);
                    }
                })
                .setBulkActions(50)
                //.setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
        for (int i = 5; i < 100; i++) {
            IndexRequest indexRequest = new IndexRequest(index, type, String.valueOf(i));
            indexRequest.source(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("user", "testBulkProcessor" + i)
                    .field("postDate", new Date())
                    .field("message", "testBulkProcessor" + i)
                    .endObject().string());
            bulkProcessor.add(indexRequest);
        }
        boolean result = bulkProcessor.awaitClose(100, TimeUnit.MINUTES);
        System.out.println("result:" + result);
    }

}
