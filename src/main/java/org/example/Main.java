package org.example;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Start");
        new Main().run();
        System.out.println("Done!");
    }

    public void run() {
        try {
            try (RestHighLevelClient client = createClientWithAuth()) {
                testBulk(client);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public RestHighLevelClient createClientWithAuth() {
        String username = "elastic";
        String password = "password";
        String host = "your.elasticsearch.host";
        int port = 9243;
        String protocol = "https";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder restClientBuilder = RestClient.builder(
                        new HttpHost(host, port, protocol))
                .setHttpClientConfigCallback((h) -> h.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(restClientBuilder);
    }

    public void testBulk(RestHighLevelClient client) throws Exception {
        // Connectivity test
        ClusterHealthResponse res = client.cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        System.out.println("Cluster name: " + res.getClusterName());

        // call elasticsearch's _bulk API
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("posts").id("1")
                .source(XContentType.JSON, "field", "foo"));
        request.add(new IndexRequest("posts").id("2")
                .source(XContentType.JSON, "field", "bar"));
        request.add(new IndexRequest("posts").id("3")
                .source(XContentType.JSON, "field", "baz"));
        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println("Bulk response: " + bulkResponse);
    }
}
