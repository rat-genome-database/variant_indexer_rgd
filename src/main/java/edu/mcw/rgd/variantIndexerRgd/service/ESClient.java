package edu.mcw.rgd.variantIndexerRgd.service;

import edu.mcw.rgd.variantIndexerRgd.Manager;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

/**
 * Created by jthota on 11/15/2019.
 */
public class ESClient {
    private static Logger log=Logger.getLogger(Manager.class);
    private static RestHighLevelClient client = null;
    private ESClient(){}
    public void init(){
        System.out.println("Initializing Elasticsearch Client...");
        client=getInstance();
    }
    public static void destroy() throws IOException {
        System.out.println("destroying Elasticsearh Client...");
        if(client!=null) {
            try{
                client.close();
                client = null;
            }catch (Exception e){
                log.info(e);
            }

        }
    }

    public static RestHighLevelClient getClient() {
        return getInstance();
    }

    public static void setClient(RestHighLevelClient client) {
        ESClient.client = client;
    }

    public static RestHighLevelClient getInstance() {

        if(client==null){

            try {
              /*  client= new PreBuiltTransportClient(settings)
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("green.rgd.mcw.edu"), 9300));*/
                client=new RestHighLevelClient(RestClient.builder(
                        new HttpHost("green.rgd.mcw.edu", 9200, "http")

                ).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback(){

                    @Override
                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                        return requestConfigBuilder
                                .setConnectTimeout(5000)
                                .setSocketTimeout(120000)
                                ;
                    }
                })
                );

            } catch (Exception e) {
                log.info(e);
                e.printStackTrace();
            }

        }

        return client;
    }

}
