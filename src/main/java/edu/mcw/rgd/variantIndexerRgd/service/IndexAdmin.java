package edu.mcw.rgd.variantIndexerRgd.service;

import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by jthota on 11/15/2019.
 */
public class IndexAdmin {
    private static Logger log=Logger.getLogger("main");
    private RgdIndex rgdIndex;
    public void createIndex(String mappings, String type) throws Exception {
        GetAliasesRequest aliasesRequest=new GetAliasesRequest(rgdIndex.getIndex());
        boolean existsAlias = ESClient.getClient().indices().existsAlias(aliasesRequest, RequestOptions.DEFAULT);
        if(existsAlias) {
            for (String index : rgdIndex.getIndices()) {
                aliasesRequest.indices(index);
                existsAlias = ESClient.getClient().indices().existsAlias(aliasesRequest, RequestOptions.DEFAULT);
                if (!existsAlias) {
                    RgdIndex.setNewAlias(index);
                    GetIndexRequest request1 = new GetIndexRequest(index);
                    boolean indexExists = ESClient.getClient().indices().exists(request1, RequestOptions.DEFAULT);

                    if (indexExists) {   /**** delete index if exists ****/

                        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
                        ESClient.getClient().indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
                        log.info(index + " deleted");
                    }
                    createNewIndex(index, mappings, type);
                }else {
                    RgdIndex.setOldAlias(index);
                }

            }
        }else{
            GetIndexRequest request1=new GetIndexRequest(rgdIndex.getIndex()+"1");
            boolean indexExists=ESClient.getClient().indices().exists(request1, RequestOptions.DEFAULT);
            if (indexExists) {   /**** delete index if exists ****/

                DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(rgdIndex.getIndex()+"1");
                ESClient.getClient().indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
                log.info(rgdIndex.getIndex()+"1" + " deleted");
            }
            createNewIndex(rgdIndex.getIndex()+"1",mappings, type);

        }

    }
    public void createNewIndex(String index, String _mappings, String type) throws Exception {

        String path="data/"+_mappings+".json";
        log.info("CREATING NEW INDEX..." + index);

//        String mappings=new String(Files.readAllBytes(Paths.get(path)));
  //      String analyzers=new String(Files.readAllBytes(Paths.get("data/analyzers.json")));

        /********* create index, put mappings and analyzers ****/
        CreateIndexRequest request=new CreateIndexRequest(index);
        request.settings(Settings.builder()
                .put("index.number_of_shards",5)
                .put("index.number_of_replicas", 0))
               // .loadFromSource(analyzers, XContentType.JSON))
        ;
    //    request.mapping(mappings, XContentType.JSON);
        org.elasticsearch.client.indices.CreateIndexResponse createIndexResponse = ESClient.getClient().indices().create(request, RequestOptions.DEFAULT);
        log.info(index + " created on  " + new Date());
        System.out.println(index + " created on  " + new Date());
        RgdIndex.setNewAlias(index);
    }
    public int updateIndex() throws Exception {
        if(rgdIndex.getIndex()!=null) {
            log.info("Updating " + rgdIndex.getIndex() + "...");
            System.out.println("Updating " + rgdIndex.getIndex() + "...");
            GetIndexRequest request=new GetIndexRequest(rgdIndex.getIndex());
            boolean indicesExists=ESClient.getClient().indices().exists(request, RequestOptions.DEFAULT);
            if (indicesExists) {  /* CHECK IF INDEX NAME PROVIDED EXISTS*/

                RgdIndex.setNewAlias(rgdIndex.getIndex());

                return 1;
            } else {
                log.info("Cannot Update. " + rgdIndex.getIndex() + " does not exists. Use REINDEX option to create index");
                System.out.println("Cannot Update. " + rgdIndex.getIndex() + " does not exists. Use REINDEX option to create index");
                return 0;
            }
        }else {
            log.info("INDEX cannot be null");
            System.out.println("INDEX cannot be null");
            return 0;
        }
    }


    public void setRgdIndex(RgdIndex rgdIndex) {
        this.rgdIndex = rgdIndex;
    }

    public RgdIndex getRgdIndex() {
        return rgdIndex;
    }


    public static void main(String[] args) throws IOException {
        IndexAdmin admin= new IndexAdmin();

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));

        ESClient es= (ESClient) bf.getBean("client");
        admin.rgdIndex= (RgdIndex) bf.getBean("rgdIndex");
        List<String> indices= new ArrayList<>();
        admin.rgdIndex.setIndex("rgd_index_"+ "dev");
        indices.add("rgd_index_"+"dev"+"1");
        indices.add("rgd_index_"+"dev"+"2");
        admin.rgdIndex.setIndices(indices);


        Logger log= Logger.getLogger(IndexAdmin.class);
        try {
            admin.createIndex("","");
        } catch (Exception e) {

            e.printStackTrace();
        }

        es.destroy();
    }
}
