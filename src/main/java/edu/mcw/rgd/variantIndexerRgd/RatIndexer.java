package edu.mcw.rgd.variantIndexerRgd;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.dao.impl.GeneLociDAO;
import edu.mcw.rgd.datamodel.GeneLoci;
import edu.mcw.rgd.datamodel.variants.VariantMapData;

import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantData;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndexObject;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;


import java.util.*;


public class RatIndexer implements Runnable {
    VariantDao variantDao=new VariantDao();

    private VariantMapData v;
    Map<Long, List<String>> geneLoci;
    Variants variants=new Variants();
    public RatIndexer(){}
    public RatIndexer(VariantMapData v, Map<Long, List<String>> geneLoci){
        this.v=v;
        this.geneLoci=geneLoci;

    }
        public void run() {
            Logger log=Manager.log;
            try {

                System.out.println(Thread.currentThread().getName()+"\tMAPKEY:"+ v.getMapKey() +"\tCHR: "+v.getChromosome() +"\tSTART POS: "+ v.getStartPos() );
                VariantIndexObject object = new VariantIndexObject();
                List<String> regionNames=geneLoci.get(v.getStartPos());
                if(regionNames!=null && regionNames.size()!=0) {
                    object.setRegionName(regionNames);
                    List<String> regionNameLC = new ArrayList<>();
                    for (String name : regionNames) {
                        regionNameLC.add(name.toLowerCase());
                    }
                    object.setRegionNameLc(regionNameLC);
                }

                object.setVariant(v);
                try {
                    List<VariantData> records = variants.getVariants(v.getSpeciesTypeKey(), v.getChromosome(), v.getMapKey(),(int) v.getId());
                    VariantIndexObject o=variantDao.mapSamplesNTranscripts(records, object);
                    object.setSamples(o.getSamples());
                    object.setVariantTranscripts(o.getVariantTranscripts());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                ObjectMapper mapper=new ObjectMapper();
                byte[] json = new byte[0];
                try {
                    json = mapper.writeValueAsBytes(object);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                IndexRequest request= new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON);
                ESClient.getClient().index(request, RequestOptions.DEFAULT);
                log.info(Thread.currentThread().getName()+"\tMAPKEY:"+ v.getMapKey() +"\tCHR: "+v.getChromosome() +"\tSTART POS: "+ v.getStartPos() +"\tEND!!" );

            } catch (Exception e) {
                e.printStackTrace();
            }


    }
    public static Map<Long, List<String>> getGeneLociMap(int mapKey, String chromosome) throws Exception {
        GeneLociDAO dao= new GeneLociDAO();
        List<GeneLoci> loci=dao.getGeneLociByMapKeyAndChr(mapKey, chromosome);
        Map<Long, List<String>> positionGeneMap=new HashMap<>();

        for(GeneLoci g: loci){
            List<String> list=new ArrayList<>();
            if(positionGeneMap.get(g.getPosition())!=null){
                list=positionGeneMap.get(g.getPosition());

            }
            list.add(g.getGeneSymbols());
            positionGeneMap.put(g.getPosition(), list);
        }
        System.out.println("GeneLoci Map size: "+ positionGeneMap.size());

        return positionGeneMap;
    }


    public BulkProcessor getBulkProcessor(){
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                //        System.out.println("ACTIONS: "+request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {
                //     System.out.println("in process...");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  Throwable failure) {

            }
        };
         return BulkProcessor.builder(
                (request, bulkListener) ->
                        ESClient.getClient().bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener)
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

    }

}
