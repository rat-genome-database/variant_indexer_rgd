package edu.mcw.rgd.variantIndexerRgd.es;

import edu.mcw.rgd.datamodel.SpeciesType;

import edu.mcw.rgd.variantIndexerRgd.service.ESClient;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;


public class ESDataService {
    public void indexVariants(int mapKey, String chr, int speciesTypeKey) throws IOException {
        String index="variants_"+ SpeciesType.getCommonName(speciesTypeKey).toLowerCase()+mapKey+"_dev";
        System.out.println("Qyery INDEX:  " +index);
        SearchSourceBuilder srb=new SearchSourceBuilder();
        srb.query(QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("variant.chromosome", chr))
                    .filter(QueryBuilders.termQuery("variant.mapKey", mapKey)));
        srb.size(100);

        SearchRequest request=new SearchRequest(index);
        request.source(srb);
        request.scroll(TimeValue.timeValueMinutes(1L));

//        ExecutorService executor= new MyThreadPoolExecutor(3,3,0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

        SearchResponse sr= ESClient.getClient().search(request, RequestOptions.DEFAULT);
        String scrollId=sr.getScrollId();
        Indexer indexer=new Indexer(sr.getHits().getHits());
        indexer.run();
      /*  Runnable workerThread=new Indexer(sr.getHits().getHits());
        executor.execute(workerThread);*/
        do {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueSeconds(60));
            sr = ESClient.getClient().scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = sr.getScrollId();
         /*    workerThread=new Indexer(sr.getHits().getHits());
             executor.execute(workerThread);*/
         indexer=new Indexer(sr.getHits().getHits());
         indexer.run();
        }while (sr.getHits().getHits().length!=0);
     /*   executor.shutdown();
        while (!executor.isTerminated()) {}*/
     /*   for(SearchHit h:sr.getHits().getHits()) {
            Map source = h.getSourceAsMap();
            ObjectMapper mapper=new ObjectMapper();
            VariantIndexObject m=mapper.convertValue(source, VariantIndexObject.class);
            VariantMapData v=m.getVariant();
            List<VariantTranscript> vts= m.getVariantTranscripts();
            List<VariantSampleDetail> vsds=m.getSamples();
            System.out.println(v.getChromosome()+"\t"+v.getStartPos()+"\t"+ v.getEndPos() + "\tTranscripts:"+vts.size()+ "\tSamples:"+vsds.size());

        }*/
    }
}
