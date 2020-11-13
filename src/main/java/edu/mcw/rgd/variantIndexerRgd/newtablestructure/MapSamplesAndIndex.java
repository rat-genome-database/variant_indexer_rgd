package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.datamodel.variants.VariantObject;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.service.ESClient;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;
import java.util.Set;

public class MapSamplesAndIndex implements Runnable {
    private VariantIndex vi;
    List<VariantSampleDetail> samples;

    ProcessChromosome p=new ProcessChromosome();
    VariantDao dao=new VariantDao();

 /*   public MapSamplesAndIndex(List<VariantIndex> indexList, Set<Long> variantIds){
    this.indexList=indexList;
    this.variantIds=variantIds;
    }

    @Override
    public void run() {

    /*    List<VariantSampleDetail> samples = null;
        try {
            samples = dao.getSamples(variantIds);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if(samples!=null && samples.size()>0){
                for(VariantIndex vi:indexList){

                    for(VariantSampleDetail vsd:samples){
                        if(vsd.getId()==vi.getVariant_id()){

                            p. mapSampleDetails(vsd, vi);
                            try {
                                ObjectMapper mapper=new ObjectMapper();
                                String json =  mapper.writeValueAsString(vi);
                                BulkIndexProcessor.getBulkProcessor().add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));
                              /*  IndexRequest request=  new IndexRequest(RgdIndex.getNewAlias()).source(vi.toString(), XContentType.JSON);
                                ESClient.getClient().index(request, RequestOptions.DEFAULT);*/
                          /*  } catch (Exception e) {
                                System.err.println(vi.toString());
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }*/

  //  }*/
    public MapSamplesAndIndex(VariantObject vmd, VariantIndex vi, List<VariantSampleDetail> samples){

        this.vi=vi;
        this.samples=samples;
    }
    @Override
    public void run() {
     /*   List<VariantSampleDetail> samples = null;
        try {
            samples = dao.getSamples(vmd.getId());
        } catch (Exception e) {
            e.printStackTrace();
        }*/
        if (samples != null) {
            for (VariantSampleDetail vsd : samples) {
                if (vi != null) {
                   p. mapSampleDetails(vsd, vi);

                    try {
                        ObjectMapper mapper=new ObjectMapper();
                        byte[]  json =  mapper.writeValueAsBytes(vi);
                        BulkIndexProcessor.getBulkProcessor().add(  new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));
                      //  ESClient.getClient().index(request, RequestOptions.DEFAULT);
                    } catch (Exception e) {
                       System.out.println( "VARIANT ID:"+vi.getVariant_id());
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
