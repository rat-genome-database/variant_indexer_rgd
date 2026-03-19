package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.datamodel.RgdIndex;
import edu.mcw.rgd.datamodel.variants.VariantObject;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;

public class MapSamplesAndIndex implements Runnable {
    private VariantIndex vi;
    List<VariantSampleDetail> samples;

    public MapSamplesAndIndex(VariantObject vmd, VariantIndex vi, List<VariantSampleDetail> samples){
        this.vi=vi;
        this.samples=samples;
    }
    @Override
    public void run() {
        if (samples != null) {
            for (VariantSampleDetail vsd : samples) {
                if (vi != null) {
                    VariantIndexUtils.mapSampleDetails(vsd, vi);

                    try {
                        ObjectMapper mapper=new ObjectMapper();
                        byte[]  json =  mapper.writeValueAsBytes(vi);
                        BulkIndexProcessor.getInstance().bulkProcessor.add(  new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));
                    } catch (Exception e) {
                       System.out.println( "VARIANT ID:"+vi.getVariant_id());
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
