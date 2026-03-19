package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import edu.mcw.rgd.datamodel.RgdIndex;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.variantIndexerRgd.model.Json;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;

public class BulkIndexer implements Runnable {
    private VariantIndex vi;
    private VariantMapData vmd;
    List<VariantSampleDetail> samples;
    public BulkIndexer(VariantIndex vi, VariantMapData vamd, List<VariantSampleDetail> samples) {
      this.vi=vi;
      this.vmd=vamd;
      this.samples=samples;
    }

    @Override
    public void run() {
        List<VariantSampleDetail> variantSamplesDetails = VariantIndexUtils.getSamplesForVariant(vmd.getId(), samples);
        for (VariantSampleDetail vsd : variantSamplesDetails) {
            VariantIndexUtils.mapSampleDetails(vsd, vi);
            try {
                byte[] json = Json.serializer().mapper().writeValueAsBytes(vi);
                BulkIndexProcessor.bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    }
}
