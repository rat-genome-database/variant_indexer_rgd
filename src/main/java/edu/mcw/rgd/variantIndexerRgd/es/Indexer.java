package edu.mcw.rgd.variantIndexerRgd.es;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndexObject;
import edu.mcw.rgd.variantIndexerRgd.service.ESClient;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Indexer  {
    private SearchHit[] hits;
    public Indexer(SearchHit[] hits){ this.hits=hits;}
  //  @Override
    public void run() {
        BulkRequest request = new BulkRequest();
        for(SearchHit h:hits) {
            Map source = h.getSourceAsMap();
            ObjectMapper mapper=new ObjectMapper();
            VariantIndexObject m=mapper.convertValue(source, VariantIndexObject.class);
            VariantMapData v=m.getVariant();
            List<VariantTranscript> vts= m.getVariantTranscripts();
            List<VariantSampleDetail> vsds=m.getSamples();

            for(VariantSampleDetail s:vsds){
                VariantIndex object=new VariantIndex();
                //   System.out.println(v.getChromosome()+"\t"+v.getStartPos()+"\t"+ v.getEndPos() + "\tTranscripts:"+vts.size()+ "\tSamples:"+vsds.size());
                object.setVariant_id(v.getId());
                object.setVarNuc(v.getVariantNucleotide());
                object.setRefNuc(v.getReferenceNucleotide());
                object.setChromosome(v.getChromosome());
                object.setMapKey(v.getMapKey());
                object.setStartPos(v.getStartPos());
                object.setEndPos(v.getEndPos());
                object.setGenicStatus(v.getGenicStatus());
                object.setPaddingBase(v.getPaddingBase());
                object.setVariantType(v.getVariantType());
                object.setRegionName(m.getRegionName());
                object.setRegionNameLc(m.getRegionNameLc());

                object.setVariantTranscripts(vts);
                /****************Sample details*********************/
               object.setQualityScore(s.getQualityScore());
               object.setSampleId(s.getSampleId());
               object.setTotalDepth(s.getDepth());
               object.setVarFreq(s.getVariantFrequency());
               object.setZygosityInPseudo(s.getZygosityInPseudo());
               object.setZygosityNumAllele(s.getZygosityNumberAllele());
               object.setZygosityPercentRead(s.getZygosityPercentRead());
               object.setZygosityPossError(s.getZygosityPossibleError());
               object.setZygosityRefAllele(s.getZygosityRefAllele());
               object.setZygosityStatus(s.getZygosityStatus());

                try {
                    ObjectMapper map=new ObjectMapper();
                    byte[] json = new byte[0];
                    json =  map.writeValueAsBytes(object);
                    request.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            try {
                ESClient.getClient().bulk(request, RequestOptions.DEFAULT);
                request=new BulkRequest();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
