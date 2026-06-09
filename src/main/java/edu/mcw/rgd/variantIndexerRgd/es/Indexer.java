package edu.mcw.rgd.variantIndexerRgd.es;

import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndexObject;
import edu.mcw.rgd.variantIndexerRgd.newtablestructure.BulkIndexProcessor;

import java.util.List;
import java.util.Map;

public class Indexer  {
    private List<Hit<Map>> hits;
    public Indexer(List<Hit<Map>> hits){ this.hits=hits;}
  //  @Override
    public void run() {
        BulkIngester<Void> bulkProcessor = BulkIndexProcessor.newIngester(1);
        try {
        for(Hit<Map> h:hits) {
            Map source = h.source();
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
                    bulkProcessor.add(BulkIndexProcessor.indexOp(object));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        } finally {
            bulkProcessor.close();
        }

    }
}
