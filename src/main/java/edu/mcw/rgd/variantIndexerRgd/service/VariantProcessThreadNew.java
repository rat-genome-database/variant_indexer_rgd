package edu.mcw.rgd.variantIndexerRgd.service;

import edu.mcw.rgd.datamodel.Sample;
import edu.mcw.rgd.datamodel.Variant;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.process.Utils;

import edu.mcw.rgd.util.Zygosity;
import edu.mcw.rgd.variantIndexerRgd.VariantIndexerThread;

import edu.mcw.rgd.variantIndexerRgd.model.CommonFormat2Line;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.process.GeneCache;
import org.elasticsearch.action.index.IndexRequest;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;



/**
 * Created by jthota on 1/6/2020.
 */
public class VariantProcessThreadNew implements Runnable {


    private CommonFormat2Line line;
    private GeneCache geneCache;
    private int lineCount;
    private String strain;
    public static Map<Long, List<String>> geneLociMap;
  // VariantLoad3 loader=new VariantLoad3();
    Zygosity zygosity = new Zygosity();
    public VariantProcessThreadNew(CommonFormat2Line line, String strain, GeneCache geneCache, int lineCount){

        this.geneCache=geneCache;
        this.line=line;
        this.strain=strain;
        this.lineCount=lineCount;

    }
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()  + " ||  "+ lineCount+" started .... " + new Date());
        boolean isSnv = !Utils.isStringEmpty(line.getRefNuc()) && !Utils.isStringEmpty(line.getVarNuc());
        long endPos = 0;
        if (isSnv) {
            endPos =line.getPos() + 1;
        } else {
            // insertions
            if (Utils.isStringEmpty(line.getRefNuc())) {
                endPos = line.getPos();
            }
            // deletions
            else if (Utils.isStringEmpty(line.getVarNuc())) {
                endPos =line.getPos() + line.getRefNuc().length();
            } else {
                System.out.println("Unexpected var type");
            }
        }
        if (!alleleIsValid(line.getRefNuc())) {
            //   System.out.println(" *** Ref Nucleotides must be A,C,G,T,N");
            return;
        }
        if (!alleleIsValid(line.getVarNuc())) {
            //     System.out.println(" *** Var Nucleotides must be A,C,G,T,N");
            return;
        }


        List<VariantTranscript> variantTranscripts = new ArrayList<>();

        try {
            variantTranscripts .addAll(getVariantTranscripts(line.getPos()));

        } catch (IOException e) {
            e.printStackTrace();
        }

        // NOTE: for snvs, only ACGT counts are provided
        //    for indels, only allele count is provided

        int totalDepth = 0;
        String totalDepthStr = String.valueOf(line.getTotalDepth());
        if (totalDepthStr == null || totalDepthStr.isEmpty()) {
            if (isSnv)
                totalDepth = line.getCountA() + line.getCountC() + line.getCountG() + line.getCountT();
            else
                totalDepth = line.getReadDepth();
        } else
            totalDepth = Integer.parseInt(totalDepthStr);

        // total reads called (AD field) vs total reads analyzed (DP field): 100*readDepth/totalDepth
        int qualityScore = 0;
        if (totalDepth > 0) {
            qualityScore = (100 *line.getReadDepth() + totalDepth / 2) / totalDepth;
        }



        List<Integer> geneRgdIds = geneCache.getGeneRgdIds(line.getPos());
        String genicStatus = !geneRgdIds.isEmpty() ? "GENIC" : "INTERGENIC";
        Sample sample = VariantIndexerThread.sampleIdMap.get(strain);

            Variant v = new Variant();
            v.setChromosome(line.getChr());
            v.setReferenceNucleotide(line.getRefNuc());
            v.setStartPos(line.getPos());
            v.setDepth(line.getReadDepth());
            v.setVariantFrequency(line.getAlleleDepth());
            v.setVariantNucleotide(line.getVarNuc());
            v.setSampleId(sample.getId());
            v.setQualityScore(qualityScore);
            v.setHgvsName(line.getHgvsName());

            v.setVariantType(determineVariantType(line.getRefNuc(), line.getVarNuc()));

            v.setGenicStatus(genicStatus);
            if (!isSnv) {
             //   v.setPaddingBase(line.getPaddingBase());
            }

            // Determine the ending position

            v.setEndPos(endPos);

            int score = 0;
            if (isSnv) {
                score = zygosity.computeVariant(line.getCountA(), line.getCountC(), line.getCountG(), line.getCountT(), sample.getGender(), v);
            } else {
                //         // parameter tweaking for indels
                zygosity.computeZygosityStatus(line.getAlleleDepth(), line.getReadDepth(), sample.getGender(), v);
//
                // compute zygosity ref allele, if possible
                if (line.getRefNuc().equals("A")) {
                    v.setZygosityRefAllele(line.getCountA() > 0 ? "Y" : "N");
                } else if (line.getRefNuc().equals("C")) {
                    v.setZygosityRefAllele(line.getCountC() > 0 ? "Y" : "N");
                } else if (line.getRefNuc().equals("G")) {
                    v.setZygosityRefAllele(line.getCountG() > 0 ? "Y" : "N");
                } else if (line.getRefNuc().equals("T")) {
                    v.setZygosityRefAllele(line.getCountT() > 0 ? "Y" : "N");
                }

                if (line.getAlleleDepth() == 0)
                    score = 0;
                else
                    score = v.getZygosityPercentRead();
            }
            if (score == 0) {
               return;
            }


            v.setRsId(line.getRsId());

            try {
                VariantIndex obj=getIndexObject(v, variantTranscripts);
                com.fasterxml.jackson.databind.ObjectMapper mapper=new com.fasterxml.jackson.databind.ObjectMapper();
                String json =  mapper.writeValueAsString(obj);
                IndexRequest request= new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON);
                ESClient.getClient().index(request, RequestOptions.DEFAULT);

            } catch (IOException e) {
                e.printStackTrace();
            }

    }
    public  VariantIndex getIndexObject(Variant v, List<VariantTranscript> vts) throws IOException {
        VariantIndex variantIndexObj=new VariantIndex();
        variantIndexObj.setChromosome(v.getChromosome());
        variantIndexObj.setStartPos(v.getStartPos());
        variantIndexObj.setEndPos(v.getEndPos());
        variantIndexObj.setRefNuc(v.getReferenceNucleotide());
        variantIndexObj.setVarNuc(v.getVariantNucleotide());
        variantIndexObj.setSampleId(v.getSampleId());
        variantIndexObj.setVariantType(v.getVariantType());
        variantIndexObj.setZygosityInPseudo(v.getZygosityInPseudo());
        variantIndexObj.setQualityScore(v.getQualityScore());
        variantIndexObj.setTotalDepth(v.getDepth());
        variantIndexObj.setVarFreq(v.getVariantFrequency());
        variantIndexObj.setRsId(v.getRsId());
        List<String> regionNames=geneLociMap.get(v.getStartPos());
        if(regionNames!=null && regionNames.size()!=0) {
            variantIndexObj.setRegionName(regionNames);
            List<String> regionNameLC = new ArrayList<>();
            for (String name : regionNames) {
                regionNameLC.add(name);
            }
            variantIndexObj.setRegionNameLc(regionNameLC);
        }
        if(vts!=null && vts.size()!=0)
            variantIndexObj.setVariantTranscripts(vts);
        return variantIndexObj;
    }
    public  boolean alleleIsValid(String allele) {
        for( int i=0; i<allele.length(); i++ ) {
            char c = allele.charAt(i);
            if( c=='A' || c=='C' || c=='G' || c=='T' || c=='N' || c=='-' )
                continue;
            return false;
        }
        return true;
    }
    public String determineVariantType(String refSeq, String varSeq) {

        // handle insertions
        if( refSeq.length()==0 )
            return "ins";

        // handle deletions
        if( varSeq.length()==0 )
            return "del";

        // handle snv
        return "snv";
    }
    public List<VariantTranscript> getVariantTranscripts(long startPos) throws IOException {
        SearchSourceBuilder srb=new SearchSourceBuilder();
        srb.query(QueryBuilders.termQuery("startPos", startPos));
        //    SearchRequest request=new SearchRequest("transcripts_human_dev1"); //chr 21 transcripts
        SearchRequest request=new SearchRequest("transcripts_human_test2"); // chr 1 transcripts
        request.source(srb);

        //   RestHighLevelClient client=ESClient.getInstance();
        SearchResponse sr=ESClient.getClient().search(request, RequestOptions.DEFAULT);
        List<VariantTranscript> tds= new ArrayList<>();
        for(SearchHit h:sr.getHits().getHits()){
            Map source=h.getSourceAsMap();
            VariantTranscript td=new VariantTranscript();
            td.setTripletError((String) source.get("tripletError"));
            td.setSynStatus((String) source.get("synStatus"));
            td.setPolyphenStatus((String) source.get("polyphenStatus"));
            td.setNearSpliceSite((String) source.get("nearSpliceSite"));
            td.setGenespliceStatus((String) source.get("genespliceStatus"));
            td.setFrameShift((String) source.get("frameShift"));
            td.setTranscriptRgdId((Integer) source.get("transcriptRgdId"));
            td.setLocationName((String) source.get("locationName"));
            td.setRefAA((String) source.get("refAA"));
            td.setVarAA((String) source.get("varAA"));
            tds.add(td);

        }
        // System.out.println("TRANSCIPTS SIZE: "+tds.size());
        return tds;
    }
}
