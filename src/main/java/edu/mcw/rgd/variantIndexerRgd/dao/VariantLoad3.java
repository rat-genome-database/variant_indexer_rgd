package edu.mcw.rgd.variantIndexerRgd.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.datamodel.Sample;
import edu.mcw.rgd.datamodel.Transcript;
import edu.mcw.rgd.datamodel.Variant;
import edu.mcw.rgd.process.FastaParser;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.util.Zygosity;

import edu.mcw.rgd.variantIndexerRgd.VariantIndexerThread;
import edu.mcw.rgd.variantIndexerRgd.model.*;
import edu.mcw.rgd.variantIndexerRgd.process.GeneCache;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;

import org.elasticsearch.client.RequestOptions;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.io.IOException;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by jthota on 11/15/2019.
 */
public class VariantLoad3 {
    private Zygosity zygosity = new Zygosity();
    VariantTranscriptDao vtdao= new VariantTranscriptDao();
    public static Map<Long, List<String>> geneLociMap;

    public List<VariantTranscript> processVariantTranscript(String chr, int varStart, int varStop, String refNuc, String variantNuc, int mapKey, GeneCache geneCache, String rsId, FastaParser fastaFile) throws Exception {

        List<VariantTranscript> vts= new ArrayList<>();

        for(int geneRgdId:geneCache.getGeneRgdIds(varStart)){
            // Iterate over all transcripts for this gene
         //   Map<Integer, String> map=vtdao.getTranscriptsResultSet(geneRgdId,mapKey );
            List<Transcript> transcripts= vtdao.getTranscriptsResult(geneRgdId, mapKey);
            System.out.println("TRANSCRIPTS SIZE: "+ transcripts.size());
        //  for(Map.Entry e: map.entrySet()) {
              for(Transcript t:transcripts) {
               // int transcriptRgdId = (int) e.getKey();
                  int transcriptRgdId = t.getRgdId();

                // Get count of exons as we need to ignore the last position of the last exon
                int totalExonCount = vtdao.getExonCount(transcriptRgdId, chr, mapKey);
          //      System.out.print("\t"+ totalExonCount);
                TranscriptFlags tflags = new TranscriptFlags();


                // See if we have a Coding region
              //  String isNonCodingRegion = (String) e.getValue();
                  boolean isNonCodingRegion = t.isNonCoding();

                vtdao.processFeatures(transcriptRgdId, chr, 17, tflags, varStart, varStop, totalExonCount);

                // not found means it was in an INTRON Region
                if (!tflags.inExon) {
                    if (tflags.transcriptLocation != null) {
                        tflags.transcriptLocation += ",INTRON";
                    } else {
                        tflags.transcriptLocation = "INTRON";
                    }
                }


                // If not in Exome log and continue
                boolean doInsert = false;
            //    if (!tflags.inExon || isNonCodingRegion.equals("Y")) {
                  if (!tflags.inExon || isNonCodingRegion) {
                    if (isNonCodingRegion) {
                        if (tflags.transcriptLocation != null) {
                            tflags.transcriptLocation += ",NON-CODING";
                        } else {
                            tflags.transcriptLocation = "NON-CODING";
                        }
                    }
                    doInsert = true;
                }
                else {

                   VariantTranscript transcript = vtdao.processTranscript(tflags, transcriptRgdId, fastaFile,  varStart, varStop, /*variantId*/0, refNuc, variantNuc);
                    if( transcript !=null) {
                    transcript.setChromosome(chr);
                    transcript.setStartPos(varStart);
                    transcript.setStopPos(varStop);
                    transcript.setRefNuc(refNuc);
                    transcript.setVarNuc(variantNuc);
                    transcript.setRsId(rsId);
                    transcript.setProteinId(t.getProteinAccId());
                        vts.add(transcript);

                    }
                }

                if( doInsert ) {
                    VariantTranscript transcript=vtdao.insertVariantTranscript(/*variantId*/0, transcriptRgdId, tflags.transcriptLocation, tflags.nearSpliceSite);
                    if( transcript !=null) {
                        transcript.setChromosome(chr);
                        transcript.setStartPos(varStart);
                        transcript.setStopPos(varStop);
                        transcript.setRefNuc(refNuc);
                        transcript.setVarNuc(variantNuc);
                        transcript.setRsId(rsId);
                        transcript.setProteinId(t.getProteinAccId());
                        vts.add(transcript);
                    }
                    // No need to determine Amino Acids if variant not in coding part of exon
                }
            }

        }
        return vts;
    }
   public void processLine(CommonFormat2Line line, GeneCache geneCache, String processName) throws Exception {
        String chr = line.getChr();
        int position = line.getPos();
        String refNuc = line.getRefNuc(); // reference nucleotide for snvs (or ref sequence for indels)
        String varNuc = line.getVarNuc(); // variant nucleotide for snvs (or var sequence for indels)
       String rsId=line.getRsId();
        boolean isSnv = !Utils.isStringEmpty(refNuc) && !Utils.isStringEmpty(varNuc);
      ;

        long endPos = 0;
        if( isSnv ) {

            if (isSnv) {
                endPos =position + 1;
            } else {
                // insertions
                if (Utils.isStringEmpty(refNuc)) {
                    endPos = position;
                }
                // deletions
                else if (Utils.isStringEmpty(varNuc)) {
                    endPos = position + refNuc.length();
                } else {
                    System.out.println("Unexpected var type");
                }
            }
            if (!alleleIsValid(refNuc)) {
                System.out.println(" *** Ref Nucleotides must be A,C,G,T,N");
                return;
            }
            if (!alleleIsValid(varNuc)) {
                System.out.println(" *** Var Nucleotides must be A,C,G,T,N");
                return;
            }
        }

       switch (processName){
           case "VariantTranscript":

               List<VariantTranscript> vts = processVariantTranscript(chr,position ,(int)endPos, refNuc, varNuc, 17, geneCache, rsId,null);

               if(vts.size()>0) {
                   indexTranscripts(vts);
               }

               break;
           case "Variant":
               List<edu.mcw.rgd.datamodel.variants.VariantTranscript> variantTranscripts= this.getVariantTranscripts(position, chr, refNuc, varNuc);

               // NOTE: for snvs, only ACGT counts are provided
               //    for indels, only allele count is provided
               int alleleDepth =line.getAlleleDepth(); // from AD field: how many times allele was called
               int readDepth =line.getReadDepth(); // from AD field: how many times all alleles were called
               int readCountA =line.getCountA();
               int readCountC = line.getCountC();
               int readCountG = line.getCountG();
               int readCountT = line.getCountT();

               int totalDepth = 0;
               String totalDepthStr = line.getTotalDepth().toString();
               if( totalDepthStr==null || totalDepthStr.isEmpty() ) {
                   if( isSnv )
                       totalDepth = readCountA + readCountC + readCountG + readCountT;
                   else
                       totalDepth = readDepth;
               } else
                   totalDepth = Integer.parseInt(totalDepthStr);

               // total reads called (AD field) vs total reads analyzed (DP field): 100*readDepth/totalDepth
               int qualityScore = 0;
               if( totalDepth>0 ) {
                   qualityScore = (100 * readDepth + totalDepth/2)/ totalDepth;
               }

               String hgvsName = line.getHgvsName();
               String varRgdId = new String();
               if(line.getRgdId()!=null)
                   varRgdId=   line.getRgdId().toString();
               List<VariantIndex> objects= new ArrayList<>();
               List<Integer> geneRgdIds= geneCache.getGeneRgdIds(position);
               String genicStatus=!geneRgdIds.isEmpty()?"GENIC":"INTERGENIC";
               for(String s:line.getStrainList()) {
                   Sample sample= VariantIndexerThread.sampleIdMap.get(s);

                   Variant v = new Variant();
                   v.setChromosome(chr);
                   v.setReferenceNucleotide(refNuc);
                   v.setStartPos(position);
                   v.setDepth(readDepth);
                   v.setVariantFrequency(alleleDepth);
                   v.setVariantNucleotide(varNuc);
                   v.setSampleId(sample.getId());
                   v.setQualityScore(qualityScore);
                   v.setHgvsName(hgvsName);
                   if (!varRgdId.isEmpty())
                       v.setRgdId(Integer.parseInt(varRgdId));
                   v.setVariantType(determineVariantType(refNuc, varNuc));

                   v.setGenicStatus(genicStatus);
                   if (!isSnv) {
                       v.setPaddingBase(line.getPaddingBase());
                   }

                   // Determine the ending position

                   v.setEndPos(endPos);

                   int score = 0;
                   if (isSnv) {
                       score = zygosity.computeVariant(readCountA, readCountC, readCountG, readCountT, sample.getGender(), v);
                   } else {
                       //         // parameter tweaking for indels
                       zygosity.computeZygosityStatus(alleleDepth, readDepth, sample.getGender(), v);
//
                       // compute zygosity ref allele, if possible
                       if (refNuc.equals("A")) {
                           v.setZygosityRefAllele(readCountA > 0 ? "Y" : "N");
                       } else if (refNuc.equals("C")) {
                           v.setZygosityRefAllele(readCountC > 0 ? "Y" : "N");
                       } else if (refNuc.equals("G")) {
                           v.setZygosityRefAllele(readCountG > 0 ? "Y" : "N");
                       } else if (refNuc.equals("T")) {
                           v.setZygosityRefAllele(readCountT > 0 ? "Y" : "N");
                       }

                       if (alleleDepth == 0)
                           score = 0;
                       else
                           score = v.getZygosityPercentRead();
                   }
                   if (score == 0) {
                       return;
                   }


                   v.setRsId(rsId);

                   objects.add(getIndexObject(v, variantTranscripts));
               }

               index(objects);

               break;
           default:
       }

        return;
        //  if( saveVariant(v) ) {
        //   System.out.println("var ins c"+v.getChromosome()+":"+v.getStartPos()+" "+v.getReferenceNucleotide()+"=>"+v.getVariantNucleotide());
        //  }


    }
    public void index(List<VariantIndex> vrs) throws IOException {

        if(vrs.size()>0){
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

            BulkProcessor bulkProcessor = BulkProcessor.builder(
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


        //   final ObjectMapper mapper = new ObjectMapper();

            for (VariantIndex o : vrs) {


                byte[] json = new byte[0];
                try {
                    ObjectMapper mapper=new ObjectMapper();
                    json =  mapper.writeValueAsBytes(o);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));
             }


            try {
                bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
                bulkProcessor.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }finally {
                bulkProcessor.close();
            }

            //        System.out.println("Indexed mapKey " + mapKey + ", chromosome: "+ chromosome+", Variant objects Size: " + vrs.size() + " Exiting thread.");
            //        System.out.println(Thread.currentThread().getName() + ": VariantThread" + mapKey +"\tSample: "+sampleId+"\tChromosome: "+chromosome+ " End " + new Date());

        }
    }
    public void indexTranscripts(List<VariantTranscript> vts) throws IOException {
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

        BulkProcessor bulkProcessor = BulkProcessor.builder(
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

        for (VariantTranscript o : vts) {

                ObjectMapper mapper = new ObjectMapper();
                String json = new String();
                try {
                    json = mapper.writeValueAsString(o);
                    bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

            }
        try {
            bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
            bulkProcessor.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            bulkProcessor.close();
        }


            //        System.out.println("Indexed mapKey " + mapKey + ", chromosome: "+ chromosome+", Variant objects Size: " + vrs.size() + " Exiting thread.");
            //        System.out.println(Thread.currentThread().getName() + ": VariantThread" + mapKey +"\tSample: "+sampleId+"\tChromosome: "+chromosome+ " End " + new Date());


    }
    public  VariantIndex getIndexObject(Variant v, List<edu.mcw.rgd.datamodel.variants.VariantTranscript> vts) throws IOException {
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
    public List<edu.mcw.rgd.datamodel.variants.VariantTranscript> getVariantTranscripts(long startPos, String chromosome, String refNuc, String varNuc) throws IOException {
        SearchSourceBuilder srb=new SearchSourceBuilder();

        if(refNuc!=null && varNuc!=null) {
            srb.query(QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("chromosome", chromosome))
                    .filter(QueryBuilders.termQuery("startPos", startPos))
                    .filter(QueryBuilders.termQuery("refNuc", refNuc))
                    .filter(QueryBuilders.termQuery("varNuc", varNuc))
            );
        }else{
            srb.query(QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("chromosome", chromosome))
                    .filter(QueryBuilders.termQuery("startPos", startPos)));
        }



      SearchRequest request=new SearchRequest("transcripts_human_dev1"); // chr 1 transcripts
        request.source(srb);


        SearchResponse sr=ESClient.getClient().search(request, RequestOptions.DEFAULT);
        List<edu.mcw.rgd.datamodel.variants.VariantTranscript> tds= new ArrayList<>();
        for(SearchHit h:sr.getHits().getHits()){
            Map source=h.getSourceAsMap();
           edu.mcw.rgd.datamodel.variants.VariantTranscript td=new edu.mcw.rgd.datamodel.variants.VariantTranscript();
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
    public List<VariantTranscript> getVariantTranscriptsByChromosome(String chromosome) throws IOException {
        SearchSourceBuilder srb=new SearchSourceBuilder();
        BoolQueryBuilder queryBuilder=new BoolQueryBuilder();
        queryBuilder.must(QueryBuilders.termQuery("chromosome", chromosome));
        queryBuilder.must(QueryBuilders.termQuery("locationName.keyword", "EXON"));
        srb.query(queryBuilder);
        SearchRequest request=new SearchRequest("transcripts_human_dev1");
        request.source(srb);

        //   RestHighLevelClient client=ESClient.getInstance();
        SearchResponse sr=ESClient.getClient().search(request, RequestOptions.DEFAULT);
        List<VariantTranscript> tds= new ArrayList<>();
        for(SearchHit h:sr.getHits().getHits()){
            Map source=h.getSourceAsMap();
            VariantTranscript td=new VariantTranscript();

            td.setTranscriptRgdId((Integer) source.get("transcriptRgdId"));
            td.setRefAA((String) source.get("refAA"));
            td.setVarAA((String) source.get("varAA"));
            td.setFullRefAA((String) source.get("fullRefAA"));
           td.setFullRefAAPos((Integer) source.get("fullRefAAPos"));
            td.setStartPos((Integer) source.get("startPos"));
            td.setRefNuc((String) source.get("refNuc"));
            tds.add(td);

        }
        ESClient.destroy();
        // System.out.println("TRANSCIPTS SIZE: "+tds.size());
        return tds;
    }
    public List<BigDecimal> getConservationScores(String chr, int position) throws Exception {
        String sql="select * from b37_conscore_part_iot where chr=? and position=?";
        Connection conn=null;
        PreparedStatement stmt=null;
        ResultSet rs=null;
        conn= DataSourceFactory.getInstance().getCarpeNovoDataSource().getConnection();
        stmt=conn.prepareStatement(sql);
        stmt.setString(1, chr);
        stmt.setInt(2, position);
        rs=stmt.executeQuery();
        List<BigDecimal> scores=new ArrayList<>();
        while(rs.next()){
       //     System.out.println(rs.getString("POSITION")+"\t"+ rs.getDouble("score"));
            scores.add(rs.getBigDecimal("score"));
        }
        rs.close();
        stmt.close();
        conn.close();
        return scores;
    }
    public String determineVariantType(String refSeq, String varSeq) {

        // handle insertions
        if( refSeq==null || refSeq.length()==0 )
            return "ins";

        // handle deletions
        if( varSeq==null || varSeq.length()==0 )
            return "del";

        // handle snv
        return "snv";
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

public static void main(String[] args) throws IOException {
    VariantLoad3 loader=new VariantLoad3();
    DefaultListableBeanFactory bf= new DefaultListableBeanFactory();
    new XmlBeanDefinitionReader(bf) .loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));

    ESClient es= (ESClient) bf.getBean("client");

    loader.getVariantTranscripts(9907220,"","", "");
    if(es!=null){
        es.destroy();
    }
    System.out.println("Done!!");
}

}
