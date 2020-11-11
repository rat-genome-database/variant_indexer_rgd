package edu.mcw.rgd.variantIndexerRgd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.datamodel.Chromosome;
import edu.mcw.rgd.datamodel.Sample;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.process.Utils;

import edu.mcw.rgd.variantIndexerRgd.dao.VariantLoad3;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.process.GeneCache;
import edu.mcw.rgd.variantIndexerRgd.process.Zygosity;
import edu.mcw.rgd.variantIndexerRgd.service.ESClient;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static edu.mcw.rgd.variantIndexerRgd.dao.VariantLoad3.geneLociMap;

/**
 * Created by jthota on 1/17/2020.
 */
public class VariantHumanIndexer implements Runnable {

    List<String> lines; int strainCount; String[] header;GeneCache geneCache; String processName;int lineCount;
    boolean processLinesWithMissingADDP=true;
    boolean processVariantsSameAsRef=false;

    Map<Integer, List<BigDecimal>> conScoresMap=new HashMap<>();
    VariantLoad3 loader=new VariantLoad3();
    Zygosity zygosity=new Zygosity();

    public VariantHumanIndexer(List<String> lines, int strainCount, String[] header, GeneCache geneCache,String processName, int lineCOunt){
        this.lines=lines; this.strainCount=strainCount; this.header=header; this.geneCache=geneCache;this.processName=processName;
        this.lineCount=lineCOunt;
    }
    @Override
    public void run() {
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



        for(String line: lines) {
            String[] v = line.split("[\\t]", -1);

            if (v.length == 0 || v[0].length() == 0 || v[0].charAt(0) == '#')
                //skip lines with "#"
                return;
            String chr = null;
            try {
                chr = getChromosome(v[0]);
            } catch (Exception e) {
                e.printStackTrace();

            }
            if (chr == null || chr.length() > 2) {
                return;
            }
            int pos = Integer.parseInt(v[1]);

            List<String> strains = new ArrayList<>();

            if (processName.equals("variants")) {
                for (int i = 9; i < 9 + strainCount && i < v.length; i++) {
                    String strain = header[i];
                    String genotype = new String();
                    try {
                        if (v[i].length() > 3)
                            genotype = v[i].substring(0, 3); // GT for diploid
                        else
                            genotype = v[i]; // GT for haploid
                        if (!genotype.equals("./.") && !genotype.equals("0/0") && !genotype.trim().equals("") && !genotype.trim().equals(".") && !genotype.contains("/")) {
                            //     System.out.println(strain);
                            if (VariantIndexerThread.sampleIdMap.get(strain) != null) {
                                strains.add(strain);
                            }
                        }

                    } catch (Exception e) {
                        System.out.println("POSITION: " + pos + "\tGENOTYPE:" + genotype);
                        e.printStackTrace();
                    }


                }
            }
            try {
                int[] readCount = null;
                // format is in 0/1:470,63:533:99:507,0,3909
                int readDepth = 0;
                if (processLinesWithMissingADDP) {
                    readDepth = 9;
                    readCount = new int[]{9, 9, 9, 9, 9, 9, 9, 9};
                }
                int totalDepth = 0;
                if (processLinesWithMissingADDP) {
                    totalDepth = 9;
                }
                // for the single Reference to multiple Variants
                int alleleCount = getAlleleCount(v[4]);
                String[] alleles = (v[3] + "," + v[4]).split(",");
                // for every allele, including refNuc
                for (String allele : alleles) {
                    // skip the line variant if it is the same with reference (unless an override is specified)
                    if (!processVariantsSameAsRef && v[3].equals(allele)) {
                        continue;
                    }

                    boolean isSnv = !Utils.isStringEmpty(v[3]) && !Utils.isStringEmpty(v[4]);
                    long endPos = 0;
                    if (isSnv) {
                        endPos = pos + 1;
                    } else {
                        // insertions
                        if (Utils.isStringEmpty(v[3])) {
                            endPos = pos;
                        }
                        // deletions
                        else if (Utils.isStringEmpty(v[4])) {
                            endPos = pos + v[3].length();
                        } else {
                            System.out.println("Unexpected var type");
                        }
                    }
                    if (!loader.alleleIsValid(v[3])) {
                        //   System.out.println(" *** Ref Nucleotides must be A,C,G,T,N");
                        continue;
                    }
                    if (!loader.alleleIsValid(v[4])) {
                        //     System.out.println(" *** Var Nucleotides must be A,C,G,T,N");
                        continue;
                    }


                    List<VariantTranscript> variantTranscripts = new ArrayList<>();
                    List<BigDecimal> conScores = new ArrayList<>();
                    try {
                        variantTranscripts.addAll(loader.getVariantTranscripts(pos, chr,v[3],allele ));
                        if (conScoresMap.get(pos) != null) {
                            conScores.addAll(conScoresMap.get(pos));
                        } else {
                            conScores.addAll(loader.getConservationScores(chr, pos));
                            conScoresMap.put(pos, conScores);
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    // NOTE: for snvs, only ACGT counts are provided
                    //    for indels, only allele count is provided
                    int alleleDepth = getReadCountForAllele(allele, alleles, readCount); // from AD field: how many times allele was called
                    int readCountA = getReadCountForAllele("A", alleles, readCount);
                    int readCountC = getReadCountForAllele("C", alleles, readCount);
                    int readCountG = getReadCountForAllele("G", alleles, readCount);
                    int readCountT = getReadCountForAllele("T", alleles, readCount);

                    String totalDepthStr = String.valueOf(totalDepth);
                    if (totalDepthStr == null || totalDepthStr.isEmpty()) {
                        if (isSnv)
                            totalDepth = readCountA + readCountC + readCountG + readCountT;
                        else
                            totalDepth = readDepth;
                    } else
                        totalDepth = Integer.parseInt(totalDepthStr);

                    // total reads called (AD field) vs total reads analyzed (DP field): 100*readDepth/totalDepth
                    int qualityScore = 0;
                    if (totalDepth > 0) {
                        qualityScore = (100 * readDepth + totalDepth / 2) / totalDepth;
                    }

                    String variantType = loader.determineVariantType(v[3], v[4]);

                    List<Integer> geneRgdIds = geneCache.getGeneRgdIds(pos);
                    String genicStatus = !geneRgdIds.isEmpty() ? "GENIC" : "INTERGENIC";
                    for (String s : strains) {
                        Sample sample = VariantIndexerThread.sampleIdMap.get(s);

                        VariantIndex vi = new VariantIndex();
                        vi.setChromosome(chr);
                        vi.setRefNuc(v[3]);
                        vi.setStartPos(pos);
                        vi.setTotalDepth(totalDepth);
                        vi.setVarFreq(alleleDepth);
                        vi.setVarNuc(v[4]);
                        vi.setSampleId(sample.getId());
                        vi.setAnalysisName(sample.getAnalysisName());
                        vi.setPatientId(sample.getPatientId());
                        vi.setGender(sample.getGender());
                        vi.setQualityScore(qualityScore);
                        //   vi.setHGVSNAME(line.getHgvsName());
                        vi.setStrainRgdId(sample.getStrainRgdId());
                        vi.setVariantType(variantType);
                        vi.setConScores(conScores);
                        vi.setGenicStatus(genicStatus);
                        if (!isSnv) {
                            System.out.println(pos+"\t"+ v[3]+"\t"+v[4]);
                          //     vi.setPaddingBase(line.getPaddingBase());
                        }
                        vi.setEndPos(endPos);

                        int score = 0;
                        if (isSnv) {
                //            score = zygosity.computeVariant(readCountA, readCountC, readCountG, readCountT, sample.getGender(), vi);
                        } else {
                            //         // parameter tweaking for indels
                 //           zygosity.computeZygosityStatus(alleleDepth, readDepth, sample.getGender(), vi);
//
                            // compute zygosity ref allele, if possible
                            if (v[3].equals("A")) {
                                vi.setZygosityRefAllele(readCountA > 0 ? "Y" : "N");
                            } else if (v[3].equals("C")) {
                                vi.setZygosityRefAllele(readCountC > 0 ? "Y" : "N");
                            } else if (v[3].equals("G")) {
                                vi.setZygosityRefAllele(readCountG > 0 ? "Y" : "N");
                            } else if (v[3].equals("T")) {
                                vi.setZygosityRefAllele(readCountT > 0 ? "Y" : "N");
                            }

                            if (alleleDepth == 0)
                                score = 0;
                            else
                                score = (int) vi.getZygosityPercentRead();
                        }
                        if (score == 0) {
                            continue;
                        }


                        vi.setRsId(v[2]);
                        List<String> regionNames = geneLociMap.get(vi.getStartPos());
                        if (regionNames != null && regionNames.size() != 0) {
                            vi.setRegionName(regionNames);
                            List<String> regionNameLC = new ArrayList<>();
                            for (String name : regionNames) {
                                regionNameLC.add(name.toLowerCase());
                            }
                            vi.setRegionNameLc(regionNameLC);
                        }
                        if (variantTranscripts != null && variantTranscripts.size() != 0)
                            vi.setVariantTranscripts(variantTranscripts);
                        vi.setMapKey(17);

                        try {
                            ObjectMapper mapper=new ObjectMapper();
                            String json =  mapper.writeValueAsString(vi);
                            bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    }
                }


            } catch (Exception e) {
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
        System.out.println("***********"+Thread.currentThread().getName()+ "\tLINE_COUNT:"+lineCount + "\tEND ...."+"\t"+ new Date()+"*********");

    }
    int getReadCountForAllele(String allele, String[] alleles, int[] readCount) {

        for( int i=0; i<alleles.length; i++ ) {
            if( alleles[i].equals(allele) )

                return readCount[0];

        }
        return 0;
    }
    String getChromosome(String chr) throws Exception {

        String c = getChromosomeImpl(chr);
        if( c!=null && c.equals("M") ) {
            c = "MT";
        }
        return c;
    }
    String getChromosomeImpl(String chr) throws Exception {
        // chromosomes could be provided as 'NC_005100.4'
        if( chr.startsWith("NC_") ) {
            return getChromosomeFromDb(chr);
        }

        chr = chr.replace("chr", "").replace("c", "");
        // skip lines with invalid chromosomes (chromosome length must be 1 or 2
        if( chr.length()>2 || chr.contains("r") || chr.equals("Un") ) {
            return null;
        }
        return chr;
    }
    String getChromosomeFromDb(String refseqNuc) throws Exception {
        String chr = _chromosomeMap.get(refseqNuc);
        if( chr==null ) {
            MapDAO dao = new MapDAO();
            Chromosome c = dao.getChromosome(refseqNuc);
            if( c!=null ) {
                chr = c.getChromosome();
                _chromosomeMap.put(refseqNuc, chr);
            }
        }
        return chr==null ? null : chr;
    }
    Map<String,String> _chromosomeMap = new HashMap<>();
    int getAlleleCount(String s) {
        int alleleCount = 1;
        for( int i=0; i<s.length(); i++ ) {
            if( s.charAt(i)==',' )
                alleleCount++;
        }
        return alleleCount;
    }
}
