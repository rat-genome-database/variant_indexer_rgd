package edu.mcw.rgd.variantIndexerRgd.futureHuman;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.SampleDAO;
import edu.mcw.rgd.datamodel.Sample;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.variantIndexerRgd.Manager;
import edu.mcw.rgd.variantIndexerRgd.VTranscriptProcessThread;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantLoad3;
import edu.mcw.rgd.variantIndexerRgd.model.CommonFormat2Line;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndexObject;
import edu.mcw.rgd.variantIndexerRgd.newtablestructure.VariantIndexUtils;
import edu.mcw.rgd.variantIndexerRgd.process.GeneCache;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;
import edu.mcw.rgd.variantIndexerRgd.process.Zygosity;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static org.apache.logging.log4j.LogManager.getLogger;

public class HumanVCFProcess {
    private String process;// transcripts or variants
    private static RgdIndex rgdIndex;
    private int mapKey;
    private String chr;
    private String fromChr;
    private String toChr;
    private String fileName;
    public static Map<Long, List<String>> geneLociMap;
    static Logger log=getLogger(Manager.class);

    Map<Integer, List<BigDecimal>> conScoresMap=new HashMap<>();
    VariantLoad3 loader=new VariantLoad3();
    Zygosity zygosity=new Zygosity();
        public List<VariantIndexObject> getIndexObjects(List<CommonFormat2Line> list, GeneCache geneCache){
        List<VariantIndexObject> indexObjects= new ArrayList<>();

        for (CommonFormat2Line line : list) {
            VariantIndexObject indexObject=new VariantIndexObject();
            List<VariantSampleDetail> sampleDetails=new ArrayList<>();

            VariantMapData md=new VariantMapData();
            boolean isSnv = !Utils.isStringEmpty(line.getRefNuc()) && !Utils.isStringEmpty(line.getVarNuc());
            long endPos = 0;
            if (isSnv) {
                endPos = line.getPos() + 1;
            } else {
                // insertions
                if (Utils.isStringEmpty(line.getRefNuc())) {
                    endPos = line.getPos();
                }
                // deletions
                else if (Utils.isStringEmpty(line.getVarNuc())) {
                    endPos = line.getPos() + line.getRefNuc().length();
                } else {
                    System.out.println("Unexpected var type");
                }
            }
            if (line.getRefNuc()!=null && !loader.alleleIsValid(line.getRefNuc())) {
                continue;
            }
            if (line.getVarNuc()!=null && !loader.alleleIsValid(line.getVarNuc())) {
                continue;
            }


            List<VariantTranscript> variantTranscripts = new ArrayList<>();
            List<BigDecimal> conScores= new ArrayList<>();
            try {
                variantTranscripts=loader.getVariantTranscripts(line.getPos(), line.getChr(), line.getRefNuc(), line.getVarNuc());
                if(conScoresMap.get(line.getPos())!=null){
                    conScores.addAll(conScoresMap.get(line.getPos()));
                }else {
                    conScores.addAll(loader.getConservationScores(line.getChr(), line.getPos()));
                    conScoresMap.put(line.getPos(), conScores);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            // NOTE: for snvs, only ACGT counts are provided
            //    for indels, only allele count is provided
            int alleleDepth = line.getAlleleDepth(); // from AD field: how many times allele was called
            int readDepth = line.getReadDepth(); // from AD field: how many times all alleles were called
            int readCountA = line.getCountA();
            int readCountC = line.getCountC();
            int readCountG = line.getCountG();
            int readCountT = line.getCountT();

            int totalDepth = 0;
            Integer totalDepthObj = line.getTotalDepth();
            if (totalDepthObj == null || totalDepthObj == 0) {
                if (isSnv)
                    totalDepth = readCountA + readCountC + readCountG + readCountT;
                else
                    totalDepth = readDepth;
            } else {
                totalDepth = totalDepthObj;
            }

            // total reads called (AD field) vs total reads analyzed (DP field): 100*readDepth/totalDepth
            int qualityScore = 0;
            if (totalDepth > 0) {
                qualityScore = (100 * readDepth + totalDepth / 2) / totalDepth;
            }

            String variantType= loader.determineVariantType(line.getRefNuc(), line.getVarNuc());

            List<Integer> geneRgdIds = geneCache.getGeneRgdIds(line.getPos());
            String genicStatus = !geneRgdIds.isEmpty() ? "GENIC" : "INTERGENIC";
           md.setChromosome(line.getChr());
            md.setReferenceNucleotide(line.getRefNuc());
            md.setStartPos(line.getPos());
            md.setVariantNucleotide(line.getVarNuc());
            md.setGenicStatus(genicStatus);
            md.setVariantType(variantType);
            if (!isSnv) {
                md.setPaddingBase(line.getPaddingBase());
            }
            md.setEndPos(endPos);
            md.setRsId(line.getRsId());
            List<String> regionNames=geneLociMap.get(md.getStartPos());
            if(regionNames!=null && !regionNames.isEmpty()) {
                indexObject.setRegionName(regionNames);
                List<String> regionNameLC = new ArrayList<>();
                for (String name : regionNames) {
                    regionNameLC.add(name.toLowerCase());
                }
                indexObject.setRegionNameLc(regionNameLC);
            }
            if(!variantTranscripts.isEmpty())
                indexObject.setVariantTranscripts(variantTranscripts);
            md.setMapKey(mapKey);

            for (String s : line.getStrainList()) {
                Sample sample = VariantIndexerThread.sampleIdMap.get(s);

                VariantSampleDetail v = new VariantSampleDetail();

                v.setDepth(line.getTotalDepth());
                v.setVariantFrequency(line.getAlleleDepth());
                v.setSampleId(sample.getId());
                v.setQualityScore(qualityScore);

                int score = 0;
                if (isSnv) {
                    score = zygosity.computeVariant(readCountA, readCountC, readCountG, readCountT, sample.getGender(), md, v);
                } else {
                    zygosity.computeZygosityStatus(alleleDepth, readDepth, sample.getGender(), md,v);

                    // compute zygosity ref allele, if possible
                    if (line.getRefNuc()!=null && line.getRefNuc().equals("A")) {
                        v.setZygosityRefAllele(readCountA > 0 ? "Y" : "N");
                    } else if (line.getRefNuc()!=null && line.getRefNuc().equals("C")) {
                        v.setZygosityRefAllele(readCountC > 0 ? "Y" : "N");
                    } else if (line.getRefNuc()!=null && line.getRefNuc().equals("G")) {
                        v.setZygosityRefAllele(readCountG > 0 ? "Y" : "N");
                    } else if (line.getRefNuc()!=null && line.getRefNuc().equals("T")) {
                        v.setZygosityRefAllele(readCountT > 0 ? "Y" : "N");
                    }

                    if (alleleDepth != 0)
                        score = (int) v.getZygosityPercentRead();
                }
                if (score == 0) {
                    continue;
                }
                sampleDetails.add(v);
            }
            indexObject.setVariant(md);
            indexObject.setSamples(sampleDetails);
            indexObjects.add(indexObject);
        }
        return indexObjects;

    }
    public static Map<String, Sample> getSampleIdMap(int mapKey,  int rowlimit) throws Exception {
        SampleDAO sdao = new SampleDAO();
        sdao.setDataSource(DataSourceFactory.getInstance().getCarpeNovoDataSource());
        List<String> populations=new ArrayList<>(Arrays.asList("ACB", "ASW", "BEB", "CDX", "CEU", "CHB", "CHS", "CLM", "ESN",
                "FIN", "GBR", "GIH", "GWD", "IBS", "ITU", "JPT", "KHV", "LWK", "MSL", "MXL", "PEL", "PJL", "pop", "PUR",
                "STU", "TSI", "YRI"
        ));
        List<Sample> samples = new ArrayList<>();
        for(String population:populations) {
            if (population != null) {
                if (rowlimit > 0) {
                    samples.addAll(sdao.getLimitedSamplesByPopulation(mapKey, population.toUpperCase(), rowlimit));
                } else
                    samples = sdao.getSamplesByMapKey(mapKey, population.toUpperCase());
            }
        }
        java.util.Map<String, Sample> sampleIdMap = new HashMap<>();
        System.out.println("SAMPLES SIZE:" +samples.size());
        for (Sample s : samples) {
            String analysisName = s.getAnalysisName();
            String substr;
            if (analysisName.contains(":")) {
                substr = analysisName.substring(analysisName.indexOf("(") + 1, analysisName.indexOf(":"));
            } else {
                if (analysisName.contains(")"))
                    substr = analysisName.substring(analysisName.indexOf("(") + 1, analysisName.indexOf(")"));
                else
                    substr = analysisName.substring(analysisName.indexOf("(") + 1);

            }
            sampleIdMap.put(substr, s);
        }
        return sampleIdMap;
    }
    void  processHumanVCF() throws Exception {
        geneLociMap = VariantIndexUtils.getGeneLociMap(mapKey, chr);
        File file = new File(fileName);
        GeneCache geneCache = new GeneCache();
        geneCache.loadCache(mapKey, chr, DataSourceFactory.getInstance().getDataSource());

        if(process.equalsIgnoreCase( "transcripts")) {
            System.out.println("Processing "+ process+"..."  );
            BufferedReader reader;
            if (file.getName().endsWith(".txt.gz") || file.getName().endsWith(".vcf.gz")) {
                reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
            } else {
                reader = new BufferedReader(new FileReader(file));
            }
            String line;
            int lineCount = 0;
            String[] header = null;
            int strainCount = 0;
            List<CommonFormat2Line> lines = new ArrayList<>();
            int clusterCount = 0;
            MyThreadPoolExecutor executor= new MyThreadPoolExecutor(10,10,0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            while ((line = reader.readLine()) != null) {
                // skip comment line
                if (line.startsWith("#")) {
                    header = line.substring(1).split("[\\t]", -1);
                    strainCount = header.length - 9;

                } else {
                    VariantIndexerThread indexer = new VariantIndexerThread();
                    List<CommonFormat2Line> list = indexer.run(line, strainCount, header, geneCache, process);
                    lines.addAll(list);
                    lineCount++;
                    if (lines.size() == 10000) {
                        Runnable workerThread = new VTranscriptProcessThread(lines, rgdIndex.getNewAlias(), geneCache, clusterCount);
                        try {
                            executor.execute(workerThread);
                        }catch (Exception e){
                            log.error("REJECTED AT LINE COUNT:"+ lineCount);
                            log.error(e.getMessage());
                        }
                        lines = new ArrayList<>();
                        clusterCount = clusterCount + 1;

                    }
                }
            }
            if (!lines.isEmpty()) {
                Runnable workerThread = new VTranscriptProcessThread(lines, rgdIndex.getNewAlias(), geneCache, clusterCount);
                executor.execute(workerThread);
            }
            System.out.println("TOTAL LINE COUNT OF VCF: " + lineCount);
            reader.close();
            VariantIndexUtils.awaitTermination(executor);
        }else if(process.equalsIgnoreCase("variants")) {
            VariantIndexerThread.sampleIdMap = getSampleIdMap(mapKey, 5);
            String line;
            int lineCount = 0;
            String[] header = null;

            int strainCount = 0;
            List<CommonFormat2Line> lines = new ArrayList<>();
            int clusterCount = 0;
            VariantIndexerThread indexer=new VariantIndexerThread();
            MyThreadPoolExecutor executor= new MyThreadPoolExecutor(10,10,0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            List<VariantIndexObject> indexObjects= new ArrayList<>();
            BufferedReader reader;
            if (file.getName().endsWith(".txt.gz") || file.getName().endsWith(".vcf.gz")) {
                reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
            } else {
                reader = new BufferedReader(new FileReader(file));
            }

            while ((line = reader.readLine()) != null) {
                // skip comment line
                if (line.startsWith("#")) {
                    header = line.substring(1).split("[\\t]", -1);
                    strainCount = header.length - 9;
                } else {
                    List<CommonFormat2Line> list = indexer.run(line, strainCount, header, geneCache, process);
                    indexObjects.addAll(getIndexObjects(list, geneCache));
                    lineCount++;

                    if (indexObjects.size() >= 1000) {
                        Runnable workerThread = new VariantProcessThread(indexObjects, strainCount, header, geneCache, process, clusterCount);
                        try {
                            executor.execute(workerThread);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.err.println("REJECTED. Restarting thread...");
                            log.info("REJECTED. Restarting thread..."+"\n"+e.getMessage());
                            executor.execute(workerThread);
                        }

                        clusterCount = clusterCount + 1;
                        indexObjects = new ArrayList<>();
                    }
                }
            }
            if (!indexObjects.isEmpty()) {
                Runnable workerThread = new VariantProcessThread(indexObjects, strainCount, header, geneCache,process, clusterCount );
                executor.execute(workerThread);
            }
            System.out.println("TOTAL LINE COUNT OF VCF: " + lineCount);
            VariantIndexUtils.awaitTermination(executor);
            reader.close();
        }
        System.out.println("Finished all threads: " + new Date());
    }


    }
