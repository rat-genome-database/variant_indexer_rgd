package edu.mcw.rgd.variantIndexerRgd;


import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.SampleDAO;
import edu.mcw.rgd.datamodel.*;

import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.services.ClientInit;
import edu.mcw.rgd.services.IndexAdmin;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantLoad3;

import edu.mcw.rgd.variantIndexerRgd.model.*;
import edu.mcw.rgd.variantIndexerRgd.newtablestructure.*;
import edu.mcw.rgd.variantIndexerRgd.process.GeneCache;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;
import edu.mcw.rgd.variantIndexerRgd.process.Zygosity;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;

import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;


import java.io.*;

import java.math.BigDecimal;
import java.util.*;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static edu.mcw.rgd.variantIndexerRgd.dao.VariantLoad3.geneLociMap;
import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Created by jthota on 11/14/2019.
 */

public class Manager {
    private String version;
    private static RgdIndex rgdIndex;
    private static List environments;
    private IndexAdmin admin;
    private int mapKey;
    private int speciesTypeKey;
    private String chr;
    private String fromChr;
    private String toChr;
    private String fileName;
    private String command;     //update or reindex
    private String process;     // transcripts or variants
    private String env;     // dev or test or prod
    List<String> chromosomes;

    Map<Integer, List<BigDecimal>> conScoresMap=new HashMap<>();
    VariantLoad3 loader=new VariantLoad3();
    Zygosity zygosity=new Zygosity();
    BulkIndexProcessor bulkIndexProcessor;

    static Logger log=getLogger(Manager.class);

    public static void main(String[] args) throws Exception {

       DefaultListableBeanFactory bf= new DefaultListableBeanFactory();
       new XmlBeanDefinitionReader(bf) .loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
       Manager manager= (Manager) bf.getBean("manager");

       log.info(manager.version);
      rgdIndex= (RgdIndex) bf.getBean("rgdIndex");
       RgdIndex.setInstance(rgdIndex);
       manager.bulkIndexProcessor=BulkIndexProcessor.getInstance();
      try{

            List<String> indices= new ArrayList<>();
            manager.command=args[0];
            manager.env=args[1];
            manager.process=args[2];
            manager.speciesTypeKey=Integer.parseInt(args[3]);
            manager.mapKey=Integer.parseInt(args[4]);

          List<String> chromosomes=new ArrayList<>();
          try {
              manager.fromChr = args[5];
              manager.toChr = args[6];
          }catch (Exception e){
              e.printStackTrace();
          }
          if(manager.fromChr!=null &&  manager.toChr==null) {
                chromosomes.add(manager.fromChr);
          }
          if(manager.fromChr!=null && manager.toChr!=null) {
              System.out.println("FROM CHR:" + manager.fromChr + "\tTO CHR: " + manager.toChr);
              for (int i = Integer.parseInt(manager.fromChr); i <= Integer.parseInt(manager.toChr); i++) {
                  chromosomes.add(Integer.toString(i));
              }
          }
          manager.chromosomes=chromosomes;

          String species = SpeciesType.getCommonName(manager.speciesTypeKey).toLowerCase().replace(" ", "");
            String index=manager.process+"_"+species+manager.mapKey;

            if (environments.contains(manager.env)) {
                rgdIndex.setIndex(index +"_"+manager.env);
                indices.add(index+"_"+manager.env + "1");
                indices.add(index + "_"+manager.env + "2");
               rgdIndex.setIndices(indices);
            }

            manager.run(args);


        }catch (Exception e){
        manager.bulkIndexProcessor.destroy();

          ClientInit.destroy();
            e.printStackTrace();
        }

        manager.bulkIndexProcessor.destroy();

        ClientInit.destroy();

    }

    public void run(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        String species= SpeciesType.getCommonName(speciesTypeKey);
        if(command.equalsIgnoreCase("reindex"))
          admin.createIndex("", species);
        else  if(command.equalsIgnoreCase("update"))
            admin.updateIndex();

        switch (speciesTypeKey) {
            case 1:
            case 2:
            case 3:
            case 6:
            case 9:
            case 13:
                System.out.println("Processing "+species+" variants...");
                this.setMapKey(mapKey);

                System.out.println("CHROMOSOMES SIZE: "+ chromosomes.size());

                       MyThreadPoolExecutor executor2 = new MyThreadPoolExecutor(3, 3, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
                       for (String chr : chromosomes) {
                          Runnable chromosomeThread=new ChromosomeThread(chr, mapKey,speciesTypeKey);
                          executor2.execute(chromosomeThread);
                       }
                       VariantIndexUtils.awaitTermination(executor2);

                break;
            default:
                break;

            }


     String clusterStatus = this.getClusterHealth(rgdIndex.getNewAlias());
        if (!clusterStatus.equalsIgnoreCase("ok")) {
            System.out.println(clusterStatus + ", refusing to continue with operations");
           log.info(clusterStatus + ", refusing to continue with operations");
        } else {
            if(command.equalsIgnoreCase("reindex")) {
                System.out.println("CLUSTER STATUS:"+ clusterStatus+". Switching Alias...");
                log.info("CLUSTER STATUS:"+ clusterStatus+". Switching Alias...");
                switchAlias();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(" - " + Utils.formatElapsedTime(start, end));
        log.info(" - " + Utils.formatElapsedTime(start, end));
        System.out.println("CLIENT IS CLOSED");
    }

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

    public String getClusterHealth(String index) throws Exception {

        ClusterHealthRequest request = new ClusterHealthRequest(index);
        ClusterHealthResponse response = ClientInit.getClient().cluster().health(request, RequestOptions.DEFAULT);
        System.out.println(response.getStatus().name());
        if (response.isTimedOut()) {
            return   "cluster state is " + response.getStatus().name();
        }

        return "OK";
    }
    public void switchAlias() throws Exception {
        String newAlias = rgdIndex.getNewAlias();
        String oldAlias = rgdIndex.getOldAlias();
        String indexName = rgdIndex.getIndex();
        System.out.println("NEW ALIAS: " + newAlias + " || OLD ALIAS:" + oldAlias);
        IndicesAliasesRequest request = new IndicesAliasesRequest();

        if (oldAlias != null) {
            IndicesAliasesRequest.AliasActions removeAliasAction =
                    new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                            .index(oldAlias)
                            .alias(indexName);
            IndicesAliasesRequest.AliasActions addAliasAction =
                    new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(newAlias)
                            .alias(indexName);
            request.addAliasAction(removeAliasAction);
            request.addAliasAction(addAliasAction);
        }else{
            IndicesAliasesRequest.AliasActions addAliasAction =
                    new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(newAlias)
                            .alias(indexName);
            request.addAliasAction(addAliasAction);
        }
        AcknowledgedResponse indicesAliasesResponse =
                ClientInit.getClient().indices().updateAliases(request, RequestOptions.DEFAULT);


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

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setEnvironments(List environments) {
        this.environments = environments;
    }

    public List getEnvironments() {
        return environments;
    }

    public void setAdmin(IndexAdmin admin) {
        this.admin = admin;
    }

    public IndexAdmin getAdmin() {
        return admin;
    }

    public int getMapKey() {
        return mapKey;
    }

    public void setMapKey(int mapKey) {
        this.mapKey = mapKey;
    }

    public int getSpeciesTypeKey() {
        return speciesTypeKey;
    }

    public void setSpeciesTypeKey(int speciesTypeKey) {
        this.speciesTypeKey = speciesTypeKey;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public String getProcess() {
        return process;
    }

    public void setProcess(String process) {
        this.process = process;
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    public static Logger getLog() {
        return log;
    }

    public static void setLog(Logger log) {
        Manager.log = log;
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

    public void setRgdIndex(RgdIndex rgdIndex) {
    }
}
