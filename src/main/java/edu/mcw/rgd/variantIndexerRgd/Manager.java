package edu.mcw.rgd.variantIndexerRgd;



import edu.mcw.rgd.dao.impl.GeneLociDAO;
import edu.mcw.rgd.datamodel.*;

import edu.mcw.rgd.datamodel.RgdIndex;

import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.services.BulkIndexProcessor;
import edu.mcw.rgd.services.ClientInit;
import edu.mcw.rgd.services.IndexAdmin;


import edu.mcw.rgd.variantIndexerRgd.vvIndexer.*;

import edu.mcw.rgd.variantIndexerRgd.utils.MyThreadPoolExecutor;


import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;

import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;



import java.util.*;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import java.util.concurrent.TimeUnit;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Created by jthota on 11/14/2019.
 */

public class Manager {
    private String version;
    private RgdIndex rgdIndex;
    private static List environments;
    private IndexAdmin admin;
    private int mapKey;
    private int speciesTypeKey;
    private String fromChr;
    private String toChr;
    private String command;     //update or reindex
    private String process;     // transcripts or variants
    private String env;     // dev or test or prod
    List<String> chromosomes;//loading to es or db

    private edu.mcw.rgd.services.BulkIndexProcessor bulkIndexProcessor;

    static Logger log=getLogger(Manager.class);
    GeneLociDAO geneLociDAO=new GeneLociDAO();
    public static void main(String[] args) throws Exception {

       DefaultListableBeanFactory bf= new DefaultListableBeanFactory();
       new XmlBeanDefinitionReader(bf) .loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
       Manager manager= (Manager) bf.getBean("manager");

       log.info(manager.version);
        manager.rgdIndex= (RgdIndex) bf.getBean("rgdIndex");
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
          String species= new String();
          if( SpeciesType.getCommonName(manager.speciesTypeKey).contains(" "))
              species=    SpeciesType.getCommonName(manager.speciesTypeKey).toLowerCase().replace(" ","");
          else
              species=    SpeciesType.getCommonName(manager.speciesTypeKey).toLowerCase();
            String index=manager.process+"_"+species+manager.mapKey;

            if (environments.contains(manager.env)) {
                manager.rgdIndex.setIndex(index +"_"+manager.env);
                indices.add(index+"_"+manager.env + "1");
                indices.add(index + "_"+manager.env + "2");
                manager.rgdIndex.setIndices(indices);
            }

            manager.run();


        }catch (Exception e){
          ClientInit.destroy();
            e.printStackTrace();
        }
      ClientInit.destroy();

    }

    public void run() throws Exception {
        long start = System.currentTimeMillis();
        String species= SpeciesType.getCommonName(speciesTypeKey);
        if(command.equalsIgnoreCase("reindex"))
          admin.createIndex("", species);
        else  if(command.equalsIgnoreCase("update"))
            admin.updateIndex();
        System.out.println("Processing "+species+" variants...");
        this.setMapKey(mapKey);
        System.out.println("CHROMOSOMES SIZE: "+ chromosomes.size());
        ExecutorService executor2 = new MyThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Runnable chromosomeThread= null;
        for (String chr : chromosomes) {
            List<GeneLoci> geneLoci=geneLociDAO.getGeneLociByMapKeyAndChr(mapKey,chr);
            chromosomeThread=new ChromosomeThread(chr, mapKey,speciesTypeKey, geneLoci);
            executor2.execute(chromosomeThread);
        }
        executor2.shutdown();
        while (!executor2.isTerminated()) {}

        String clusterStatus = this.getClusterHealth(RgdIndex.getNewAlias());
        if (!clusterStatus.equalsIgnoreCase("ok")) {
            System.out.println(clusterStatus + ", refusing to continue with operations");
           log.info(clusterStatus + ", refusing to continue with operations");
        } else {
            if(command.equalsIgnoreCase("reindex")) {
                System.out.println("CLUSTER STATUR:"+ clusterStatus+". Switching Alias...");
                log.info("CLUSTER STATUR:"+ clusterStatus+". Switching Alias...");
                switchAlias();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(" - " + Utils.formatElapsedTime(start, end));
        log.info(" - " + Utils.formatElapsedTime(start, end));
        System.out.println("CLIENT IS CLOSED");
    }

    public String getClusterHealth(String index) throws Exception {

        ClusterHealthRequest request = new ClusterHealthRequest(index);
        ClusterHealthResponse response = ClientInit.getClient().cluster().health(request, RequestOptions.DEFAULT);
        System.out.println(response.getStatus().name());
   //     log.info("CLUSTER STATE: " + response.getStatus().name());
        if (response.isTimedOut()) {
            return   "cluster state is " + response.getStatus().name();
        }

        return "OK";
    }
    public boolean switchAlias() throws Exception {
       log.info("NEW ALIAS: " + RgdIndex.getNewAlias() + " || OLD ALIAS:" + RgdIndex.getOldAlias());
        IndicesAliasesRequest request = new IndicesAliasesRequest();


        if (RgdIndex.getOldAlias() != null) {

            IndicesAliasesRequest.AliasActions removeAliasAction =
                    new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                            .index(RgdIndex.getOldAlias())
                            .alias(rgdIndex.getIndex());
            IndicesAliasesRequest.AliasActions addAliasAction =
                    new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(RgdIndex.getNewAlias())
                            .alias(rgdIndex.getIndex());
            request.addAliasAction(removeAliasAction);
            request.addAliasAction(addAliasAction);
        //    log.info("Switched from " + RgdIndex.getOldAlias() + " to  " + RgdIndex.getNewAlias());

        }else{
            IndicesAliasesRequest.AliasActions addAliasAction =
                    new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(RgdIndex.getNewAlias())
                            .alias(rgdIndex.getIndex());
            request.addAliasAction(addAliasAction);
        //    log.info(rgdIndex.getIndex() + " pointed to " + RgdIndex.getNewAlias());
        }
        AcknowledgedResponse indicesAliasesResponse =
                ClientInit.getClient().indices().updateAliases(request, RequestOptions.DEFAULT);

        return  true;

    }
    public static Map<Long, List<String>> getGeneLociMap(int mapKey, String chromosome) throws Exception {
        GeneLociDAO dao= new GeneLociDAO();
        List<GeneLoci> loci=dao.getGeneLociByMapKeyAndChr(mapKey, chromosome);
        Map<Long, List<String>> positionGeneMap=new HashMap<>();

        for(GeneLoci g: loci){
            List<String> list=new ArrayList<>();
            if(positionGeneMap.get(g.getPosition())!=null){
               list=positionGeneMap.get(g.getPosition());

            }
            list.add(g.getGeneSymbols());
            positionGeneMap.put(g.getPosition(), list);
        }
        System.out.println("GeneLoci Map size of CHR-:"+ chromosome+"\t"+ positionGeneMap.size());

        return positionGeneMap;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public RgdIndex getRgdIndex() {
        return rgdIndex;
    }

    public void setRgdIndex(RgdIndex rgdIndex) {
        this.rgdIndex = rgdIndex;
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


    public BulkIndexProcessor getBulkIndexProcessor() {
        return bulkIndexProcessor;
    }

    public void setBulkIndexProcessor(BulkIndexProcessor bulkIndexProcessor) {
        this.bulkIndexProcessor = bulkIndexProcessor;
    }
}
