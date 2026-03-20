package edu.mcw.rgd.variantIndexerRgd;


import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.SampleDAO;
import edu.mcw.rgd.datamodel.*;

import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;

import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.services.ClientInit;
import edu.mcw.rgd.services.IndexAdmin;

import edu.mcw.rgd.variantIndexerRgd.newtablestructure.*;

import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;


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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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
    private String fromChr;
    private String toChr;
    private String command;     //update or reindex
    private String process;     // transcripts or variants
    private String env;     // dev or test or prod
    List<String> chromosomes;

    BulkIndexProcessor bulkIndexProcessor;

    static Logger log=getLogger("main");

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
          ClientInit.destroy();
            e.printStackTrace();
        } finally {
            manager.bulkIndexProcessor.destroy();
            ClientInit.destroy();
        }
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
                log.info("Processing "+species+" variants...");
               log.info("CHROMOSOMES SIZE: "+ chromosomes.size());

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

    public String getClusterHealth(String index) throws Exception {
        ClusterHealthRequest request = new ClusterHealthRequest(index);
        ClusterHealthResponse response = ClientInit.getClient().cluster().health(request, RequestOptions.DEFAULT);
        log.info(response.getStatus().name());
        if (response.isTimedOut()) {
            return   "cluster state is " + response.getStatus().name();
        }
        return "OK";
    }

    public void switchAlias() throws Exception {
        String newAlias = rgdIndex.getNewAlias();
        String oldAlias = rgdIndex.getOldAlias();
        String indexName = rgdIndex.getIndex();
        log.info("Switched Alias!!\nNEW ALIAS: " + newAlias + " || OLD ALIAS:" + oldAlias);
        System.out.println("Switched Alias!!\nNEW ALIAS: " + newAlias + " || OLD ALIAS:" + oldAlias);
        IndicesAliasesRequest request = new IndicesAliasesRequest();

        if (oldAlias != null) {
            request.addAliasAction(
                    new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                            .index(oldAlias)
                            .alias(indexName));
        }
        request.addAliasAction(
                new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                        .index(newAlias)
                        .alias(indexName));

        ClientInit.getClient().indices().updateAliases(request, RequestOptions.DEFAULT);
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

    public void setRgdIndex(RgdIndex rgdIndex) {
    }
}
