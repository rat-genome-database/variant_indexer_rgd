package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.dao.impl.GeneLociDAO;
import edu.mcw.rgd.datamodel.GeneLoci;
import edu.mcw.rgd.datamodel.Sample;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ProcessChromosome implements  Runnable{
    private String chr;
    private int mapKey;
    List<Sample> samples;
    List<VariantTranscript> transcripts;
    BulkIndexProcessor bulkIndexProcessor;
    Map<Long, List<String>> geneLociMap;
    VariantDao vdao=new VariantDao();

    public ProcessChromosome(String chr, int mapKey,List<Sample> samples,  BulkIndexProcessor bulkIndexProcessor,
                             List<VariantTranscript> transcripts,
                             Map<Long, List<String>> geneLociMap){
        this.chr=chr;
        this.mapKey=mapKey;
        this.samples=samples;
        this.transcripts=transcripts;
        this.bulkIndexProcessor=bulkIndexProcessor;
        this.geneLociMap=geneLociMap;
    }
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+"\t*********** CHR: "+ chr+ "\t**************STARTED");

        ExecutorService executor = new MyThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Runnable workerThread=null;
      /*  Map<Long, List<String>> geneLociMap = null;
        try {
            geneLociMap = getGeneLociMap(mapKey, chr);
        } catch (Exception e) {
            e.printStackTrace();
        }*/
        for (Sample s : samples) {
            List<VariantIndex> indexList = null;
            try {
                indexList = vdao.getVariants(s.getId(),chr, mapKey );
            } catch (Exception e) {
                e.printStackTrace();
            }
            workerThread =new ProcessSample(indexList,geneLociMap,chr, s.getId(),bulkIndexProcessor,transcripts);
            executor.execute(workerThread);

        }
        executor.shutdown();
        while (!executor.isTerminated()) {}
        System.out.println(Thread.currentThread().getName()+"\t*********** CHR: "+ chr+ "\t*************/tEND");

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
        System.out.println("GeneLoci Map size: "+ positionGeneMap.size());

        return positionGeneMap;
    }
}
