package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.datamodel.Sample;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;

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
        for (Sample s : samples) {
            List<VariantIndex> indexList = null;
            try {
                indexList = vdao.getVariants(s.getId(),chr, mapKey );
            } catch (Exception e) {
                e.printStackTrace();
            }
            Runnable workerThread =new ProcessSample(indexList,geneLociMap,chr, s.getId(),bulkIndexProcessor,transcripts);
            executor.execute(workerThread);

        }
        VariantIndexUtils.awaitTermination(executor);
        System.out.println(Thread.currentThread().getName()+"\t*********** CHR: "+ chr+ "\t**************END");

    }
}
