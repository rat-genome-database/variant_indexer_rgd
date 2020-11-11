package edu.mcw.rgd.variantIndexerRgd.newtablestructure;


import edu.mcw.rgd.datamodel.Sample;
import edu.mcw.rgd.variantIndexerRgd.Variants;
import edu.mcw.rgd.variantIndexerRgd.model.VariantData;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ProcessingThread implements Runnable {
    private Map<Long, List<String>> geneLoci;
    private int mapKey;
    private String chromosome;
    private int speciesTypeKey;
    private List<Sample> samples;
    public ProcessingThread(int mapKey,int speciesTypeKey, String chromosome,Map<Long, List<String>> geneLoci, List<Sample> samples){
        this.geneLoci=geneLoci;
        this.mapKey=mapKey;
        this.chromosome=chromosome;
        this.speciesTypeKey=speciesTypeKey;
        this.samples=samples;
    }
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+ "\tMapKey:"+mapKey + "\tchromosome:"+chromosome +"\tSAMPLES:"+samples.size());
        ExecutorService executor = new MyThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

      //  System.out.println("SAMPLES SIZE: "+samples.size());
        for(Sample s:samples) {
            // int sampleId = 911;
            int sampleId = s.getId();
            Variants variants = new Variants();
            List<VariantData> vrs = null;
            try {
                vrs = variants.getVariants(speciesTypeKey, chromosome, mapKey, sampleId);
            } catch (Exception e) {
                e.printStackTrace();
            }

            if(vrs!=null && vrs.size()>0) {
                System.out.println("Variants SIZE: " + vrs.size());
                Runnable workerThread = new Indexer(vrs, geneLoci,mapKey,  chromosome);
                executor.execute(workerThread);
            }
        }
        executor.shutdown();
        while (!executor.isTerminated()) {}
        System.out.println("***********"+Thread.currentThread().getName()+ "\tMapKey:"+mapKey + "\tchromosome:"+chromosome+ "\tEND ...."+"\t"+ new Date()+"*********");

    }
}
