package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ProcessSample  implements Runnable{
    private List<VariantIndex> indexList;
    private Map<Long, List<String>> geneLociMap;
    private String chr;
    private int sampleId;
    public ProcessSample( List<VariantIndex> indexList, Map<Long, List<String>> geneLociMap, String chr, int sampleId){
       this.indexList=indexList;
       this.geneLociMap=geneLociMap;
       this.chr=chr;
       this.sampleId=sampleId;
    }
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+"\tVARIANTS OF CHR: "+ chr+ "\tSAMPLE-"+sampleId+":"+indexList.size());

        ExecutorService executor = new MyThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Runnable workerThread=null;
        for(VariantIndex vi: indexList) {

            workerThread=new ProcessVariant(geneLociMap, vi);
            executor.execute(workerThread);

        }
        executor.shutdown();
        while (!executor.isTerminated()) {}
        System.out.println(Thread.currentThread().getName()+"CHR: "+ chr+ "\tSAMPLE-"+sampleId +"\t END");

    }
}
