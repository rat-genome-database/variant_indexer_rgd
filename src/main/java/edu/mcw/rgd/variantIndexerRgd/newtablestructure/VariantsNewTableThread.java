package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class VariantsNewTableThread implements Runnable{
    private List<Integer> variantIds;
    private int mapKey;

    VariantDao variantDao=new VariantDao();
    public VariantsNewTableThread(int mapKey, List<Integer> variantIds){
        this.variantIds=variantIds;
        this.mapKey=mapKey;
    }
    @Override
    public void run() {
        List<Integer> list = (List<Integer>) variantIds;
        //      for (Sample s : samples) {
        ExecutorService executor = new MyThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Runnable workerThread= null;
        List<VariantIndex> indexList = new ArrayList<>();
        try {
            indexList = variantDao.getVariantsNewTbaleStructure(mapKey, list);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //   workerThread = new ProcessPartChromosome(list,mapKey);
        if (indexList.size() > 0) {
            workerThread = new ProcessPartChromosome(indexList);
            executor.execute(workerThread);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {}
    }
}
