package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.datamodel.GeneLoci;
import edu.mcw.rgd.datamodel.Variant;
import edu.mcw.rgd.services.IndexDocument;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ChromosomeThread extends VariantDao implements Runnable{
    private String chr;
    private int mapKey;
    private int speciesTypeKey;
    private  List<GeneLoci> geneLoci;
    VariantDao variantDao=new VariantDao();
    public ChromosomeThread(String chr, int mapKey, int speciesTypeKey, List<GeneLoci> geneLoci){
        this.chr=chr;
        this.mapKey=mapKey;
        this.speciesTypeKey=speciesTypeKey;
        this.geneLoci=geneLoci;
    }
    @Override
    public void run() {
        ExecutorService executor2 = new MyThreadPoolExecutor(5, 5, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Runnable variantsNewTableThread= null;
//        List<Integer> variantIds = null;
//        try {
//            variantIds = variantDao.getUniqueVariantsIds(chr, mapKey, speciesTypeKey);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println("UNIQUE VAIANTS SIZE of CHR:" + chr + ":\t" + variantIds.size());
//        Collection[] collections = new Collection[0];
//        try {
//            collections = split(variantIds, 1000);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        for (int i = 0; i < collections.length; i++) {
//            variantsNewTableThread=new VariantsNewTableThread(mapKey, (List<Integer>) collections[i]);
//            executor2.execute(variantsNewTableThread);
//        }
        try{
            int batchSize=1000;
            int offset=0;
            int count=0;
            int commitInterval=0;
            while (true){
                List<VariantIndex> documents=variantDao.getVariantDocs(mapKey,chr,batchSize, offset);
                if(documents==null || documents.size()==0){
                    break;
                }
                try {
                    variantsNewTableThread=new ProcessPartChromosome(documents, mapKey, geneLoci);
                    executor2.execute(variantsNewTableThread);
                }catch (Exception e){e.printStackTrace();}
                offset+=batchSize;
                count++;

            }

        }catch (Exception e){
            e.printStackTrace();
        }
        executor2.shutdown();
        while (!executor2.isTerminated()) {}
    }
    public Collection[] split(List<Integer> rgdids, int size) throws Exception {
        int numOfBatches = rgdids.size() / size + 1;
        Collection[] batches = new Collection[numOfBatches];

        for(int index = 0; index < numOfBatches; ++index) {
            int count = index + 1;
            int fromIndex = Math.max((count - 1) * size, 0);
            int toIndex = Math.min(count * size, rgdids.size());
            batches[index] = rgdids.subList(fromIndex, toIndex);
        }

        return batches;
    }
}
