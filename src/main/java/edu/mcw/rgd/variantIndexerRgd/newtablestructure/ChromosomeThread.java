package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.datamodel.GeneLoci;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ChromosomeThread extends VariantDao implements Runnable{
    private final String chr;
    private final int mapKey;
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
        logger.info(Thread.currentThread().getName() + " || MAPKEY : " + mapKey + " CHROMOSOME:"+chr+" started .... " + new Date());

        ExecutorService executor2 = new MyThreadPoolExecutor(5, 5, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Runnable variantsNewTableThread= null;
        try{
            int batchSize=1000;
            int offset=0;
            int count=0;
            while (true){
                List<VariantIndex> documents=variantDao.getVariantDocs(mapKey,chr,batchSize, offset);
                if(documents==null || documents.size()==0){
                    break;
                }
                try {
                    variantsNewTableThread=new ProcessVariant(documents, mapKey, geneLoci);
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
        logger.info(Thread.currentThread().getName() + " || MAPKEY : " + mapKey + " CHROMOSOME:"+chr+" END .... " + new Date());

    }

}
