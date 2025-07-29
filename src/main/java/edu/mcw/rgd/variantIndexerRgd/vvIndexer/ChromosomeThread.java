package edu.mcw.rgd.variantIndexerRgd.vvIndexer;

import edu.mcw.rgd.dao.impl.variants.VariantDAO;
import edu.mcw.rgd.datamodel.GeneLoci;
import edu.mcw.rgd.datamodel.variants.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.utils.MyThreadPoolExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ChromosomeThread  implements Runnable{
    private final String chr;
    private final int mapKey;
    private final int speciesTypeKey;
    private final List<GeneLoci> geneLoci;
    VariantDAO variantDao=new VariantDAO();
    protected static Logger logger= LogManager.getLogger();
    public ChromosomeThread(String chr, int mapKey, int speciesTypeKey, List<GeneLoci> geneLoci){
        this.chr=chr;
        this.mapKey=mapKey;
        this.speciesTypeKey=speciesTypeKey;
        this.geneLoci=geneLoci;
    }
    @Override
    public void run() {
        logger.info(Thread.currentThread().getName() + " || MAPKEY : " + mapKey + " CHROMOSOME:"+chr+" started .... " + new Date());

        ExecutorService executor2 = new MyThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Runnable variantsNewTableThread= null;
        try{
            int batchSize=1000;
            int offset=0;
            int count=0;
            while (true){
                List<VariantIndex> documents=variantDao.getVariantIndexDocs(mapKey,chr,batchSize, offset);
                if(documents==null || documents.size()==0){
                    break;
                }
                try {
                    variantsNewTableThread=new ProcessVariant(documents, mapKey, geneLoci, chr);
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
