package edu.mcw.rgd.variantIndexerRgd.process;

import edu.mcw.rgd.services.ClientInit;
import edu.mcw.rgd.variantIndexerRgd.Manager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.*;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Created by jthota on 12/20/2019.
 */
public class MyThreadPoolExecutor extends ThreadPoolExecutor {
    Logger log=getLogger(Manager.class);
    public MyThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    @Override
    public void afterExecute(Runnable r, Throwable t){
        super.afterExecute(r,t);
        if(t==null && r instanceof Future){
            try{
                Object result=((Future) r).get();

            }catch (CancellationException e){
                t=e;
            }catch (ExecutionException e){
                t=e.getCause();
            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
            }
        }
        if(t!=null){
            System.err.println("Uncaught exception! "+t +" STACKTRACE:"+ Arrays.toString(t.getStackTrace()));
            log.info("Uncaught exception! "+t +" STACKTRACE:"+ Arrays.toString(t.getStackTrace()));

                try {
                    ClientInit.destroy();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            System.exit(1);
        }
    }
}
