package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Exchanger;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {

    private static final Logger logger = getLogger(FileWriter.class);
    private String resultFileName;
    private Exchanger<List<Pair<String, Integer>>> exchanger;

    public FileWriter
            (@Nonnull String resultFileName, @Nonnull Exchanger<List<Pair<String, Integer>>> exchanger) {

        this.resultFileName = resultFileName;
        this.exchanger = exchanger;
    }

    @Override
    public void run() {

        logger.info("Started writer thread {}", currentThread().getName());

        try(final java.io.FileWriter writer = new java.io.FileWriter(resultFileName, false)) {

            while (!currentThread().isInterrupted()) {

                List<Pair<String, Integer>> pairsToWrite;

                try {
                    pairsToWrite = exchanger.exchange(null);
                } catch (InterruptedException e) {
                    break;
                }

                for (Pair<String, Integer> pair : pairsToWrite) {
                    String text = pair.getLeft() + " " + pair.getRight();
                    writer.write(text + "\n");
                }
                writer.flush();
            }

        } catch(IOException ex){
            logger.error(ex.getMessage());
        }

        logger.info("Finish writer thread {}", currentThread().getName());
    }
}
