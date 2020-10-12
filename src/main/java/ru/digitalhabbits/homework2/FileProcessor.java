package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;


public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();


    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);

        final Exchanger<List<Pair<String, Integer>>> exchanger = new Exchanger<>();
        final ExecutorService executorService = Executors.newFixedThreadPool(CHUNK_SIZE);

        final Thread writerThread = new Thread(new FileWriter(resultFileName, exchanger));
        // запускаем FileWriter в отдельном потоке
        writerThread.start();

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {

            while (scanner.hasNext()) {

                // вычитываем CHUNK_SIZE строк для параллельной обработки
                final List<String> buffer = readPortion(scanner);

                // обрабатываем строки с помощью LineProcessor. Каждый поток обрабатывает свою строку.
                final List<Pair<String, Integer>> processedLines = processLinesUsingLineProcessor(buffer, executorService);

                // добавляем обработанные данные в результирующий файл
                sendProcessedLinesToFileWriter(processedLines, exchanger);

            }
        } catch (IOException exception) {
            logger.error("", exception);
        }

        // останавливаем поток writerThread
        writerThread.interrupt();

        executorService.shutdown();

        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void sendProcessedLinesToFileWriter
            (List<Pair<String, Integer>> processedLines, Exchanger<List<Pair<String, Integer>>> exchanger) {
        try {
            exchanger.exchange(processedLines);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private List<Pair<String, Integer>> processLinesUsingLineProcessor
            (List<String> lines, ExecutorService executorService) {

        List<Pair<String, Integer>> result = new ArrayList<>();

        List<Future<Pair<String, Integer>>> futureResultList = new ArrayList<>();

        for (String line : lines) {
            Callable<Pair<String, Integer>> lineProcessorCallable = new LineProcessorThread(line);
            Future<Pair<String, Integer>> pairFuture = executorService.submit(lineProcessorCallable);
            futureResultList.add(pairFuture);
        }

        for (Future<Pair<String, Integer>> future : futureResultList) {
            try {
                result.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        return result;

    }

    private List<String> readPortion(Scanner scanner) {

        List<String> result = new ArrayList<>();

        int resultCounter = 0;
        while (resultCounter < CHUNK_SIZE && scanner.hasNextLine()) {
            String line = scanner.nextLine();
            result.add(line);
            resultCounter++;
        }

        return result;
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }

}
