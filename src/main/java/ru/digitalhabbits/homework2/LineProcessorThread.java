package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.Callable;

public class LineProcessorThread implements Callable<Pair<String, Integer>> {

    private String line;

    public LineProcessorThread(String line) {
        this.line = line;
   }

    @Override
    public Pair<String, Integer> call() {
        return new LineCounterProcessor().process(line);
    }
}
