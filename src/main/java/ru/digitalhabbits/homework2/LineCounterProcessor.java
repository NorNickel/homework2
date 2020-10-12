package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.util.concurrent.ThreadLocalRandom;

import static org.slf4j.LoggerFactory.getLogger;

public class LineCounterProcessor
        implements LineProcessor<Integer> {

    private static final Logger logger = getLogger(LineCounterProcessor.class);

    private static final int BASE_DELAY = 200;
    private static final int RANDOM_DELAY = 200;

    @Nonnull
    @Override
    public Pair<String, Integer> process(@Nonnull String line) {
        // подсчет кол-ва символов в строке + произвольная задержка randomSleep()
        randomSleep();
        int symbolCount = line.toCharArray().length;
        return Pair.of(line, symbolCount);
    }

    private void randomSleep() {
        try {
            Thread.sleep(BASE_DELAY + ThreadLocalRandom.current().nextInt(RANDOM_DELAY));
        } catch (InterruptedException exception) {
            logger.warn("", exception);
        }
    }
}
