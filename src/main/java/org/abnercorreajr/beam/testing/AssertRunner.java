package org.abnercorreajr.beam.testing;

import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssertRunner {

    private static final Logger LOG = LoggerFactory.getLogger(AssertRunner.class);

    private static boolean enabled = true;

    public static void enable() {
        enabled = true;
    }

    public static void disable() {
        enabled = false;
    }

    public static <T> void run(AssertAction<T> action, PCollection<T> pColl) {
        if (enabled) {
            LOG.info("Running assertions for {}...", action.getClass());

            action.runAsserts(pColl);
        }
        else {
            LOG.warn("{} is DISABLED!!! no assertions (AsserAction) will be executed.", AssertRunner.class.getSimpleName());
        }
    }
}
