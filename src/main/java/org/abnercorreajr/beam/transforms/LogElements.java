package org.abnercorreajr.beam.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.StringJoiner;


/**
 * Helper PTransform mainly used for tests / debugging.
 *
 * Support logging {@link PCollection} elements using {@link Logger}. Example:
 *
 * <pre>{@code
 * pcoll.apply(LogElements.withLevel(Level.INFO))
 * }</pre>
 *
 * Also supports writing to {@link System#out}. Example:
 *
 * <pre>{@code
 * pcoll.apply(LogElements.toSysOut())
 * }</pre>
 *
 * If running in Dataflow, use the {@link LogElements#withLevel(Level)} method to use slf4j, since System.out is not
 * captured in the Dataflow logs.
 *
 * @param <T> Generic type of the input and output PCollection.
 */
@SuppressWarnings("NullableProblems")
public class LogElements<T> extends PTransform<PCollection<T>, PCollection<T>> {

    private final Level level;
    private final boolean timeOnly;

    public LogElements(Level level, boolean timeOnly) {
        this.level = level;
        this.timeOnly = timeOnly;
    }

    /**
     * If running in Dataflow, use the {@link LogElements#withLevel(Level)} method to use slf4j, since System.out is not
     * captured in the Dataflow logs.
     *
     * @return
     * @param <T>
     */
    public static <T> LogElements<T> toSysOut() {
        return new LogElements<>(null, true);
    }

    /**
     * If running in Dataflow, use the {@link LogElements#withLevel(Level)} method to use slf4j, since System.out is not
     * captured in the Dataflow logs.
     *
     * @param timeOnly
     * @return
     * @param <T>
     */
    public static <T> LogElements<T> toSysOut(boolean timeOnly) {
        return new LogElements<>(null, timeOnly);
    }

    public static <T> LogElements<T> withLevel(Level level) {
        return new LogElements<>(level, true);
    }

    public static <T> LogElements<T> withLevel(Level level, boolean timeOnly) {
        return new LogElements<>(level, timeOnly);
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        return input.apply(ParDo.of(new LogElementsDoFn<>(level, timeOnly)));
    }

    /**
     * DoFn to log PCollection items. Supports writing to System.out
     *
     * @param <T>
     */
    static class LogElementsDoFn<T> extends DoFn<T, T> {

        private static final Logger logger = LoggerFactory.getLogger(LogElementsDoFn.class);

        private final Level level;
        private final boolean timeOnly;

        public LogElementsDoFn(Level level, boolean timeOnly) {
            this.level = level;
            this.timeOnly = timeOnly;
        }

        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
            StringJoiner lineBuilder = new StringJoiner(" - ");

            if (window instanceof IntervalWindow) {
                DateTimeFormatter tsFmt = ((timeOnly) ? DateTimeFormat.forPattern("HH:mm:ss.SSS") : ISODateTimeFormat.dateTime()).withZoneUTC();
                DateTimeFormatter windowTsFmt = ((timeOnly) ? DateTimeFormat.forPattern("HH:mm:ss") : ISODateTimeFormat.dateTime()).withZoneUTC();

                // Adds the element timestamp
                lineBuilder.add(c.timestamp().toString(tsFmt));

                // Adds window info
                IntervalWindow w = (IntervalWindow) window;
                lineBuilder.add("[" + w.start().toString(windowTsFmt) + ", " + w.end().toString(windowTsFmt) + ")");

                // Adds pane info
                PaneInfo p = c.pane();
                String paneInfo = new StringJoiner(",")
                    .add("pane=" + p.getIndex())
                    .add(String.valueOf(p.getNonSpeculativeIndex()))
                    .add(p.isFirst() ? "F" : " ")
                    .add(p.isLast() ? "L" : " ")
                    .add(c.pane().getTiming().name())
                    .toString();
                lineBuilder.add(paneInfo);
            }

            // Adds element. Will add the string "null" if element == null.
            lineBuilder.add(String.valueOf(c.element()));

            String logLine = lineBuilder.toString();

            if (level == null) {
                System.out.println(logLine);
            }
            else {
                switch (level) {
                    case TRACE:
                        logger.trace(logLine);
                        break;
                    case DEBUG:
                        logger.debug(logLine);
                        break;
                    case INFO:
                        logger.info(logLine);
                        break;
                    case WARN:
                        logger.warn(logLine);
                        break;
                    case ERROR:
                        logger.error(logLine);
                        break;
                }
            }

            c.output(c.element());
        }
    }
}


