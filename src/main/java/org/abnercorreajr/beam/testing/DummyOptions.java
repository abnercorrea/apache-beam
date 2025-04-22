package org.abnercorreajr.beam.testing;

/**
 * Dummy option values that can be used for testing
 */
public class DummyOptions {
    // A public topic that can pass topic and project name validation.
    public static final String PUBLIC_TAXI_RIDES_TOPIC = "projects/pubsub-public-data/topics/taxirides-realtime";

    // Public tables that can pass project, dataset and table name validation.
    public static final String PUBLIC_COVID19_TABLE = "bigquery-public-data.covid19_open_data.covid19_open_data";
    public static final String PUBLIC_COUNTRY_CODES_TABLE = "bigquery-public-data.country_codes.country_codes";
}
