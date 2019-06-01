package io.blockchainetl.analyticsdemo.utils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimeUtils {

    private static final ZoneId UTC = ZoneId.of("UTC");

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter
        .ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(UTC);
    
    public static ZonedDateTime convertToZonedDateTime(Long unixTimestamp) {
        Instant blockInstant = Instant.ofEpochSecond(unixTimestamp);
        return ZonedDateTime.from(blockInstant.atZone(UTC));
    }
    
    public static String formatTimestamp(ZonedDateTime dateTime) {
        return TIMESTAMP_FORMATTER.format(dateTime); 
    }

    public static String formatTimestamp(Long unixTimestamp) {
        return formatTimestamp(convertToZonedDateTime(unixTimestamp));
    }
}
