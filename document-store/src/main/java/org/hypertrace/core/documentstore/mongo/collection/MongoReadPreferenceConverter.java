package org.hypertrace.core.documentstore.mongo.collection;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.secondaryPreferred;
import static java.util.Map.entry;
import static org.hypertrace.core.documentstore.model.options.DataFreshness.NEAR_REALTIME_FRESHNESS;
import static org.hypertrace.core.documentstore.model.options.DataFreshness.REALTIME_FRESHNESS;
import static org.hypertrace.core.documentstore.model.options.DataFreshness.SYSTEM_DEFAULT;

import com.mongodb.ReadPreference;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.documentstore.model.options.DataFreshness;

public class MongoReadPreferenceConverter {
  private static final Map<DataFreshness, ReadPreference> DATA_FRESHNESS_TO_READ_PREFERENCE =
      Map.ofEntries(
          entry(SYSTEM_DEFAULT, primary()),
          entry(REALTIME_FRESHNESS, primary()),
          entry(NEAR_REALTIME_FRESHNESS, secondaryPreferred()));

  public static ReadPreference convert(final DataFreshness dataFreshness) {
    if (dataFreshness == null) {
      return primary();
    }

    return Optional.ofNullable(DATA_FRESHNESS_TO_READ_PREFERENCE.get(dataFreshness))
        .orElseThrow(
            () ->
                new UnsupportedOperationException("Unsupported data freshness: " + dataFreshness));
  }
}
