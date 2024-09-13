package org.hypertrace.core.documentstore.mongo.query;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.secondaryPreferred;
import static java.util.Map.entry;
import static org.hypertrace.core.documentstore.model.options.DataFreshness.NEAR_REAL_TIME_FRESHNESS;
import static org.hypertrace.core.documentstore.model.options.DataFreshness.REAL_TIME_FRESHNESS;

import com.mongodb.ReadPreference;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.documentstore.model.options.DataFreshness;

public class MongoReadPreferenceMapper {
  private static final Map<DataFreshness, ReadPreference> DATA_FRESHNESS_TO_READ_PREFERENCE =
      Map.ofEntries(
          entry(REAL_TIME_FRESHNESS, primary()),
          entry(NEAR_REAL_TIME_FRESHNESS, secondaryPreferred()));

  public ReadPreference readPreferenceFor(final DataFreshness dataFreshness) {
    return Optional.ofNullable(DATA_FRESHNESS_TO_READ_PREFERENCE.get(dataFreshness))
        .orElseThrow(
            () ->
                new UnsupportedOperationException("Unsupported data freshness: " + dataFreshness));
  }
}
