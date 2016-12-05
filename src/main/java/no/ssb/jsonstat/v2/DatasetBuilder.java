package no.ssb.jsonstat.v2;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;

/**
 * Base builder interface for {@link Dataset}
 */
public interface DatasetBuilder {

    DatasetBuilder withLabel(String label);

    DatasetBuilder withSource(String source);

    DatasetBuilder updatedAt(Instant update);

    DatasetBuilder withExtension(Object jsonNodes);

    DatasetValueBuilder withDimensions(Iterable<Dimension.Builder> values);

    DatasetValueBuilder withDimensions(Dimension.Builder... values);

}
