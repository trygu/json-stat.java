package no.ssb.jsonstat.v2.deser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import no.ssb.jsonstat.v2.Dataset;
import no.ssb.jsonstat.v2.Dimension;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Deserializer for Dataset.
 * <p>
 * TODO: Use builder instead.
 * TODO: Check {@link com.fasterxml.jackson.databind.deser.ResolvableDeserializer}
 */
public class DatasetDeserializer extends StdDeserializer<Dataset.Builder> {

    static final TypeReference<List<Number>> VALUES_LIST = new TypeReference<List<Number>>() {
    };
    static final TypeReference<Map<String, Dimension.Builder>> DIMENSION_MAP = new TypeReference<Map<String, Dimension.Builder>>() {
    };
    static final TypeReference<Set<String>> ID_SET = new TypeReference<Set<String>>() {
    };
    static final TypeReference<List<Integer>> SIZE_LIST = new TypeReference<List<Integer>>() {
    };
    static final TypeReference<ArrayListMultimap<String, String>> ROLE_MULTIMAP = new TypeReference<ArrayListMultimap<String, String>>() {
    };

    public DatasetDeserializer() {
        super(Dataset.Builder.class);
    }

    @Override
    public Dataset.Builder deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        if (p.getCurrentToken() == JsonToken.START_OBJECT) {
            p.nextToken();
        }

        Set<String> ids;
        List<Integer> sizes;
        Multimap<String, String> roles;

        Dataset.Builder builder = Dataset.create();
        while (p.nextValue() != JsonToken.END_OBJECT) {
            switch (p.getCurrentName()) {
                case "label":
                    builder.withLabel(_parseString(p, ctxt));
                    break;
                case "source":
                    builder.withSource(_parseString(p, ctxt));
                    break;
                case "href":
                    break;
                case "updated":
                    builder.updatedAt(
                            p.readValueAs(Instant.class)
                    );
                    break;
                case "value":
                    List<Number> values = p.readValueAs(
                            VALUES_LIST
                    );
                    builder.withValues(values);
                    break;
                case "dimension":
                    Map<String, Dimension.Builder> dims;
                    dims = p.readValueAs(DIMENSION_MAP);
                    builder.withDimensions(dims.values());
                    break;
                case "id":
                    ids = p.readValueAs(ID_SET);
                    break;
                case "size":
                    sizes = p.readValueAs(SIZE_LIST);
                    break;
                case "role":
                    roles = p.readValueAs(ROLE_MULTIMAP);
                    break;
                case "link":
                case "version":
                case "class":
                default:
                    p.skipChildren();
                    break;
            }
        }

        return builder;
    }
}
