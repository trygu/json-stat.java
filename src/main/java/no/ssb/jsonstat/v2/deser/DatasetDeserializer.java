package no.ssb.jsonstat.v2.deser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import no.ssb.jsonstat.v2.Dataset;
import no.ssb.jsonstat.v2.Dimension;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Deserializer for Dataset.
 * <p>
 * TODO: Use builder instead.
 * TODO: Check {@link com.fasterxml.jackson.databind.deser.ResolvableDeserializer}
 */
public class DatasetDeserializer extends JsonDeserializer<Dataset> {

    @Override
    public Dataset deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        if (p.getCurrentToken() == JsonToken.START_OBJECT) {
            p.nextToken();
        }

        Set<String> ids;
        List<Integer> size;
        Multimap<String, String> roles;

        Dataset.Builder builder = Dataset.create();
        while (p.nextValue() != JsonToken.END_OBJECT) {
            switch (p.getCurrentName()) {
                case "label":
                    builder.withLabel(p.getValueAsString());
                    break;
                case "source":
                    builder.withSource(p.getValueAsString());
                    break;
                case "href":
                    break;
                case "updated":
                    builder.updatedAt(
                            p.readValueAs(Instant.class)
                    );
                    break;
                case "value":
                    List<Number> values = ctxt.readValue(
                            p,
                            ctxt.getTypeFactory().constructCollectionType(
                                    ArrayList.class,
                                    Number.class
                            ));
                    builder.withValues(values);
                    break;
                case "dimension":
                    Map<String, Dimension.Builder> dims = ctxt.readValue(p, ctxt.getTypeFactory().constructMapType(
                            Map.class,
                            String.class,
                            Dimension.Builder.class
                    ));
                    break;
                case "id":
                    ids = ctxt.readValue(p,
                            ctxt.getTypeFactory().constructCollectionType(
                                    Set.class,
                                    String.class
                            ));
                    break;
                case "size":
                    size = ctxt.readValue(p, ctxt.getTypeFactory().constructCollectionType(
                            List.class,
                            Integer.class
                    ));
                    break;
                case "role":
                    roles = ctxt.readValue(p,
                            ctxt.getTypeFactory().constructParametricType(
                                    ArrayListMultimap.class,
                                    String.class,
                                    String.class
                            ));
                    break;
                case "link":
                case "version":
                case "class":
                default:
                    p.skipChildren();
                    break;
            }
        }

        return builder.build();
    }
}
