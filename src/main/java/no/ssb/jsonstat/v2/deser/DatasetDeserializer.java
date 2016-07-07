package no.ssb.jsonstat.v2.deser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import no.ssb.jsonstat.v2.Dataset;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;

/**
 * Created by hadrien on 07/07/16.
 */
public class DatasetDeserializer extends JsonDeserializer<Dataset> {

    @Override
    public Dataset deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        if (p.getCurrentToken() == JsonToken.START_OBJECT) {
            p.nextToken();
        }

        Dataset.Builder builder = Dataset.create();
        JsonToken currentToken;
        while ((currentToken = p.nextValue()) != JsonToken.END_OBJECT) {
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
                    // TODO: Check cast.
                    builder.withValues(
                            (Collection<Number>) p.readValueAs(
                                    new TypeReference<List<Number>>() {
                                    })
                    );
                    break;
                case "id":
                case "size":
                case "role":
                case "dimension":
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
