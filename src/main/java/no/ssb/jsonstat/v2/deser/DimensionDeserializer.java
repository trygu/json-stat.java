package no.ssb.jsonstat.v2.deser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import no.ssb.jsonstat.v2.Dimension;

import java.io.IOException;

/**
 * Created by hadrien on 08/07/16.
 */
public class DimensionDeserializer extends JsonDeserializer<Dimension> {

    @Override
    public Dimension deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        return null;
    }
}
