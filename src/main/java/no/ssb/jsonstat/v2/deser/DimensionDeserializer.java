package no.ssb.jsonstat.v2.deser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.base.Functions;
import com.google.common.collect.*;
import me.yanaga.guava.stream.MoreCollectors;
import no.ssb.jsonstat.v2.Dimension;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Deserialize dimensions to {@link no.ssb.jsonstat.v2.Dimension.Builder}.
 */
public class DimensionDeserializer extends JsonDeserializer<Dimension.Builder> {

    @Override
    public Dimension.Builder deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        if (p.getCurrentToken() == JsonToken.START_OBJECT) {
            p.nextToken();
        }

        String name = parseName(p, ctxt);
        Dimension.Builder dimension;
        dimension = Dimension.create(name);

        if (p.nextValue() != JsonToken.START_OBJECT)
            ctxt.reportWrongTokenException(
                    p,
                    JsonToken.START_OBJECT,
                    "dimension should be an object",
                    (Object) null
            );

        p.nextValue();

        while (p.nextValue() != JsonToken.END_OBJECT) {
            switch (p.getCurrentName()) {
                case "category":
                    parseCategory(dimension, p, ctxt);
                    break;
                case "label":
                    dimension.withLabel(parseLabel(p, ctxt));
                    break;
                default:
                    ctxt.handleUnknownProperty(
                            p,
                            this,
                            Dimension.Builder.class,
                            p.getCurrentName()
                    );
                    break;
            }
        }

        return dimension;
    }

    private void parseCategory(Dimension.Builder dimension, JsonParser p, DeserializationContext ctxt) throws IOException {
        Map<String, String> index = null;
        Map<String, String> label = null;
        while (p.nextValue() != JsonToken.END_OBJECT) {
            switch (p.getCurrentName()) {
                case "index":
                    index = parseIndex(p, ctxt);
                    break;
                case "label":
                    label = parseCategoryLabel(p, ctxt);
                    break;
                case "unit":
                    // TODO: Support units.
                    parseUnit(p, ctxt);
                    break;
                default:
                    ctxt.handleUnknownProperty(
                            p,
                            this,
                            Dimension.Builder.class,
                            p.getCurrentName()
                    );
                    break;
            }
        }
        checkArgument(!(index == null && label == null), "either label or index is required");

        // Once we have everything, we can build the dimension.

        if (index == null) {
            checkArgument(label.size() >= 1, "category label must contain a least one element if " +
                    "no index is provided");
            dimension.withCategories(ImmutableSet.copyOf(label.values()));
            return;
        }
        if (label == null) {
            dimension.withCategories(
                    ImmutableSet.copyOf(
                            index.keySet()
                    )
            );
            return;
        }

        // TODO: Maybe the checks should reside inside the builder?
        checkArgument(
                label.size() == index.size(),
                "label and index's sizes were inconsistent"
        );

        ImmutableMap<String, String> withIndex = index.keySet().stream()
                .collect(
                        MoreCollectors.toImmutableMap(
                                Function.identity(),
                                label::get
                        )
                );
        dimension.withIndexedLabels(withIndex);

    }

    private Map<String, String> parseCategoryLabel(JsonParser p, DeserializationContext ctxt) throws IOException {
        if (p.currentToken() != JsonToken.START_OBJECT)
            ctxt.reportWrongTokenException(
                    p, JsonToken.START_OBJECT,
                    "label was not an object", (Object) null
            );

        Map<String, String> label = ctxt.readValue(
                p, ctxt.getTypeFactory().constructMapType(
                        Map.class,
                        String.class,
                        String.class
                )
        );

        return checkNotNull(label, "label object was null");
    }

    private void parseUnit(JsonParser p, DeserializationContext ctxt) {

    }

    /**
     * Extract the dimension name.
     */
    private String parseName(JsonParser p, DeserializationContext ctxt) throws IOException {
        if (p.currentToken() != JsonToken.FIELD_NAME)
            ctxt.reportWrongTokenException(p, JsonToken.FIELD_NAME,
                    "could not determine dimension name", p.getCurrentToken());

        String name = checkNotNull(
                p.getCurrentName(),
                "dimension name cannot be null"
        );

        checkArgument(!name.isEmpty(), "dimension name cannot be empty");
        return name;
    }

    private String parseLabel(JsonParser p, DeserializationContext ctxt) throws IOException {
        String label = ctxt.readValue(p, String.class);
        checkArgument(!label.trim().isEmpty(), "label cannot be empty");
        return label;
    }

    private ImmutableMap<String, String> parseIndex(JsonParser p, DeserializationContext ctxt) throws IOException {
        // Index can either be an array or object with id values. Here we transform
        // both to array.
        ImmutableMap<String, String> index = null;
        JsonToken token = p.currentToken();
        if (token == JsonToken.START_ARRAY) {
            List<String> listIndex = ctxt.readValue(
                    p,
                    ctxt.getTypeFactory().constructCollectionType(
                            List.class,
                            String.class
                    )
            );
            index = IntStream.range(0, listIndex.size())
                    .boxed()
                    .collect(MoreCollectors.toImmutableMap(
                            listIndex::get, Object::toString
                    ));

        } else if (token == JsonToken.START_OBJECT) {
            Map<String, Integer> mapIndex = ctxt.readValue(
                    p,
                    ctxt.getTypeFactory().constructMapType(
                            Map.class,
                            String.class,
                            Integer.class
                    )
            );
            // Even though the type is String, the sorting actually uses the
            // integer value thanks to the forMap function.
            Ordering<String> byValue = Ordering.natural().onResultOf(
                    Functions.forMap(mapIndex)
            );


            index = ImmutableSortedMap.copyOf(
                    Maps.transformValues(mapIndex, Object::toString),
                    byValue
            );

        } else
            ctxt.reportMappingException("could not deserialize category index, need either an array " +
                    "or an object, got %s", token);

        // TODO: If the index is a map, should it starts with 0?
        return checkNotNull(index, "could not parse index");

    }
}
