package no.ssb.jsonstat.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import no.ssb.jsonstat.v2.deser.DatasetDeserializer;
import no.ssb.jsonstat.v2.deser.DimensionDeserializer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.net.URL;
import java.time.Instant;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class DatasetDeserializationTest {

    private ObjectMapper mapper;

    @BeforeMethod
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule());

        SimpleModule module = new SimpleModule();
        module.addDeserializer(Dataset.Builder.class, new DatasetDeserializer());
        module.addDeserializer(Dimension.Builder.class, new DimensionDeserializer());
        mapper.registerModule(module);
    }

    @Test
    public void testExtension() throws Exception {

        URL galicia = Resources.getResource(getClass(), "./galicia.json");

        ObjectNode node = mapper.readValue(new BufferedInputStream(
                galicia.openStream()
        ), ObjectNode.class);

        assertThat(node).isNotNull();

        // Manually add extension
        node.with("extension")
                .put("number", 10)
                .putArray("array")
                    .add("string");

        Dataset jsonStat = mapper.readValue(
                mapper.writeValueAsBytes(node),
                Dataset.Builder.class
        ).build();

        assertThat(jsonStat).isNotNull();
        assertThat(jsonStat.getExtension())
                .isNotNull()
                .isInstanceOf(ObjectNode.class);

    }

    @Test
    public void testGalicia() throws Exception {

        URL galicia = Resources.getResource(getClass(), "./galicia.json");

        Dataset jsonStat = mapper.readValue(
                new BufferedInputStream(
                        galicia.openStream()
                ),
                Dataset.Builder.class
        ).build();

        assertThat(jsonStat.getVersion()).isEqualTo("2.0");
        assertThat(jsonStat.getClazz()).isEqualTo("dataset");
        assertThat(jsonStat.getLabel()).contains("Population by province of residence, place of birth, age, gender and year in Galicia");
        assertThat(jsonStat.getSource()).contains("INE and IGE");
        assertThat(jsonStat.getUpdated()).contains(Instant.parse("2012-12-27T12:25:09Z"));


        ImmutableMap<List<String>, List<Number>> limit = ImmutableMap.copyOf(
                Iterables.limit(jsonStat.getRowMap().entrySet(), 5)
        );
        assertThat(limit).containsExactly(
                entry(asList("T", "T", "T", "2001", "T", "pop"), singletonList(2695880)),
                entry(asList("T", "T", "T", "2001", "15", "pop"), singletonList(1096027)),
                entry(asList("T", "T", "T", "2001", "27", "pop"), singletonList(357648)),
                entry(asList("T", "T", "T", "2001", "32", "pop"), singletonList(338446)),
                entry(asList("T", "T", "T", "2001", "36", "pop"), singletonList(903759))
        );

    }
}