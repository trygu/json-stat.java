package no.ssb.jsonstat.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;

public class DatasetDeserializationTest {

    private ObjectMapper mapper;

    @BeforeMethod
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule());

        SimpleModule module = new SimpleModule();
        module.addDeserializer(DatasetBuildable.class, new DatasetDeserializer());
        module.addDeserializer(Dimension.Builder.class, new DimensionDeserializer());
        mapper.registerModule(module);
    }

    @Test
    public void testGalicia() throws Exception {

        URL galicia = Resources.getResource(getClass(), "./galicia.json");

        Dataset jsonStat = mapper.readValue(
                new BufferedInputStream(
                        galicia.openStream()
                ),
                DatasetBuildable.class
        ).build();

        assertThat(jsonStat.getVersion()).isEqualTo("2.0");
        assertThat(jsonStat.getClazz()).isEqualTo("dataset");
        assertThat(jsonStat.getLabel()).contains("Population by province of residence, place of birth, age, gender and year in Galicia");
        assertThat(jsonStat.getSource()).contains("INE and IGE");
        assertThat(jsonStat.getUpdated()).contains(Instant.parse("2012-12-27T12:25:09Z"));

        Iterable<Map.Entry<List<String>, List<Number>>> limit = Iterables.limit(jsonStat.asMap().entrySet(), 5);
        assertThat(limit).containsExactly(
                entry(asList("T", "T", "T", "2001", "T"), singletonList(2695880)),
                entry(asList("T", "T", "T", "2001", "15"), singletonList(1096027)),
                entry(asList("T", "T", "T", "2001", "27"), singletonList(357648)),
                entry(asList("T", "T", "T", "2001", "32"), singletonList(338446)),
                entry(asList("T", "T", "T", "2001", "36"), singletonList(903759))
        );

    }
}