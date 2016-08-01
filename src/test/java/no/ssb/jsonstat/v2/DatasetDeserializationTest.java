package no.ssb.jsonstat.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.io.Resources;
import no.ssb.jsonstat.v2.deser.DatasetDeserializer;
import no.ssb.jsonstat.v2.deser.DimensionDeserializer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.net.URL;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

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

        Set<String> dimensions = jsonStat.getDimension().keySet();
        for (List<Number> numbers : jsonStat.getRows()) {
            Iterator<String> nameIt = dimensions.iterator();
            Iterator<Number> valueIt = numbers.iterator();
            while (nameIt.hasNext() && valueIt.hasNext())
                System.err.println(nameIt.next() + ": " + valueIt.next());

            System.err.println("---");
        }

    }
}