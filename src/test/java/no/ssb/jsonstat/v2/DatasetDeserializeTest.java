package no.ssb.jsonstat.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.io.Resources;
import no.ssb.jsonstat.v2.deser.DatasetDeserializer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.net.URL;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by hadrien on 07/07/16.
 */
public class DatasetDeserializeTest {

    private ObjectMapper mapper;

    @BeforeMethod
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule());

        SimpleModule module = new SimpleModule();
        module.addDeserializer(Dataset.class, new DatasetDeserializer());
        mapper.registerModule(module);
    }

    @Test
    public void testGalicia() throws Exception {


        URL galicia = Resources.getResource(getClass(), "./galicia.json");

        Dataset jsonStat = mapper.readValue(
                new BufferedInputStream(
                        galicia.openStream()
                ),
                Dataset.class
        );

        assertThat(jsonStat.getVersion()).isEqualTo("2.0");
        assertThat(jsonStat.getClazz()).isEqualTo("dataset");
        assertThat(jsonStat.getLabel()).contains("Population by province of residence, place of birth, age, gender and year in Galicia");
        assertThat(jsonStat.getSource()).contains("INE and IGE");
        assertThat(jsonStat.getUpdated()).contains(Instant.parse("2012-12-27T12:25:09Z"));

    }
}