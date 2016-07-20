package no.ssb.jsonstat.v2.deser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import no.ssb.jsonstat.v2.Dimension;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DimensionDeserializerTest {

    private ObjectMapper mapper;

    @BeforeMethod
    public void setUp() throws Exception {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Dimension.Builder.class, new DimensionDeserializer());

        mapper = new ObjectMapper();
        mapper.registerModule(module);
    }

    @Test
    public void testComplete() throws Exception {

        String json = "" +
                "{ " +
                " \"birth\" : {" +
                "   \"label\" : \"place of birth\"," +
                "     \"category\" : {" +
                "       \"index\" : [\"T\", \"C\", \"P\", \"G\", \"A\", \"F\"]," +
                "       \"label\" : {" +
                "         \"T\" : \"total\"," +
                "         \"C\" : \"county of residence\"," +
                "         \"P\" : \"another county in the same province\"," +
                "         \"G\" : \"another province of Galicia\"," +
                "         \"A\" : \"in another autonomous community\"," +
                "         \"F\" : \"abroad\"" +
                "       }" +
                "     }" +
                "   }" +
                "}";

        Dimension.Builder builder = mapper.readValue(json, Dimension.Builder.class);

        System.err.println(builder);
    }

    @Test
    public void testNoLabel() throws Exception {

        String json = "" +
                "{" +
                "\"time\" : {" +
                "  \"label\" : \"year\"," +
                "  \"category\" : {" +
                "    \"index\" : {" +
                "      \"2001\" : 0," +
                "      \"2011\" : 1" +
                "    }" +
                "  }" +
                "}";

        Dimension.Builder builder = mapper.readValue(json, Dimension.Builder.class);

        System.err.println(builder);
    }

    @Test
    public void testNoIndex() throws Exception {

        String json = "" +
                "{" +
                "\"gender\" : {" +
                "    \"label\" : \"gender\"," +
                "    \"category\" : {" +
                "      \"label\" : {" +
                "        \"T\" : \"total\"," +
                "        \"M\" : \"male\"," +
                "        \"F\" : \"female\"" +
                "      }" +
                "    }" +
                "  }" +
                "}";

        Dimension.Builder builder = mapper.readValue(json, Dimension.Builder.class);

        System.err.println(builder);
    }
}