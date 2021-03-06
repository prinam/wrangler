package co.cask.wrangler.config;

import co.cask.wrangler.api.DirectiveConfig;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link Config}
 */
public class DirectiveConfigTest {

  private static final String SPECIFICATION = "{\n" +
    "\t\"exclusions\" : [\n" +
    "\t\t\"parse-as-csv\",\n" +
    "\t\t\"parse-as-excel\",\n" +
    "\t\t\"set\",\n" +
    "\t\t\"invoke-http\"\n" +
    "\t],\n" +
    "\n" +
    "\t\"aliases\" : {\n" +
    "\t\t\"json-parser\" : \"parse-as-json\",\n" +
    "\t\t\"js-parser\" : \"parse-as-json\"\n" +
    "\t}\n" +
    "}";

  private static final String ONLY_EXCLUSIONS = "\n" +
    "{\n" +
    "\t\"exclusions\" : [\n" +
    "\t\t\"parse-as-csv\",\n" +
    "\t\t\"parse-as-excel\",\n" +
    "\t\t\"set\",\n" +
    "\t\t\"invoke-http\"\n" +
    "\t]\n" +
    "}";

  private static final String ONLY_ALIASES = "{\n" +
    "\t\"aliases\" : {\n" +
    "\t\t\"json-parser\" : \"parse-as-json\",\n" +
    "\t\t\"js-parser\" : \"parse-as-json\"\n" +
    "\t}\n" +
    "}";

  private static final String EMPTY = "{}";

  @Test
  public void testParsingOfConfiguration() throws Exception {
    DirectiveConfig config = new Gson().fromJson(SPECIFICATION, DirectiveConfig.class);
    Assert.assertNotNull(config);
    Assert.assertEquals(4, config.getExclusions().size());
    Assert.assertEquals(2, config.getAliases().size());
  }

  @Test
  public void testParsingOnlyExclusions() throws Exception {
    DirectiveConfig config = new Gson().fromJson(ONLY_EXCLUSIONS, DirectiveConfig.class);
    Assert.assertNotNull(config);
    Assert.assertEquals(4, config.getExclusions().size());
    Assert.assertNull(config.getAliases());
  }

  @Test
  public void testParsingOnlyAliases() throws Exception {
    DirectiveConfig config = new Gson().fromJson(ONLY_ALIASES, DirectiveConfig.class);
    Assert.assertNotNull(config);
    Assert.assertEquals(2, config.getAliases().size());
    Assert.assertNull(config.getExclusions());
  }

  @Test
  public void testParsingEmpty() throws Exception {
    DirectiveConfig config = new Gson().fromJson(EMPTY, DirectiveConfig.class);
    Assert.assertNotNull(config);
    Assert.assertNull(config.getAliases());
    Assert.assertNull(config.getExclusions());
  }

}