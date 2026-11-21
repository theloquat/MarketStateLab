package io.quantos;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

class ScorerTest {
  @Test
  void matchesPythonForwardFilterAndPcaGoldenFixtures() throws Exception {
    JsonNode fixtures =
        MarketFeatures.JSON.readTree(getClass().getResourceAsStream("/scorer-fixtures.json"));
    for (JsonNode fixture : fixtures) {
      FrozenScorer scorer = new FrozenScorer(fixture.path("artifact"));
      for (JsonNode step : fixture.path("steps")) {
        JsonNode previous = step.path("previous");
        var actual =
            scorer.infer(
                FrozenScorer.vector(step.path("values")),
                previous.isNull() ? null : FrozenScorer.vector(previous));
        var expected = step.path("expected");
        assertEquals(expected.path("state").asText(), actual.path("state").asText());
        for (String key : java.util.List.of("probability", "risk_score", "log_evidence"))
          assertEquals(expected.path(key).asDouble(), actual.path(key).asDouble(), 1e-12, key);
        expected
            .path("probabilities")
            .fieldNames()
            .forEachRemaining(
                key ->
                    assertEquals(
                        expected.path("probabilities").path(key).asDouble(),
                        actual.path("probabilities").path(key).asDouble(),
                        1e-12));
      }
      var config = MarketFeatures.JSON.createObjectNode();
      config.put("artifact_canonical", fixture.path("canonical").asText());
      config.putObject("card").put("artifact_hash", fixture.path("artifact_hash").asText());
      assertEquals(fixture.path("artifact"), RunInference.validatedArtifact(config));
      config.withObject("card").put("artifact_hash", "invalid");
      assertThrows(IllegalArgumentException.class, () -> RunInference.validatedArtifact(config));
    }
  }

  @Test
  void rejectsInvalidInputAndProbabilityState() throws Exception {
    JsonNode fixture =
        MarketFeatures.JSON
            .readTree(getClass().getResourceAsStream("/scorer-fixtures.json"))
            .get(0);
    var scorer = new FrozenScorer(fixture.path("artifact"));
    assertThrows(
        IllegalArgumentException.class, () -> scorer.infer(new double[] {Double.NaN, 0}, null));
    assertThrows(IllegalArgumentException.class, () -> scorer.infer(new double[] {0}, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> scorer.infer(new double[] {0, 0}, new double[] {.1, .2}));
  }
}
