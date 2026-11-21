package io.quantos;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import org.junit.jupiter.api.Test;

class FeatureTest {
  @Test
  void duplicatesAndRevisionsAreHandledBeforeWindowBuffering() {
    var a = MarketFeatures.parse("{\"available_date\":\"2020-01-02\",\"event_id\":\"a\"}");
    var old = MarketFeatures.parse("{\"available_date\":\"2020-01-01\",\"event_id\":\"old\"}");
    var revision = MarketFeatures.parse("{\"available_date\":\"2020-01-02\",\"event_id\":\"new\"}");
    assertEquals("duplicate", MarketFeatures.disposition(a, a));
    assertEquals("late", MarketFeatures.disposition(a, old));
    assertEquals("accept", MarketFeatures.disposition(old, a));
    assertEquals("conflicting_revision", MarketFeatures.disposition(a, revision));
  }

  @Test
  void invalidRecordsDoNotAdvanceWatermarksOrPoisonTheJob() {
    assertEquals(Long.MIN_VALUE, MarketFeatures.timestamp("broken json"));
    assertEquals("_invalid", MarketFeatures.routingKey("broken json"));
    assertEquals("missing_identity", MarketFeatures.invalidReason(MarketFeatures.parse("{}")));
    var rejected = MarketFeatures.parse(MarketFeatures.quarantine("broken json", "invalid_json"));
    assertEquals("invalid_json", rejected.path("rejection_reason").asText());
    assertEquals("broken json", rejected.path("rejected_payload").asText());
  }

  @Test
  void replayClockIsNotBrokerTimestamp() {
    assertNull(MarketFeatures.transportRecord("topic", "{}").timestamp());
    assertTrue(
        MarketFeatures.timestamp(
                "{\"kind\":\"watermark_control\",\"series_id\":\"_control\",\"event_id\":\"clock\",\"stream_run_id\":\"fixture\",\"available_date\":\"2020-01-01\",\"watermark_date\":\"2030-01-01\"}")
            > MarketFeatures.timestamp(
                "{\"kind\":\"watermark_control\",\"series_id\":\"_control\",\"event_id\":\"clock\",\"stream_run_id\":\"fixture\",\"available_date\":\"2020-01-01\"}"));
  }

  @Test
  void yieldBpsAndNegativePrice() {
    var a =
        MarketFeatures.calculate(
            List.of(
                MarketFeatures.parse("{\"value\":4.25,\"quote_kind\":\"yield\"}"),
                MarketFeatures.parse("{\"value\":4.30,\"quote_kind\":\"yield\"}")));
    assertEquals(5, a.path("change1").asDouble(), 1e-9);
    var b =
        MarketFeatures.calculate(
            List.of(
                MarketFeatures.parse("{\"value\":20,\"quote_kind\":\"close\"}"),
                MarketFeatures.parse("{\"value\":-13.1,\"quote_kind\":\"close\"}")));
    assertTrue(b.path("change1").isNull());
  }

  @Test
  void completeWindowRequired() {
    List<JsonNode> rows = new ArrayList<>();
    for (int i = 0; i < 21; i++)
      rows.add(MarketFeatures.parse("{\"value\":" + (100 + i) + ",\"quote_kind\":\"close\"}"));
    assertTrue(MarketFeatures.calculate(rows).path("vol20").isNumber());
    assertTrue(MarketFeatures.calculate(rows.subList(0, 20)).path("vol20").isNull());
  }

  @Test
  void compactCheckpointStatePreservesCalculationsAndDeduplication() {
    List<JsonNode> rows = new ArrayList<>();
    for (int i = 0; i < 21; i++)
      rows.add(
          MarketFeatures.parse(
              "{\"value\":"
                  + (100 + i)
                  + ",\"quote_kind\":\"close\",\"event_id\":\"e"
                  + i
                  + "\",\"observation_date\":\"2020-01-"
                  + String.format("%02d", i + 1)
                  + "\",\"available_date\":\"2020-01-22\",\"source_file\":\"fixture.csv\"}"));
    var expected = MarketFeatures.calculate(rows);
    var state = MarketFeatures.rollingRecord(rows);
    List<JsonNode> restored = new ArrayList<>();
    state.forEach(restored::add);
    restored.set(20, rows.get(20)); // today's full observation carries source/evidence
    assertEquals(expected, MarketFeatures.calculate(restored));
    assertFalse(state.get(0).has("source_file"));
    var current = rows.get(20);
    assertEquals(
        "duplicate", MarketFeatures.disposition(MarketFeatures.guardRecord(current), current));
    assertEquals("r|s", MarketFeatures.routingKey("{\"stream_run_id\":\"r\",\"series_id\":\"s\"}"));
  }
}
