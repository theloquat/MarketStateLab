package io.quantos;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Frozen JSON model forward inference; no fitting or backward smoothing. */
public final class FrozenScorer {
  final JsonNode artifact;
  final double[] mean, scale, axis, weights, center;
  final double[][] components, means, variances, transition;
  final boolean hmm;

  public FrozenScorer(JsonNode a) {
    artifact = a;
    mean = vector(a.path("mean"));
    scale = vector(a.path("scale"));
    axis = vector(a.path("risk_axis"));
    weights = vector(a.path("weights"));
    means = matrix(a.path("means"));
    variances = matrix(a.path("covariances"));
    hmm = "hmm".equals(a.path("method").asText());
    transition = hmm ? matrix(a.path("transition")) : null;
    JsonNode p = a.path("projection");
    components = p.isObject() ? matrix(p.path("components")) : null;
    center = p.isObject() ? vector(p.path("center")) : null;
    if (mean.length != scale.length
        || axis.length != mean.length
        || weights.length != means.length
        || variances.length != means.length)
      throw new IllegalArgumentException("Invalid model dimensions");
    for (double value : scale)
      if (!(value > 0)) throw new IllegalArgumentException("Invalid model scaling");
    for (double[] row : variances)
      for (double value : row)
        if (!(value > 0)) throw new IllegalArgumentException("Invalid covariance");
  }

  static double[] vector(JsonNode a) {
    double[] result = new double[a.size()];
    for (int i = 0; i < result.length; i++) {
      result[i] = a.get(i).asDouble();
      if (!Double.isFinite(result[i]))
        throw new IllegalArgumentException("Non-finite model parameter");
    }
    return result;
  }

  static double[][] matrix(JsonNode a) {
    double[][] result = new double[a.size()][];
    for (int i = 0; i < result.length; i++) result[i] = vector(a.get(i));
    return result;
  }

  public ObjectNode infer(double[] values, double[] previous) {
    if (values.length != mean.length)
      throw new IllegalArgumentException("Model input dimensions differ");
    double[] z = new double[values.length];
    double risk = 0;
    ArrayNode contributions = MarketFeatures.JSON.createArrayNode();
    for (int j = 0; j < values.length; j++) {
      if (!Double.isFinite(values[j])) throw new IllegalArgumentException("Non-finite input");
      z[j] = (values[j] - mean[j]) / scale[j];
      double c = z[j] * axis[j];
      risk += c;
      contributions.add(c);
    }
    double[] working = z;
    if (components != null) {
      working = new double[components.length];
      for (int i = 0; i < working.length; i++)
        for (int j = 0; j < z.length; j++) working[i] += (z[j] - center[j]) * components[i][j];
    }
    double[] prior = weights.clone();
    if (hmm && previous != null) {
      if (previous.length != weights.length)
        throw new IllegalArgumentException("Invalid filtering dimensions");
      double total = 0;
      for (double v : previous) {
        if (!Double.isFinite(v) || v < 0)
          throw new IllegalArgumentException("Invalid filtering probability");
        total += v;
      }
      if (Math.abs(total - 1) > 1e-6)
        throw new IllegalArgumentException("Filtering state does not sum to one");
      java.util.Arrays.fill(prior, 0);
      for (int i = 0; i < previous.length; i++)
        for (int j = 0; j < prior.length; j++) prior[j] += previous[i] * transition[i][j];
    }
    double[] logs = new double[means.length];
    double maximum = Double.NEGATIVE_INFINITY;
    for (int i = 0; i < logs.length; i++) {
      double emission = 0;
      for (int j = 0; j < working.length; j++) {
        double delta = working[j] - means[i][j];
        emission += Math.log(2 * Math.PI * variances[i][j]) + delta * delta / variances[i][j];
      }
      logs[i] = Math.log(Math.max(prior[i], 1e-300)) - .5 * emission;
      maximum = Math.max(maximum, logs[i]);
    }
    double sum = 0;
    for (double log : logs) sum += Math.exp(log - maximum);
    double evidence = maximum + Math.log(sum);
    ObjectNode probabilities = MarketFeatures.JSON.createObjectNode();
    ArrayNode alpha = MarketFeatures.JSON.createArrayNode();
    int best = 0;
    double probability = -1;
    for (int i = 0; i < logs.length; i++) {
      double p = Math.exp(logs[i] - evidence);
      alpha.add(p);
      probabilities.put(artifact.path("names").path(Integer.toString(i)).asText(), p);
      if (p > probability) {
        best = i;
        probability = p;
      }
    }
    ObjectNode out = MarketFeatures.JSON.createObjectNode();
    out.put("state", artifact.path("names").path(Integer.toString(best)).asText());
    out.put("probability", probability);
    out.set("probabilities", probabilities);
    out.put("risk_score", risk);
    out.set("contributions", contributions);
    out.put("log_evidence", evidence);
    out.put("rule_baseline", risk > .75 ? "高压力" : risk < -.75 ? "低压力" : "中等压力");
    if (hmm) out.set("filtering_state", alpha);
    return out;
  }
}
