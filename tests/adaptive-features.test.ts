import { expect, test } from "bun:test";

import { normalizeRidForFusion, weightedAverageFusion } from "../adaptive-features";

test("normalizeRidForFusion strips chunk suffix", () => {
  expect(normalizeRidForFusion("orn:notion.page:regen/abc#chunk14")).toBe(
    "orn:notion.page:regen/abc"
  );
  expect(normalizeRidForFusion("orn:notion.page:regen/abc")).toBe(
    "orn:notion.page:regen/abc"
  );
});

test("weightedAverageFusion merges base doc and chunk rid", () => {
  const vectorResults = [
    { id: "doc:1", content: "", similarity: 0.2, source: "vector" as const }
  ];
  const entityResults = [
    {
      id: "doc:1#chunk14",
      content: "",
      similarity: 0.9,
      source: "sparql" as const,
      metadata: { entities_matched: ["Claims"] }
    }
  ];
  const keywordResults = [
    { id: "doc:1", content: "", similarity: 0.4, source: "keyword" as const }
  ];

  const results = weightedAverageFusion(vectorResults, entityResults, keywordResults);

  expect(results.length).toBe(1);
  expect(results[0].metadata.base_rid).toBe("doc:1");
  expect(results[0].metadata.keyword_score).toBe(0.4);
  expect(results[0].metadata.entity_score).toBe(0.9);
});
