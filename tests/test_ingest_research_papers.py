import importlib.util
import json
import sys
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ingest_research_papers.py"


def load_ingester():
    spec = importlib.util.spec_from_file_location("ingest_research_papers", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_authored_by_requires_entity_object():
    ingester = load_ingester()

    reason = ingester.invalid_fact_reason(
        "AUTHORED_BY",
        object_uri=None,
        object_literal="arXiv:2605.15778v1 [q-fin.MF] 15 May 2026",
    )

    assert reason == "AUTHORED_BY requires an entity object, not a literal"


def test_entity_object_predicate_requires_some_object():
    ingester = load_ingester()

    reason = ingester.invalid_fact_reason(
        "AUTHORED_BY",
        object_uri=None,
        object_literal=None,
    )

    assert reason == "AUTHORED_BY requires an entity object"


def test_literal_has_description_is_not_retired():
    ingester = load_ingester()

    assert ingester.invalid_fact_reason("HAS_DESCRIPTION", None, "A scientific concept.") is None


def test_self_relation_is_retired():
    ingester = load_ingester()

    reason = ingester.invalid_fact_reason(
        "GENERALIZES",
        object_uri="orn:personal-koi.entity:concept-tarski-fixed-point-theorem",
        object_literal=None,
        subject_uri="orn:personal-koi.entity:concept-tarski-fixed-point-theorem",
    )

    assert reason == "entity fact links an entity to itself"


def test_arxiv_header_description_is_retired():
    ingester = load_ingester()

    reason = ingester.invalid_fact_reason(
        "HAS_DESCRIPTION",
        object_uri=None,
        object_literal="arXiv:2501.03890v2 [math.CT] 22 Jan 2026",
    )

    assert reason == "bibliographic header should be document metadata, not a description fact"


def test_reversed_funding_support_is_retired():
    ingester = load_ingester()

    reason = ingester.invalid_fact_reason(
        "SUPPORTS",
        object_uri="orn:personal-koi.entity:org-darpa",
        object_literal=None,
        subject_type="Person",
        object_type="Organization",
    )

    assert reason == "person-to-organization SUPPORTS is likely a reversed funding acknowledgement"


def test_proves_generic_subject_needs_review():
    ingester = load_ingester()

    assert ingester.proves_predicate_needs_review("PROVES", "network sheaf")
    assert not ingester.proves_predicate_needs_review("PROVES", "Hodge-Lawvere Theorem")
    assert not ingester.proves_predicate_needs_review("RELATES_TO", "network sheaf")


def test_numbered_statement_subject_text_mismatch_needs_review():
    ingester = load_ingester()

    assert ingester.numbered_statement_needs_review(
        "Theorem 6.4",
        "Theorem 6.5 gives a deterministic sparsification algorithm.",
    )
    assert not ingester.numbered_statement_needs_review(
        "Proposition 8.3",
        "Proposition 8.3 characterizes 0-approximations to the constant sheaf.",
    )
    assert not ingester.numbered_statement_needs_review(
        "spectral sparsification",
        "Theorem 6.4 gives a sparsification result.",
    )
    assert ingester.numbered_statement_needs_review(
        "Theorem 8",
        "Lemma 11 is used in the proof of Theorem 7.",
    )
    assert ingester.numbered_statement_needs_review(
        "Theorem 1 (Network Preference Dynamics using Lattice Theory)",
        "Theorem 2 proves that stable preference profiles form a complete lattice.",
    )
    assert not ingester.numbered_statement_needs_review(
        "Theorem 2 (Network Preference Dynamics using Lattice Theory)",
        "Theorem 2 proves that stable preference profiles form a complete lattice.",
    )
    assert not ingester.numbered_statement_needs_review(
        "Lemma 11",
        "Lemma 11 is used in the proof of Theorem 7.",
    )


def test_algorithm_fact_owner_mismatch_needs_review():
    ingester = load_ingester()

    assert ingester.algorithm_fact_owner_needs_review(
        "distributor",
        "COMPUTES",
        "Algorithm 1 (Distributed Computation of Clearing Sections) computes clearing sections.",
    )
    assert ingester.algorithm_fact_owner_needs_review(
        "distributor",
        "HAS_BOUND",
        "For finite lattices, the distributed algorithm converges in O(sum h_v) iterations.",
    )
    assert not ingester.algorithm_fact_owner_needs_review(
        "Algorithm 1",
        "HAS_BOUND",
        "For finite lattices, the distributed algorithm converges in O(sum h_v) iterations.",
    )
    assert not ingester.algorithm_fact_owner_needs_review(
        "distributor",
        "PART_OF",
        "Each vertex carries a monotone distributor.",
    )


def test_kg_embedding_model_label_mismatch_needs_review():
    ingester = load_ingester()

    assert ingester.kg_embedding_model_fact_needs_review(
        "transformers",
        "knowledge graph embedding",
        "TransE is a translational knowledge graph embedding model subsumed within the sheaf framework.",
    )
    assert ingester.kg_embedding_model_fact_needs_review(
        "Transcript",
        "knowledge graph embedding",
        "TransR is a translational knowledge graph embedding model expressible within the sheaf framework.",
    )
    assert ingester.kg_embedding_model_fact_needs_review(
        "ExtensionSE model",
        None,
        "ExtensionTransE achieves an MRR of 0.340 on the pi easy query type on NELL-995.",
    )
    assert not ingester.kg_embedding_model_fact_needs_review(
        "TransE knowledge graph embedding model",
        "TransR knowledge graph embedding model",
        "TransE is recovered from TransR by taking the relation projection R_r to be the identity matrix.",
    )
    assert not ingester.kg_embedding_model_fact_needs_review(
        "Energy functional",
        "TransE knowledge graph embedding model",
        "The energy family includes TransE in real d-dimensional space as a special case.",
    )
    assert not ingester.kg_embedding_model_fact_needs_review(
        "Harmonic Extension",
        "NaiveTransE model",
        "Harmonic extension is compared against a naive TransE method for answering complex queries.",
    )
    assert not ingester.kg_embedding_model_fact_needs_review(
        "sheaf embedding",
        "RotatE knowledge graph embedding model",
        "RotatE scoring can be encoded by taking the head restriction map to be a diagonal matrix.",
    )


def test_scientific_label_overmerge_needs_review():
    ingester = load_ingester()

    assert ingester.scientific_label_overmerge_needs_review(
        "KnowledgeTab",
        "Cellular Sheaf",
        "A knowledge sheaf is a cellular sheaf on the directed multigraph schema Q.",
    )
    assert ingester.scientific_label_overmerge_needs_review(
        "knowledge graph embedding",
        "Knowledge Graph UI",
        "Knowledge graph embedding enables tasks such as knowledge graph completion.",
    )
    assert not ingester.scientific_label_overmerge_needs_review(
        "knowledge sheaf",
        "Cellular Sheaf",
        "A knowledge sheaf is a cellular sheaf on the directed multigraph schema Q.",
    )
    assert not ingester.scientific_label_overmerge_needs_review(
        "KnowledgeTab",
        "project workspace",
        "KnowledgeTab is a local interface for browsing knowledge.",
    )


def test_discourse_title_similarity_flags_near_duplicates():
    ingester = load_ingester()

    assert ingester.discourse_title_similarity(
        "Multi-valued clearing sections exist and form a complete lattice under an induced order.",
        "Multi-valued clearing sections exist and form a complete lattice, established via Zhou's fixed point theorem for correspondences.",
    ) >= 0.70
    assert ingester.discourse_title_similarity(
        "How can clearing sections be computed with privacy guarantees in distributed settings?",
        "Can clearing sections be computed with formal privacy guarantees beyond the basic locality property of Algorithm 1?",
    ) >= 0.70
    assert ingester.discourse_title_similarity(
        "The classical Eisenberg-Noe clearing condition is precisely recovered as a special case.",
        "Supply chain networks with manufacturing transformations fit the lattice liability network framework.",
    ) == 0.0


def test_discourse_title_contrast_prevents_false_duplicate():
    ingester = load_ingester()

    assert ingester.discourse_titles_are_contrastive(
        "The Figure 5 planar example confirms that positive cohomology correctly certifies existence of an evasion path.",
        "The Figure 6 planar example confirms that positive cohomology correctly certifies non-existence of an evasion path.",
    )
    assert not ingester.discourse_titles_are_contrastive(
        "How can clearing sections be computed with privacy guarantees in distributed settings?",
        "Can clearing sections be computed with formal privacy guarantees beyond the basic locality property of Algorithm 1?",
    )


def test_build_paper_ledger_entry_includes_source_routing_and_quality(tmp_path):
    ingester = load_ingester()
    paper_dir = tmp_path / "authors" / "test-author" / "2026-test-paper"
    paper_dir.mkdir(parents=True)
    (paper_dir / "metadata.yaml").write_text(
        yaml.safe_dump(
            {
                "paper_id": "test-author/2026-test-paper",
                "title": "A Test Paper",
                "year": 2026,
                "authors": ["Ada Lovelace"],
                "source_url": "https://arxiv.org/abs/2601.00001",
                "pdf_url": "https://arxiv.org/pdf/2601.00001",
                "arxiv_id": "2601.00001",
                "decision": "download_now",
                "relevance_score": 12,
                "matched_topics": ["sheaf"],
                "project_tags": ["sheaf-explorer"],
                "created": "2026-06-02",
                "pdf_status": "downloaded",
                "ingest_status": "deep_ingested",
                "extraction_profile": "scientific-discourse-v1",
                "deep_ingestion": {
                    "facts_count": 3,
                    "discourse_moves_count": 2,
                    "chunks_count": 1,
                    "updated_at": "2026-06-02T00:00:00+00:00",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (paper_dir / "extracted.md").write_text("# A Test Paper\n\nScientific text.", encoding="utf-8")
    (paper_dir / "quality-review.json").write_text(
        json.dumps({"verdict": "ok", "warnings": [], "facts": {"count": 3}, "discourse": {"count": 2}}),
        encoding="utf-8",
    )

    entry = ingester.build_paper_ledger_entry(paper_dir / "metadata.yaml", tmp_path, "test-author")

    assert entry["schema"] == "personal-koi-paper-ledger-v1"
    assert entry["paper_id"] == "test-author/2026-test-paper"
    assert entry["source"]["source_tier"] == "scholarly_preprint"
    assert entry["routing"]["project_tags"] == ["sheaf-explorer"]
    assert entry["koi"]["facts_count"] == 3
    assert entry["koi"]["quality_verdict"] == "ok"
    assert entry["local"]["extracted_path"] == "authors/test-author/2026-test-paper/extracted.md"
    assert entry["local"]["extracted_word_count"] == 5
    assert entry["local"]["extracted_text_quality"] == "ok"


def test_build_paper_ledger_entry_uses_rag_ingestion_chunks(tmp_path):
    ingester = load_ingester()
    paper_dir = tmp_path / "authors" / "test-author" / "2026-rag-paper"
    paper_dir.mkdir(parents=True)
    (paper_dir / "metadata.yaml").write_text(
        yaml.safe_dump(
            {
                "paper_id": "test-author/2026-rag-paper",
                "title": "A RAG Paper",
                "year": 2026,
                "authors": ["Ada Lovelace"],
                "source_url": "https://arxiv.org/abs/2601.00002",
                "decision": "review_then_download",
                "relevance_score": 8,
                "matched_topics": ["sheaf"],
                "pdf_status": "downloaded",
                "ingest_status": "rag_ingested",
                "rag_ingestion": {
                    "chunks_count": 51,
                    "group_id": "sheaf-explorer",
                    "updated_at": "2026-06-03T00:00:00+00:00",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (paper_dir / "extracted.md").write_text("# A RAG Paper\n\nScientific text.", encoding="utf-8")

    entry = ingester.build_paper_ledger_entry(paper_dir / "metadata.yaml", tmp_path, "test-author")

    assert entry["koi"]["ingest_status"] == "rag_ingested"
    assert entry["koi"]["chunks_count"] == 51
    assert entry["koi"]["last_processed_at"] == "2026-06-03T00:00:00+00:00"


def test_load_candidates_explicit_paper_id_bypasses_default_decision_filter(tmp_path):
    ingester = load_ingester()
    paper_dir = tmp_path / "authors" / "test-author" / "2022-review-paper"
    paper_dir.mkdir(parents=True)
    (paper_dir / "metadata.yaml").write_text(
        yaml.safe_dump(
            {
                "paper_id": "test-author/2022-review-paper",
                "title": "A Review Candidate",
                "year": 2022,
                "authors": ["Ada Lovelace"],
                "source_url": "https://arxiv.org/abs/2201.00001",
                "decision": "review_then_download",
                "relevance_score": 5,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (paper_dir / "extracted.md").write_text("# A Review Candidate\n\nScientific text.", encoding="utf-8")

    candidates = ingester.load_candidates(
        tmp_path,
        "test-author",
        decisions={"download_now"},
        paper_ids={"test-author/2022-review-paper"},
        min_score=None,
        require_extracted=True,
    )

    assert [p.paper_id for p in candidates] == ["test-author/2022-review-paper"]


def test_write_author_ledgers_writes_jsonl_and_source_yaml(tmp_path):
    ingester = load_ingester()
    paper_dir = tmp_path / "authors" / "test-author" / "2026-paywalled-paper"
    paper_dir.mkdir(parents=True)
    (paper_dir / "metadata.yaml").write_text(
        yaml.safe_dump(
            {
                "paper_id": "test-author/2026-paywalled-paper",
                "title": "A Paywalled Paper",
                "year": 2026,
                "authors": ["Ada Lovelace"],
                "source_url": "https://doi.org/10.1234/example",
                "decision": "download_now",
                "relevance_score": 7,
                "paywalled": True,
                "pdf_status": "paywalled_metadata_only",
                "ingest_status": "metadata_only",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    summary = ingester.write_author_ledgers(tmp_path, "test-author")
    paper_lines = (paper_dir.parent / "paper-ledger.jsonl").read_text(encoding="utf-8").splitlines()
    source_ledger = yaml.safe_load((paper_dir.parent / "source-ledger.yaml").read_text(encoding="utf-8"))

    assert summary["papers"] == 1
    assert len(paper_lines) == 1
    assert json.loads(paper_lines[0])["source"]["source_tier"] == "paywalled_metadata"
    assert source_ledger["schema"] == "personal-koi-paper-source-ledger-v1"
    assert source_ledger["sources"][0]["paper_id"] == "test-author/2026-paywalled-paper"
    assert source_ledger["issue_log_path"] == "ingestion-issue-log.jsonl"


def test_write_backtest_report_summarizes_quality_and_issues(tmp_path):
    ingester = load_ingester()
    author_dir = tmp_path / "authors" / "test-author"
    paper_dir = author_dir / "2026-test-paper"
    paper_dir.mkdir(parents=True)
    (paper_dir / "metadata.yaml").write_text(
        yaml.safe_dump(
            {
                "paper_id": "test-author/2026-test-paper",
                "title": "A Test Paper",
                "year": 2026,
                "authors": ["Ada Lovelace"],
                "source_url": "https://arxiv.org/abs/2601.00001",
                "decision": "download_now",
                "ingest_status": "deep_ingested",
                "deep_ingestion": {"facts_count": 5, "discourse_moves_count": 3, "chunks_count": 2},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (paper_dir / "extracted.md").write_text("# A Test Paper\n\nScientific text.", encoding="utf-8")
    (paper_dir / "quality-review.json").write_text(
        json.dumps(
            {
                "verdict": "needs_review",
                "warnings": ["1 near-duplicate discourse move pair(s) need merge review"],
                "facts": {"count": 5, "proves_generic_subject": 0},
                "discourse": {"count": 3, "near_duplicate_move_pairs": [{"left_id": "a", "right_id": "b"}]},
            }
        ),
        encoding="utf-8",
    )
    issue = {
        "paper_id": "test-author/2026-test-paper",
        "document_rid": "document:test",
        "category": "quality_warning",
        "code": "near_duplicate_discourse",
        "severity": "review",
        "status": "open",
        "detail": "Needs merge review.",
    }
    ingester.append_issue_log(tmp_path, "test-author", [issue])

    result = ingester.write_backtest_report(tmp_path, "test-author")
    report = json.loads(Path(result["path"]).read_text(encoding="utf-8"))

    assert report["schema"] == "personal-koi-paper-backtest-v1"
    assert report["papers_with_quality_reviews"] == 1
    assert report["quality_verdicts"] == {"needs_review": 1}
    assert report["totals"]["facts"] == 5
    assert report["review_candidates"][0]["near_duplicate_move_pairs"] == 1
    assert report["open_issues"][0]["code"] == "near_duplicate_discourse"
    assert "Review and merge near-duplicate discourse moves before synthesizing across papers." in report["recommendations"]


def test_promotion_candidate_scores_unreviewed_ready_papers():
    ingester = load_ingester()
    entry = {
        "paper_id": "test-author/2026-discourse-sheaf-paper",
        "title": "Discourse Sheaves and Belief Communication",
        "year": 2026,
        "source": {"source_tier": "scholarly_preprint", "paywalled": False},
        "routing": {
            "decision": "download_now",
            "relevance_score": 12,
            "matched_topics": ["sheaf", "discourse", "belief", "communication"],
            "project_tags": ["sheaf-explorer", "spore"],
        },
        "local": {
            "pdf_path": "authors/test/source.pdf",
            "extracted_path": "authors/test/extracted.md",
            "extracted_word_count": ingester.MIN_EXTRACTED_WORDS_FOR_TEXT_READY,
        },
        "koi": {"quality_verdict": "", "pdf_status": "downloaded", "chunks_count": 5},
    }

    candidate = ingester.promotion_candidate(entry)

    assert candidate["paper_id"] == "test-author/2026-discourse-sheaf-paper"
    assert candidate["recommended_level"] == "deep_ingest_reviewed"
    assert candidate["text_ready"] is True
    assert candidate["rag_ready"] is True
    assert any(reason.startswith("topics:") for reason in candidate["reasons"])
    assert candidate["promotion_score"] >= ingester.DEEP_PROMOTION_THRESHOLD


def test_promotion_candidate_distinguishes_local_text_from_rag_ready():
    ingester = load_ingester()
    entry = {
        "paper_id": "test-author/2026-local-only",
        "title": "Local Text But Not RAG Ingested",
        "year": 2026,
        "source": {"source_tier": "scholarly_preprint", "paywalled": False},
        "routing": {
            "decision": "download_now",
            "relevance_score": 12,
            "matched_topics": ["sheaf", "discourse", "belief", "communication"],
            "project_tags": ["sheaf-explorer", "spore"],
        },
        "local": {
            "pdf_path": "authors/test/source.pdf",
            "extracted_path": "authors/test/extracted.md",
            "extracted_word_count": ingester.MIN_EXTRACTED_WORDS_FOR_TEXT_READY,
        },
        "koi": {"quality_verdict": "", "pdf_status": "downloaded", "chunks_count": 0},
    }

    candidate = ingester.promotion_candidate(entry)

    assert candidate["text_ready"] is True
    assert candidate["rag_ready"] is False
    assert candidate["recommended_level"] == "rag_then_deep_ingest_reviewed"


def test_promotion_candidate_prioritizes_participatory_mapping_disagreement_arc():
    ingester = load_ingester()
    entry = {
        "paper_id": "test-author/2024-participatory-seams",
        "title": "Participatory Mapping of Irreducible Disagreement",
        "year": 2024,
        "source": {"source_tier": "scholarly_preprint", "paywalled": False},
        "routing": {
            "decision": "review_then_download",
            "relevance_score": 6,
            "matched_topics": [
                "participatory mapping",
                "stakeholder conflict",
                "deep disagreement",
                "heterogeneous lens",
                "seams",
            ],
            "project_tags": ["spore", "personal-knowledge-graph"],
        },
        "local": {
            "pdf_path": "authors/test/source.pdf",
            "extracted_path": "authors/test/extracted.md",
            "extracted_word_count": ingester.MIN_EXTRACTED_WORDS_FOR_TEXT_READY,
        },
        "koi": {"quality_verdict": "", "pdf_status": "downloaded", "chunks_count": 0},
    }

    candidate = ingester.promotion_candidate(entry)

    assert candidate["recommended_level"] == "rag_then_deep_ingest_reviewed"
    assert candidate["promotion_score"] >= ingester.DEEP_PROMOTION_THRESHOLD
    assert any("participatory mapping" in reason for reason in candidate["reasons"])


def test_promotion_candidate_treats_hollow_extraction_as_not_text_ready():
    ingester = load_ingester()
    entry = {
        "paper_id": "test-author/2010-scanned-notes",
        "title": "Scanned Notes",
        "year": 2010,
        "source": {"source_tier": "author_homepage", "paywalled": False},
        "routing": {
            "decision": "review_then_download",
            "relevance_score": 8,
            "matched_topics": ["sensor", "network"],
            "project_tags": ["sheaf-explorer", "spore"],
        },
        "local": {
            "pdf_path": "authors/test/source.pdf",
            "extracted_path": "authors/test/extracted.md",
            "extracted_word_count": 31,
        },
        "koi": {"quality_verdict": "", "pdf_status": "downloaded", "chunks_count": 0},
    }

    candidate = ingester.promotion_candidate(entry)

    assert candidate["text_ready"] is False
    assert candidate["rag_ready"] is False
    assert candidate["recommended_level"] == "light_ingest"
    assert "extracted_text_short:31 (<500)" in candidate["reasons"]


def test_extracted_text_metrics_flags_mojibake(tmp_path):
    ingester = load_ingester()
    extracted = tmp_path / "extracted.md"
    bad_line = "\x02\x01\x04\x03 km~vY:c\\lIfvzzY:lI_bkzfvc " * 120
    extracted.write_text("# Bad PDF\n\n" + bad_line, encoding="utf-8")

    metrics = ingester.extracted_text_metrics(extracted)

    assert metrics["word_count"] >= ingester.MIN_EXTRACTED_WORDS_FOR_TEXT_READY
    assert metrics["quality"] == "mojibake_suspected"


def test_promotion_candidate_treats_mojibake_extraction_as_not_text_ready():
    ingester = load_ingester()
    entry = {
        "paper_id": "test-author/2004-type3-paper",
        "title": "Type 3 Paper",
        "year": 2004,
        "source": {"source_tier": "author_homepage", "paywalled": False},
        "routing": {
            "decision": "review_then_download",
            "relevance_score": 8,
            "matched_topics": ["coordination", "robot"],
            "project_tags": ["sheaf-explorer", "spore"],
        },
        "local": {
            "pdf_path": "authors/test/source.pdf",
            "extracted_path": "authors/test/extracted.md",
            "extracted_word_count": 2000,
            "extracted_text_quality": "mojibake_suspected",
        },
        "koi": {"quality_verdict": "", "pdf_status": "downloaded", "chunks_count": 0},
    }

    candidate = ingester.promotion_candidate(entry)

    assert candidate["text_ready"] is False
    assert candidate["recommended_level"] == "light_ingest"
    assert "extracted_text_quality:mojibake_suspected" in candidate["reasons"]


def test_promotion_candidate_excludes_ocr_blocked_papers():
    ingester = load_ingester()

    assert (
        ingester.promotion_candidate(
            {
                "paper_id": "test-author/2010-scanned-notes",
                "title": "Scanned Notes",
                "year": 2010,
                "source": {"source_tier": "author_homepage", "paywalled": False},
                "routing": {"decision": "review_then_download", "relevance_score": 8, "matched_topics": ["sensor"]},
                "local": {"pdf_path": "authors/test/source.pdf", "extracted_path": "authors/test/extracted.md"},
                "koi": {"quality_verdict": "", "pdf_status": "downloaded", "chunks_count": 0, "ingest_status": "ocr_blocked"},
            }
        )
        is None
    )


def test_promotion_candidate_excludes_reviewed_papers():
    ingester = load_ingester()

    assert (
        ingester.promotion_candidate(
            {
                "paper_id": "test-author/2026-reviewed",
                "title": "Reviewed Paper",
                "year": 2026,
                "source": {"source_tier": "scholarly_preprint"},
                "routing": {"decision": "download_now", "relevance_score": 99, "matched_topics": ["sheaf"]},
                "local": {"extracted_path": "authors/test/extracted.md"},
                "koi": {"quality_verdict": "ok"},
            }
        )
        is None
    )


def test_build_promotion_candidates_orders_by_score():
    ingester = load_ingester()
    low_value = {
        "paper_id": "test-author/2020-background",
        "title": "Background Geometry",
        "year": 2020,
        "source": {"source_tier": "scholarly_preprint"},
        "routing": {"decision": "maybe", "relevance_score": 1, "matched_topics": ["geometry"]},
        "local": {},
        "koi": {"quality_verdict": ""},
    }
    high_value = {
        "paper_id": "test-author/2025-network-sheaf",
        "title": "Network Sheaves for Communication",
        "year": 2025,
        "source": {"source_tier": "author_homepage"},
        "routing": {
            "decision": "download_now",
            "relevance_score": 10,
            "matched_topics": ["sheaves", "communication", "network"],
        },
        "local": {"pdf_path": "authors/test/source.pdf"},
        "koi": {"quality_verdict": ""},
    }

    candidates = ingester.build_promotion_candidates([low_value, high_value])

    assert [candidate["paper_id"] for candidate in candidates] == [
        "test-author/2025-network-sheaf",
        "test-author/2020-background",
    ]
    assert candidates[0]["promotion_score"] > candidates[1]["promotion_score"]


def test_append_issue_log_dedupes_and_summarizes(tmp_path):
    ingester = load_ingester()

    issue = {
        "paper_id": "test-author/2026-test-paper",
        "document_rid": "document:test",
        "category": "entity_resolution",
        "code": "wrong_same_type_concept_resolution",
        "severity": "medium",
        "status": "fixed",
        "detail": "constant sheaf resolved to ConstraintSet",
    }

    first = ingester.append_issue_log(tmp_path, "test-author", [issue])
    second = ingester.append_issue_log(tmp_path, "test-author", [issue])
    path = tmp_path / "authors" / "test-author" / "ingestion-issue-log.jsonl"
    lines = path.read_text(encoding="utf-8").splitlines()
    summary = ingester.summarize_issue_log(path)

    assert first["appended"] == 1
    assert second["appended"] == 0
    assert len(lines) == 1
    assert json.loads(lines[0])["schema"] == "personal-koi-paper-ingestion-issue-v1"
    assert summary["total"] == 1
    assert summary["by_category"] == {"entity_resolution": 1}
    assert summary["by_status"] == {"fixed": 1}


def test_retired_invalid_fact_issue_id_ignores_transient_fact_id(tmp_path):
    ingester = load_ingester()

    base_issue = {
        "paper_id": "test-author/2026-test-paper",
        "document_rid": "document:test",
        "category": "fact_validation",
        "code": "retired_invalid_fact",
        "severity": "low",
        "status": "fixed",
        "detail": "entity fact links an entity to itself",
        "predicate": "EQUIVALENT_TO",
        "fact_text": "Lemma 4.9 proves that L_Z is precisely M_Z.",
    }

    first = ingester.append_issue_log(tmp_path, "test-author", [{**base_issue, "fact_id": "fact-1"}])
    second = ingester.append_issue_log(tmp_path, "test-author", [{**base_issue, "fact_id": "fact-2"}])
    path = tmp_path / "authors" / "test-author" / "ingestion-issue-log.jsonl"
    lines = path.read_text(encoding="utf-8").splitlines()

    assert first["appended"] == 1
    assert second["appended"] == 0
    assert len(lines) == 1


def test_append_issue_log_dedupes_legacy_retired_invalid_fact_ids(tmp_path):
    ingester = load_ingester()
    path = tmp_path / "authors" / "test-author" / "ingestion-issue-log.jsonl"
    path.parent.mkdir(parents=True)
    legacy_issue = {
        "issue_id": "legacy-fact-specific-id",
        "paper_id": "test-author/2026-test-paper",
        "document_rid": "document:test",
        "category": "fact_validation",
        "code": "retired_invalid_fact",
        "severity": "low",
        "status": "fixed",
        "detail": "entity fact links an entity to itself",
        "predicate": "EQUIVALENT_TO",
        "fact_id": "fact-1",
        "fact_text": "Lemma 4.9 proves that L_Z is precisely M_Z.",
    }
    path.write_text(json.dumps(legacy_issue, sort_keys=True) + "\n", encoding="utf-8")

    result = ingester.append_issue_log(
        tmp_path,
        "test-author",
        [{**legacy_issue, "issue_id": None, "fact_id": "fact-2"}],
    )
    lines = path.read_text(encoding="utf-8").splitlines()

    assert result["appended"] == 0
    assert len(lines) == 1


def test_issues_from_processing_maps_warnings_and_retired_facts():
    ingester = load_ingester()
    paper = ingester.PaperCandidate(
        path=Path("/tmp/paper"),
        metadata_path=Path("/tmp/paper/metadata.yaml"),
        metadata={},
        paper_id="test-author/2026-test-paper",
        title="A Test Paper",
        year=2026,
        decision="download_now",
        relevance_score=10,
        source_url="https://example.test/paper",
        extracted_path=Path("/tmp/paper/extracted.md"),
        document_rid="document:test",
    )

    issues = ingester.issues_from_processing(
        paper,
        {
            "verdict": "needs_review",
            "warnings": ["1 PROVES fact(s) have generic concept subjects; review predicate ownership"],
            "type_mismatches": [{"payload": {"entity_text": "Flocking"}}],
        },
        [
            {
                "id": "fact-1",
                "predicate": "GENERALIZES",
                "fact_text": "A self relation.",
                "reason": "entity fact links an entity to itself",
            }
        ],
    )

    assert [issue["category"] for issue in issues] == [
        "quality_warning",
        "entity_resolution",
        "fact_validation",
    ]
    assert issues[0]["status"] == "open"
    assert issues[0]["code"].startswith("1_proves_fact_s_have_generic_concept_subjects")
    assert issues[2]["status"] == "fixed"
    assert issues[2]["fact_id"] == "fact-1"
