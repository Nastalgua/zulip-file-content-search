"""
Performance stress tests for the GIN index on AttachmentContent.search_tsvector.

These tests measure query time with and without the GIN index at increasing scales,
and assert that the index provides a meaningful speedup. They are skipped by default
to avoid slowing down the standard test suite.

Run with:
    RUN_PERF_TESTS=1 tools/test-backend zerver.tests.test_file_content_performance
"""

import os
import random
import time
from unittest import skipUnless

from django.db import connection
from django.utils.timezone import now as timezone_now

from zerver.lib.test_classes import ZulipTestCase
from zerver.models import Attachment, AttachmentContent

# Search term used across all tests. Appears in ~10% of generated documents.
SEARCH_TERM = "quarterly"

# Scales at which performance is measured, in increasing order.
TEST_SIZES = [100, 500, 100000]

# Vocabulary used to generate realistic filler text around the search term.
WORD_POOL = [
    "the", "project", "team", "meeting", "review", "document", "report",
    "analysis", "summary", "update", "status", "progress", "deadline",
    "budget", "planning", "schedule", "milestone", "deliverable", "scope",
    "design", "implementation", "testing", "deployment", "architecture",
    "performance", "security", "database", "backend", "frontend", "sprint",
    "backlog", "feature", "release", "integration", "pipeline", "workflow",
]

# Minimum speedup the GIN index must provide at the largest test scale.
MIN_EXPECTED_SPEEDUP = 1.1


@skipUnless(os.environ.get("RUN_PERF_TESTS"), "Performance tests skipped by default. Set RUN_PERF_TESTS=1 to run.")
class FileContentIndexPerformanceTest(ZulipTestCase):
    """
    Stress tests for the GIN index on AttachmentContent.search_tsvector.

    Three test methods:
      - test_search_performance_with_index: verifies the planner uses the GIN
        index and prints timing at each scale.
      - test_search_performance_without_index: disables index scans at the
        session level, verifies the tsvector match is no longer an index
        condition, and prints timing at each scale as a baseline.
      - test_index_speedup_comparison: side-by-side comparison at the largest
        scale; asserts the indexed query is at least MIN_EXPECTED_SPEEDUP faster.
    """

    # -------------------------------------------------------------------------
    # Helpers: data generation
    # -------------------------------------------------------------------------

    def _generate_text(self, include_term: bool) -> str:
        """Return ~80 words of filler text, optionally containing SEARCH_TERM."""
        words = random.choices(WORD_POOL, k=80)
        text = " ".join(words)
        if include_term:
            # Insert the search term near the start so it is reliably indexed.
            text = f"{SEARCH_TERM} {text}"
        return text

    def _bulk_create_attachments(self, n: int, tag: str = "") -> tuple[list[int], list[int]]:
        """
        Create n Attachment rows and n corresponding AttachmentContent rows.

        Exactly n // 10 of the content rows contain SEARCH_TERM.
        The search_tsvector is populated via a single raw SQL UPDATE using
        to_tsvector('zulip.english_us_search', ...) to match the config used
        in by_file_content.

        Returns (attachment_ids, content_ids).
        """
        user = self.example_user("hamlet")
        realm = user.realm

        # Use tag + index for unique path_ids within a test run.
        prefix = f"perf_{tag}_{n}" if tag else f"perf_{n}"

        attachments = Attachment.objects.bulk_create([
            Attachment(
                file_name=f"{prefix}_{i}.txt",
                path_id=f"{prefix}/{i}/file.txt",
                owner=user,
                realm=realm,
                size=1024,
                content_type="text/plain",
                create_time=timezone_now(),
            )
            for i in range(n)
        ])

        # 10% of rows contain the search term.
        contents = AttachmentContent.objects.bulk_create([
            AttachmentContent(
                attachment=attachment,
                extraction_status=AttachmentContent.ExtractionStatus.SUCCESS,
                extracted_text=self._generate_text(include_term=(i % 10 == 0)),
            )
            for i, attachment in enumerate(attachments)
        ])

        # Populate search_tsvector using the same config as by_file_content.
        content_ids = [c.id for c in contents]
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE zerver_attachmentcontent
                SET search_tsvector = to_tsvector('zulip.english_us_search', extracted_text)
                WHERE id = ANY(%s)
                  AND extracted_text IS NOT NULL
                """,
                [content_ids],
            )

        attachment_ids = [a.id for a in attachments]
        return attachment_ids, content_ids

    def _cleanup(self, attachment_ids: list[int]) -> None:
        """
        Delete Attachment rows. AttachmentContent rows are removed via CASCADE.
        """
        Attachment.objects.filter(id__in=attachment_ids).delete()

    # -------------------------------------------------------------------------
    # Helpers: query execution
    # -------------------------------------------------------------------------

    def _run_search(self, term: str) -> tuple[int, float]:
        """
        Run the tsvector search against zerver_attachmentcontent.
        Mirrors the SQL produced by by_file_content in narrow.py.

        Returns (match_count, elapsed_seconds).
        """
        start = time.perf_counter()
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM zerver_attachmentcontent
                WHERE extraction_status = 2
                  AND search_tsvector @@ plainto_tsquery('zulip.english_us_search', %s)
                """,
                [term],
            )
            count = cursor.fetchone()[0]
        elapsed = time.perf_counter() - start
        return count, elapsed

    def _get_query_plan(self, term: str) -> str:
        """
        Return the EXPLAIN ANALYZE output as a string.
        Used to assert whether the planner used the GIN index.
        """
        with connection.cursor() as cursor:
            cursor.execute(
                """
                EXPLAIN (ANALYZE, FORMAT TEXT)
                SELECT COUNT(*)
                FROM zerver_attachmentcontent
                WHERE extraction_status = 2
                  AND search_tsvector @@ plainto_tsquery('zulip.english_us_search', %s)
                """,
                [term],
            )
            rows = cursor.fetchall()
        return "\n".join(row[0] for row in rows)

    # -------------------------------------------------------------------------
    # Helpers: planner settings
    # -------------------------------------------------------------------------

    def _disable_index_scans(self) -> None:
        """
        Tell the PostgreSQL planner to avoid index and bitmap scans for this
        session. This forces the tsvector match to run as a sequential filter
        rather than routing through the GIN index, giving us a no-index
        baseline without any DDL.

        Using session-level settings instead of DROP/CREATE INDEX avoids the
        'pending trigger events' error that arises when DDL is attempted inside
        Django's TestCase transaction wrapper.
        """
        with connection.cursor() as cursor:
            cursor.execute("SET enable_indexscan = OFF")
            cursor.execute("SET enable_bitmapscan = OFF")

    def _enable_index_scans(self) -> None:
        """Restore the planner's default index scan behaviour."""
        with connection.cursor() as cursor:
            cursor.execute("SET enable_indexscan = ON")
            cursor.execute("SET enable_bitmapscan = ON")

    # -------------------------------------------------------------------------
    # Tests
    # -------------------------------------------------------------------------

    def test_search_performance_with_index(self) -> None:
        """
        Verify the GIN index is used and print query time at each scale.

        Asserts:
          - The query planner uses a Bitmap Index Scan (GIN index path).
        """
        print("\n--- GIN index performance ---")
        for n in TEST_SIZES:
            with self.subTest(n=n):
                attachment_ids, _ = self._bulk_create_attachments(n, tag="with")
                try:
                    count, elapsed = self._run_search(SEARCH_TERM)
                    plan = self._get_query_plan(SEARCH_TERM)

                    print(f"  n={n:>5}: {count} matches in {elapsed:.4f}s")

                    # The GIN index causes PostgreSQL to use a Bitmap Index Scan.
                    self.assertIn(
                        "Bitmap Index Scan",
                        plan,
                        f"Expected GIN index (Bitmap Index Scan) to be used at n={n}.\nPlan:\n{plan}",
                    )
                finally:
                    self._cleanup(attachment_ids)

    def test_search_performance_without_index(self) -> None:
        """
        Disable index scans at the session level and print timing at each scale
        as a no-index baseline.

        Asserts that the GIN index name no longer appears in the query plan,
        confirming the planner is not routing through it.

        Index scans are always re-enabled in a finally block.
        """
        self._disable_index_scans()
        try:
            print("\n--- Sequential scan performance (no index) ---")
            for n in TEST_SIZES:
                with self.subTest(n=n):
                    attachment_ids, _ = self._bulk_create_attachments(n, tag="without")
                    try:
                        count, elapsed = self._run_search(SEARCH_TERM)
                        plan = self._get_query_plan(SEARCH_TERM)

                        print(f"  n={n:>5}: {count} matches in {elapsed:.4f}s")

                        # With index scans disabled, the GIN index name should
                        # not appear anywhere in the plan.
                        self.assertNotIn(
                            "attachment_content_search_tsvector",
                            plan,
                            f"GIN index should not be used when index scans are disabled at n={n}.\nPlan:\n{plan}",
                        )
                    finally:
                        self._cleanup(attachment_ids)
        finally:
            self._enable_index_scans()

    def test_index_speedup_comparison(self) -> None:
        """
        Side-by-side comparison at the largest scale (TEST_SIZES[-1]).

        Creates data once, measures time with index scans enabled, then
        disables them and measures again with the same data.

        Asserts the indexed query is at least MIN_EXPECTED_SPEEDUP times faster.
        """
        n = TEST_SIZES[-1]
        attachment_ids, _ = self._bulk_create_attachments(n, tag="cmp")
        try:
            # Warm up the connection so first-call overhead doesn't skew results.
            self._run_search(SEARCH_TERM)

            _, time_with_index = self._run_search(SEARCH_TERM)

            self._disable_index_scans()
            try:
                _, time_without_index = self._run_search(SEARCH_TERM)
            finally:
                self._enable_index_scans()

            speedup = time_without_index / time_with_index if time_with_index > 0 else float("inf")
            print(
                f"\n--- Speedup comparison at n={n} ---\n"
                f"  with index:    {time_with_index:.4f}s\n"
                f"  without index: {time_without_index:.4f}s\n"
                f"  speedup:       {speedup:.1f}x"
            )

            self.assertGreater(
                speedup,
                MIN_EXPECTED_SPEEDUP,
                f"GIN index should be at least {MIN_EXPECTED_SPEEDUP}x faster at n={n}. "
                f"Got {speedup:.1f}x (with={time_with_index:.4f}s, without={time_without_index:.4f}s).",
            )
        finally:
            self._cleanup(attachment_ids)