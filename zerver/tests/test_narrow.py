from django.db import connection

from zerver.models import Attachment, Message
from zerver.models.messages import AttachmentContent
from zerver.lib.test_classes import ZulipTestCase
from zerver.lib.narrow import (
    NarrowParameter,
    add_narrow_conditions,
    get_base_query_for_search,
)
from zerver.lib.sqlalchemy_utils import get_sqlalchemy_connection

from sqlalchemy.sql import column, select, table
from sqlalchemy.types import Integer


class FileContentNarrowTest(ZulipTestCase):
    """
    Integration tests for the file-content narrow operator.
    """

    def setUp(self) -> None:
        super().setUp()
        self.user_profile = self.example_user("hamlet")
        self.realm = self.user_profile.realm

    def _create_attachment(
        self,
        message_id: int,
        filename: str,
        path_id: str,
    ) -> Attachment:
        """Create an Attachment and link it to a message"""
        attachment = Attachment.objects.create(
            file_name=filename,
            path_id=path_id,
            owner=self.user_profile,
            realm=self.realm,
            size=1024,
            content_type="application/pdf",
        )
        message = Message.objects.get(id=message_id)
        attachment.messages.add(message)
        return attachment

    def _create_attachment_content(
        self,
        attachment: Attachment,
        text: str | None,
        status: int = AttachmentContent.ExtractionStatus.SUCCESS,
    ) -> AttachmentContent:
        """
        Create an AttachmentContent row. If text is provided and status is SUCCESS,
        populate the search_tsvector via raw SQL since Django's ORM does not support
        writing tsvector values directly.
        """
        content = AttachmentContent.objects.create(
            attachment=attachment,
            extracted_text=text,
            extraction_status=status,
        )
        if text is not None and status == AttachmentContent.ExtractionStatus.SUCCESS:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE zerver_attachmentcontent
                    SET search_tsvector = to_tsvector('zulip.english_us_search', %s)
                    WHERE id = %s
                    """,
                    [text, content.id],
                )
        return content

    def _get_message_ids_for_narrow(
        self,
        operand: str,
        negated: bool = False,
    ) -> list[int]:
        """
        Execute a file-content narrow and return the matching message IDs.
        Uses the same query pipeline as the real message fetch endpoint.
        """
        query, inner_msg_id_col = get_base_query_for_search(
            realm_id=self.realm.id,
            user_profile=self.user_profile,
            need_user_message=True,
        )
        narrow = [
            NarrowParameter(
                operator="file-content",
                operand=operand,
                negated=negated,
            )
        ]
        query, _, _ = add_narrow_conditions(
            user_profile=self.user_profile,
            inner_msg_id_col=inner_msg_id_col,
            query=query,
            narrow=narrow,
            is_web_public_query=False,
            realm=self.realm,
        )
        with get_sqlalchemy_connection() as sa_conn:
            rows = sa_conn.execute(query).fetchall()
        return [row[0] for row in rows]

    def test_returns_only_matching_message(self) -> None:
        """
        Core case: only the message whose attachment contains the search
        term should be returned. Three other messages should be excluded:
        one with non-matching content, one with a pending extraction, and
        one with no attachment at all.
        """
        # attachment containing the target term
        msg1_id = self.send_stream_message(self.user_profile, "Denmark", "has matching attachment")
        attachment1 = self._create_attachment(msg1_id, "report.pdf", "a/b/c/report.pdf")
        self._create_attachment_content(attachment1, "This is our quarterly financial report")

        # attachment with unrelated content
        msg2_id = self.send_stream_message(self.user_profile, "Denmark", "unrelated attachment")
        attachment2 = self._create_attachment(msg2_id, "notes.pdf", "a/b/c/notes.pdf")
        self._create_attachment_content(attachment2, "Meeting notes about project milestones")

        # attachment that has not yet been extracted (PENDING)
        msg3_id = self.send_stream_message(self.user_profile, "Denmark", "pending attachment")
        attachment3 = self._create_attachment(msg3_id, "pending.pdf", "a/b/c/pending.pdf")
        self._create_attachment_content(
            attachment3,
            text=None,
            status=AttachmentContent.ExtractionStatus.PENDING,
        )

        # no attachment
        msg4_id = self.send_stream_message(self.user_profile, "Denmark", "no attachment here")

        result_ids = self._get_message_ids_for_narrow("quarterly")

        self.assertIn(msg1_id, result_ids)
        self.assertNotIn(msg2_id, result_ids)
        self.assertNotIn(msg3_id, result_ids)
        self.assertNotIn(msg4_id, result_ids)

    def test_stemming(self) -> None:
        """
        PostgreSQL search stems tokens, so searching "report"
        should match a document containing "reports".
        """
        msg_id = self.send_stream_message(self.user_profile, "Denmark", "stemming test")
        attachment = self._create_attachment(msg_id, "stem.pdf", "a/b/c/stem.pdf")
        self._create_attachment_content(attachment, "The quarterly reports are enclosed")

        result_ids = self._get_message_ids_for_narrow("report")
        self.assertIn(msg_id, result_ids)

    def test_multi_word_query(self) -> None:
        """
        A multi-word operand should require all words to be present
        (plainto_tsquery joins terms with AND).
        """
        msg_match_id = self.send_stream_message(self.user_profile, "Denmark", "both words")
        attachment_match = self._create_attachment(msg_match_id, "both.pdf", "a/b/c/both.pdf")
        self._create_attachment_content(attachment_match, "quarterly financial report for review")

        msg_partial_id = self.send_stream_message(self.user_profile, "Denmark", "one word only")
        attachment_partial = self._create_attachment(
            msg_partial_id, "partial.pdf", "a/b/c/partial.pdf"
        )
        self._create_attachment_content(attachment_partial, "quarterly summary only")

        # Both "quarterly" and "report" must be present
        result_ids = self._get_message_ids_for_narrow("quarterly report")

        self.assertIn(msg_match_id, result_ids)
        self.assertNotIn(msg_partial_id, result_ids)

    def test_negation(self) -> None:
        """
        A negated file-content term (-file-content:quarterly) should
        exclude messages whose attachments match and include those that don't.
        """
        msg_matching_id = self.send_stream_message(
            self.user_profile, "Denmark", "has matching attachment"
        )
        attachment1 = self._create_attachment(
            msg_matching_id, "match.pdf", "a/b/c/match.pdf"
        )
        self._create_attachment_content(attachment1, "quarterly financial report")

        msg_other_id = self.send_stream_message(
            self.user_profile, "Denmark", "has non-matching attachment"
        )
        attachment2 = self._create_attachment(msg_other_id, "other.pdf", "a/b/c/other.pdf")
        self._create_attachment_content(attachment2, "project timeline and milestones")

        result_ids = self._get_message_ids_for_narrow("quarterly", negated=True)

        self.assertNotIn(msg_matching_id, result_ids)
        self.assertIn(msg_other_id, result_ids)

    def test_failed_extraction_excluded(self) -> None:
        """
        Attachments with FAILED extraction status must not appear in results
        even if a tsvector was somehow written for them.
        """
        msg_id = self.send_stream_message(self.user_profile, "Denmark", "failed extraction")
        attachment = self._create_attachment(msg_id, "fail.pdf", "a/b/c/fail.pdf")
        content = AttachmentContent.objects.create(
            attachment=attachment,
            extracted_text="quarterly report content",
            extraction_status=AttachmentContent.ExtractionStatus.FAILED,
        )
        # Deliberately write a tsvector to confirm it is the status check
        # (not the absence of a tsvector) that causes exclusion.
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE zerver_attachmentcontent
                SET search_tsvector = to_tsvector('zulip.english_us_search', %s)
                WHERE id = %s
                """,
                ["quarterly report content", content.id],
            )

        result_ids = self._get_message_ids_for_narrow("quarterly")
        self.assertNotIn(msg_id, result_ids)

    def test_unsupported_file_type_excluded(self) -> None:
        """
        Attachments with UNSUPPORTED extraction status are also excluded.
        """
        msg_id = self.send_stream_message(self.user_profile, "Denmark", "unsupported file")
        attachment = self._create_attachment(msg_id, "image.png", "a/b/c/image.png")
        self._create_attachment_content(
            attachment,
            text=None,
            status=AttachmentContent.ExtractionStatus.UNSUPPORTED,
        )

        result_ids = self._get_message_ids_for_narrow("quarterly")
        self.assertNotIn(msg_id, result_ids)

    def test_message_with_multiple_attachments_any_match(self) -> None:
        """
        A message with multiple attachments should match if ANY one of them
        contains the search term — the EXISTS subquery naturally handles this.
        """
        msg_id = self.send_stream_message(self.user_profile, "Denmark", "two attachments")

        # First attachment: no match
        attachment1 = self._create_attachment(msg_id, "notes.pdf", "a/b/c/multi_notes.pdf")
        self._create_attachment_content(attachment1, "project timeline and milestones")

        # Second attachment: matches
        attachment2 = self._create_attachment(msg_id, "report.pdf", "a/b/c/multi_report.pdf")
        self._create_attachment_content(attachment2, "quarterly financial report")

        result_ids = self._get_message_ids_for_narrow("quarterly")
        self.assertIn(msg_id, result_ids)

    def test_no_attachmentcontent_row_excluded(self) -> None:
        """
        A message with an Attachment that has no AttachmentContent row at all
        (i.e. not yet enqueued) should not appear in results. The inner JOIN
        in the EXISTS subquery naturally handles this — no row means no match.
        """
        msg_id = self.send_stream_message(self.user_profile, "Denmark", "attachment not queued")
        self._create_attachment(msg_id, "new.pdf", "a/b/c/new.pdf")
        # Deliberately skip creating an AttachmentContent row

        result_ids = self._get_message_ids_for_narrow("quarterly")
        self.assertNotIn(msg_id, result_ids)