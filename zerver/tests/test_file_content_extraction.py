from zerver.lib.test_classes import UploadSerializeMixin, ZulipTestCase
from zerver.lib.upload import upload_message_attachment
from zerver.models import Attachment
from zerver.worker.file_content_extraction import FileContentExtractionWorker


class FileContentExtractionWorkerTest(UploadSerializeMixin, ZulipTestCase):
    def test_consume_processes_attachment(self) -> None:
        """Sanity check: worker fetches attachment and runs without error."""
        user_profile = self.example_user("hamlet")
        url, _ = upload_message_attachment(
            "dummy.txt", "text/plain", b"zulip!", user_profile
        )
        path_id = url.replace("/user_uploads/", "")
        attachment = Attachment.objects.get(path_id=path_id)

        worker = FileContentExtractionWorker()
        worker.consume({"id": attachment.id})

        # Worker completed without raising; attachment unchanged
        attachment.refresh_from_db()
        self.assertEqual(attachment.path_id, path_id)
        self.assertIn("text/plain", attachment.content_type or "")
