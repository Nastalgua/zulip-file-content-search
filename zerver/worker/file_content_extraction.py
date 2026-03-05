# Document content extraction worker.
#
# Extracts text from PDF, DOCX, and other document attachments asynchronously
# after upload. See zerver/worker/thumbnail.py for the reference implementation.
#
# Flow:
# 1. Fetch the file from local or S3 using save_attachment_contents(path_id, filehandle)
# 2. Determine file type from content_type (set at upload time)
# 3. Extract text using the appropriate library (e.g., PyMuPDF for PDF, python-docx for DOCX)
# 4. Store the extracted text and update the tsvector in PostgreSQL
import logging
from io import BytesIO
from typing import Any

from django.db import transaction
from typing_extensions import override

from zerver.lib.upload import save_attachment_contents
from zerver.models import Attachment
from zerver.worker.base import QueueProcessingWorker, assign_queue
from docx import Document

logger = logging.getLogger(__name__)


@assign_queue("file_content_extraction")
class FileContentExtractionWorker(QueueProcessingWorker):
    @override
    def consume(self, event: dict[str, Any]) -> None:
        attachment_id = event["id"]
        print(f"FileContentExtractionWorker: processing attachment id={attachment_id}")
        with transaction.atomic(savepoint=False):
            try:
                attachment = Attachment.objects.select_for_update(of=("self",)).get(
                    id=attachment_id
                )
            except Attachment.DoesNotExist:  # nocoverage
                logger.info("Attachment %d missing, skipping content extraction", attachment_id)
                return

            path_id = attachment.path_id
            content_type = attachment.content_type or ""

            # 1. Fetch file from local or S3
            with BytesIO() as filehandle:
                save_attachment_contents(path_id, filehandle)
                file_bytes = filehandle.getvalue()

            # 2. Determine file type from content_type
            # 3. Extract text using appropriate library (TODO: implement per content_type)
            # 4. Store extracted text + update tsvector (TODO: implement DB update)
            _extract_and_store(attachment, file_bytes, content_type)


def _extract_and_store(
    attachment: Attachment, file_bytes: bytes, content_type: str
) -> None:
    """Extract text from file_bytes and store in DB with updated tsvector.

    TODO: Implement extraction for PDF (e.g., PyMuPDF), DOCX (e.g., python-docx),
    and other supported types. Update DB fields.
    """
    if not content_type or not file_bytes:
        return

    # TODO: Dispatch by content_type and extract text

    elif content_type == "docx":
        extracted_text = extract_from_docx(file_bytes)
    # TODO: Save to attachment model and update DB

def extract_from_docx(file_bytes):
    file_content = BytesIO(file_bytes)
    document = Document(file_content)
    text = ""
    for paragraph in document.paragraphs:
        text+=paragraph.text+"\n"
    return text