import re
from io import StringIO

from zerver.lib.test_classes import UploadSerializeMixin, ZulipTestCase
from zerver.lib.test_helpers import get_test_image_file
from zerver.models import Attachment


class TestTryUpload(UploadSerializeMixin, ZulipTestCase):
  def test_png_attachment_content_type_stored(self) -> None:
    self.login("hamlet")

    with get_test_image_file("img.png") as fp:
      result = self.client_post("/json/user_uploads", {"file": fp})

    response_dict = self.assert_json_success(result)
    url = response_dict["url"]
    path_id = re.sub(r"/user_uploads/", "", url)

    attachment = Attachment.objects.get(path_id=path_id)
    self.assertEqual(attachment.content_type, "image/png")
