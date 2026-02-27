from sqlalchemy.sql import column, select, table
from sqlalchemy.types import Integer

from zerver.lib.narrow import (
    BadNarrowOperatorError,
    NarrowBuilder,
    NarrowParameter,
    get_base_query_for_search,
)
from zerver.lib.test_classes import ZulipTestCase


class FileContentNarrowTest(ZulipTestCase):
    """
    Tests for the file_content narrow operator added to NarrowBuilder.
    At this stage, by_file_content is a placeholder (raises NotImplementedError),
    so we're validating registration and routing - not the SQL logic itself.
    """

    def setUp(self) -> None:
        super().setUp()
        self.user_profile = self.example_user("hamlet")
        self.realm = self.user_profile.realm
        self.msg_id_column = column("message_id", Integer)
        self.builder = NarrowBuilder(
            self.user_profile,
            self.msg_id_column,
            self.realm,
        )

    def test_file_content_operator_is_registered(self) -> None:
        """The operator should be in by_method_map and not raise 'unknown operator'."""
        self.assertIn("file-content", self.builder.by_method_map)