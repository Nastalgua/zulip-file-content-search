from django.db import migrations, models
import django.db.models.deletion
from django.contrib.postgres.indexes import GinIndex
from django.contrib.postgres.search import SearchVectorField


class Migration(migrations.Migration):

    dependencies = [
        ("zerver", "0778_realm_rendered_description_version"),
    ]

    operations = [
        migrations.CreateModel(
            name="AttachmentContent",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "attachment",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="content",
                        to="zerver.attachment",
                    ),
                ),
                (
                    "extraction_status",
                    models.PositiveSmallIntegerField(
                        choices=[
                            (1, "Pending"),
                            (2, "Success"),
                            (3, "Failed"),
                            (4, "Unsupported file type"),
                        ],
                        default=1,
                        db_index=True,
                    ),
                ),
                ("extracted_text", models.TextField(null=True)),
                ("search_tsvector", SearchVectorField(null=True)),
                ("last_attempted", models.DateTimeField(null=True, db_index=True)),
            ],
            options={
                "indexes": [
                    GinIndex(
                        fields=["search_tsvector"],
                        fastupdate=False,
                        name="attachment_content_search_tsvector",
                    )
                ],
            },
        ),
    ]