import peewee

from utils import get_db

from models import BookIO

SET_TYPES = [
    ("train", "train"),
    ("test", "test"),
    ("benchmark", "benchmark"),
]


class TopicClassificationTrainingDataset(peewee.Model):
    """
    `topic_classification_training_dataset` table: Filtered items from TopicClassification used for training a classification model.
    """

    class Meta:
        table_name = "topic_classification_training_dataset"
        database = get_db()

    book = peewee.ForeignKeyField(
        model=BookIO,
        field="barcode",
        index=True,
        primary_key=True,
    )

    target_topic = peewee.CharField(
        max_length=256,
        null=True,
        unique=False,
        index=True,
    )

    set = peewee.CharField(
        max_length=256,
        null=True,
        unique=False,
        index=True,
        choices=SET_TYPES,
    )
