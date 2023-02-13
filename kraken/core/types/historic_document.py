from datetime import datetime, timedelta
from typing import Tuple

import jsonpatch
import json

from mongoengine.document import (
    Document,
    EmbeddedDocument,
)
from mongoengine.fields import (
    EmbeddedDocumentListField,
    DateTimeField,
    ListField,
    ReferenceField,
)

from .crawl import Crawl

CFM_MAX_AGE = timedelta(days=356)


# TODO: Add documentation
class Patch(EmbeddedDocument):
    crawl = ReferenceField("Crawl")  # TODO: Add type hint
    timestamp: datetime = DateTimeField(default=datetime.utcnow)  # type: ignore
    changes: list[dict] = ListField(default=list)  # type: ignore


# TODO: Add documentation
class Witness(EmbeddedDocument):
    """EmbeddedDocument to indicate the time when a data point was observed"""

    crawl = ReferenceField("Crawl")  # TODO: Add type hint
    timestamp: datetime = DateTimeField(default=datetime.utcnow())  # type: ignore


class HistoricDocument(Document):
    witnesses: list[Witness] = EmbeddedDocumentListField(Witness)  # type: ignore
    updates: list[Patch] = EmbeddedDocumentListField(Patch)  # type: ignore

    meta = {
        "abstract": True,
    }

    def save(self, crawl: Crawl, *args, **kwargs) -> Tuple[bool, int, dict]:
        # Update self based on existing records of the document.
        self, new_document, changes_observed, patch = history(self, crawl)

        # Call super-constructor
        super(HistoricDocument, self).save(*args, **kwargs)

        # Compute metrics based on the different models.
        metrics = self.metrics(
            new_document=new_document,
            changes_observed=changes_observed,
            patch=patch,
        )

        # Return a summary of the performed changes
        return (new_document, changes_observed, metrics)

    def metrics(self, new_document: bool, changes_observed: int, patch: Patch | None):
        return {
            model_name: model(
                new_document=new_document,
                changes_observed=changes_observed,
                patch=patch,
            )
            for (model_name, model) in self.models()
        }

    def models(self):
        return [("bfm", self.bfm_model), ("cfm", self.cfm_model)]

    def bfm_model(self, new_document: bool, changes_observed: int, *args, **kwargs):
        # If document is new return 0
        if new_document:
            return 1
        # If document is not new and has not changed since the last observation
        # return 0
        elif changes_observed == 0:
            return 0
        # If document has changed since the last observation return 1
        else:
            return 1

    def cfm_model(self, new_document: bool, patch: Patch | None, *args, **kwargs):
        # If document is new return 0
        if new_document:
            return 1
        # If document is not new and has not changed since the last observation
        # return 0
        elif patch is None:
            return 0
        # If document has changed since the last observation derive change based
        # on the time elapsed between the two observations
        else:
            latest_timestamp: datetime = self.witnesses[-1].timestamp
            previous_timestamp: datetime = self.witnesses[-2].timestamp
            age_div = latest_timestamp - previous_timestamp

            return min((age_div.total_seconds() / CFM_MAX_AGE.total_seconds()), 1)

    def wcf_model(self, new_document: bool, patch: Patch | None, *args, **kwargs):
        if new_document:
            return 1
        elif patch is None:
            return 0

        total_weight = sum(self.wcf_weights().values())
        wcf = 0
        for key, value in self.wcf_weights().items():
            for change in patch.changes:
                if change["path"].startswith(f"/{key}"):
                    wcf += value / total_weight
                    break

        return wcf

    def wcf_weights(self):
        raise NotImplementedError

    def weight(self):
        return 0


def history(
    document: HistoricDocument, crawl: Crawl
) -> Tuple[HistoricDocument, bool, int, Patch | None]:
    """Saves timestamp and modified fields in history.
    Updates data so that the old state can be reconstructed. Caution, the
    passed observed_state can also be altered. Caution this operation may
    clears all version control information before populating them again."""  # TODO: Rewrite documentation

    changes_observed = 0
    patch = None

    # Try to load the persistent version of the document
    try:
        persistent_document: HistoricDocument | None = type(document).objects.get(  # type: ignore
            pk=document.pk
        )

    except type(document).DoesNotExist:  # type: ignore
        persistent_document = None

    # If the document has not been seen before, there is no need to merge
    # different versions.
    if persistent_document is None:
        # Mark document as new
        new_document = True

    # If a persistent version was found, merge the new version with the
    # persistent version.
    else:
        # Mark document as old
        new_document = False

        # Copy the VCI of the persistent version to the newly acquired version,
        # before comparing them.
        document.witnesses = persistent_document.witnesses
        document.updates = persistent_document.updates

        # If changes to the payload are detected, update the document accordingly.
        if document.to_mongo() != persistent_document.to_mongo():

            # Prepare both documents for patch-generation by transforming them
            # to JSON.
            serialized_live_document = json.loads(document.to_json())
            serialized_persistent_document = json.loads(persistent_document.to_json())

            # Generate patch. Since we have already copied all VCI to the new
            # document in advance, these are effectively ignored here.
            changes = list(
                jsonpatch.make_patch(
                    serialized_live_document, serialized_persistent_document
                )
            )
            patch = Patch(crawl=crawl.pk, changes=changes)
            changes_observed = len(patch.changes)

            # Add patch to the document so that it is saved.
            document.updates.append(patch)

    # Adding new witnesses to the document so that it can be reconstructed when
    # the persistent version of the document has been compared to the live version.
    document.witnesses.append(Witness(crawl=crawl))

    # return modified document, the amount of changes and the added patch
    return (document, new_document, changes_observed, patch)
