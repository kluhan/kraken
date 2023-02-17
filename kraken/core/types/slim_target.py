from dataclasses import asdict, dataclass, field
from typing import Union

from dacite import from_dict


@dataclass(unsafe_hash=True)
class SlimTarget:
    """
    A slimmed down version of :class:`Target` that is used to transfer data
    between Tasks, without having to pass all metadata of the full
    :class:`Target` object.
    """

    id: str | None = field(default=None)
    """The ID of the corresponding :class:`Target`, if it exists."""
    tags: list[str] = field(default_factory=list)
    """The tags of the corresponding :class:`Target`"""
    kwargs: dict = field(default_factory=dict)
    """The kwargs of the corresponding :class:`Target`"""

    def update(self, other: Union["SlimTarget", dict]) -> None:
        """
        Updates the Target with the values of another Target. The tags are
        combined by joining the lists and removing duplicates. The kwargs are
        combined via the :func:`dict.update`. Values of :attr:`other.kwargs`
        will take precedence over the values of `self.kwargs` for keys present
        in both.

        Parameters
        ----------
        other: :class:`SlimTarget`
            The other Target to update this Target with.

        Raises
        ------
        ValueError
            If the other Target has an ID.
        """

        # If the other is a SlimTarget, convert it to a dict
        if isinstance(other, SlimTarget):
            other = asdict(other)

        # If the other is a dict, convert it to a SlimTarget
        if other.get("id", None) is not None:
            raise ValueError("Cannot update SlimTarget using a SlimTarget with an ID.")

        # Update the tags and kwargs
        self.tags = list(set(self.tags + other.get("tags", [])))
        self.kwargs.update(other.get("kwargs", {}))

    @classmethod
    def from_dict(cls, data: dict) -> "SlimTarget":
        """
        Creates a :class:`SlimTarget` object from a dictionary.

        Parameters
        ----------
        data: dict
            The dictionary to create the object from.

        Returns
        -------
        :class:`SlimTarget`
            The created object.
        """
        return from_dict(data_class=cls, data=data)

    @classmethod
    def merge(
        cls, a: Union["SlimTarget", dict], b: Union["SlimTarget", dict]
    ) -> "SlimTarget":
        """
        Combines two :class:`SlimTarget` objects into a single one, similar to
        :meth:`update`, but without modifying the original objects and instead
        returning a new one. 'kwargs' will be merged by :meth:`dict.update`, so
        values of `b.kwargs` will take precedence over the values of `a.kwargs` for
        keys present in both `a.kwargs` and `b.kwargs`.

        Parameters
        ----------
        a: :class:`SlimTarget` | :class:`dict`
            The first SlimTarget to merge.
        b: :class:`SlimTarget` | :class:`dict`
            The second SlimTarget to merge.

        Returns
        -------
        :class:`SlimTarget`
            The created object.
        """
        # If the a is a SlimTarget, convert it to a dict
        if isinstance(a, SlimTarget):
            a = asdict(a)

        # If the b is a SlimTarget, convert it to a dict
        if isinstance(b, SlimTarget):
            b = asdict(b)

        if (
            a.get("id", None) is not None
            and b.get("id", None) is not None
            and a["id"] != b["id"]
        ):
            raise ValueError("Cannot merge SlimTargets with multiple, but unequal IDs.")

        # Merge field by field
        id = a.get("id", None) if a.get("id", None) is not None else b.get("id", None)
        tags = list(set(a.get("tags", []) + b.get("tags", [])))
        kwargs = a.get("kwargs", {}).copy()
        kwargs.update(b.get("kwargs", {}))

        return SlimTarget(id=id, tags=tags, kwargs=kwargs)
