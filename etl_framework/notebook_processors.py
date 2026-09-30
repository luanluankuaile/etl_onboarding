"""Ordinary Python entry points suitable for notebook-exported processors."""
from .blueprints import LandingToRawBlueprint, PersistentToConsumptionBlueprint, RawToPersistentBlueprint
from .control import ControlService
from .metadata import Metadata


def _processor_name(context) -> str:
    return context.processor_parameters["processor_name"]


def _target_name(target: str) -> str:
    return target.split(".", 1)[-1]


def landing_to_raw(context):
    metadata = context.values["metadata"]
    blueprint = LandingToRawBlueprint(context, ControlService(context.control_db), metadata)
    return blueprint.processor(_processor_name(context)).execute()


def raw_to_persistent(context):
    metadata: Metadata = context.values["metadata"]
    targets = {_target_name(target) for target in context.target_metadata["tables"]}
    mappings = [mapping for mapping in metadata.persistent if not targets or mapping.target_table in targets]
    if not mappings:
        raise ValueError(f"{_processor_name(context)} has no matching Persistent table mapping")
    return [
        RawToPersistentBlueprint(context, ControlService(context.control_db), mapping)
        .processor(_processor_name(context)).execute()
        for mapping in mappings
    ]


def persistent_to_consumption(context):
    metadata: Metadata = context.values["metadata"]
    items = metadata.consumption if isinstance(metadata.consumption, list) else [metadata.consumption]
    targets = {_target_name(target) for target in context.target_metadata["tables"]}
    items = [item for item in items if not targets or item.get("name", item.get("table")) in targets]
    if not items:
        raise ValueError(f"{_processor_name(context)} has no matching Consumption transformation")
    return [
        PersistentToConsumptionBlueprint(context, ControlService(context.control_db), item)
        .processor(_processor_name(context)).execute()
        for item in items
    ]