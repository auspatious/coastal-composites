import json
import sys
from itertools import product
from typing import Annotated, Optional

import typer
from dep_tools.azure import blob_exists
from dep_tools.aws import object_exists

from run_task import get_tile_list, get_item_path


def main(
    years: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    tile_ids: Optional[str] = None,
    limit: Optional[str] = None,
    low_or_high: Optional[str] = "low",
    output_bucket: Optional[str] = None,
    overwrite: Annotated[bool, typer.Option()] = False,
) -> None:
    tiles = get_tile_list()

    if tile_ids is not None:
        tiles = tile_ids.split(";")
        for tile_id in tiles:
            if tile_id not in tiles:
                raise ValueError(f"{tile_id} is not a valid value for --tile-ids")

    if limit is not None:
        limit = int(limit)

    # Makes a list no matter what
    years = years.split("-")
    if len(years) == 2:
        years = range(int(years[0]), int(years[1]) + 1)
    elif len(years) > 2:
        ValueError(f"{years} is not a valid value for --years")

    tasks = [
        {"tile-id": tile, "year": year, "version": version}
        for tile, year in product(list(tiles), years)
    ]

    # If we don't want to overwrite, then we should only run tasks that don't already exist
    # i.e., they failed in the past or they're missing for some other reason
    if not overwrite:
        valid_tasks = []
        for task in tasks:
            itempath = get_item_path(
                "s2", low_or_high, version, task["year"], prefix="ausp"
            )
            stac_path = itempath.stac_path(task["tile-id"])
            exists = False
            if output_bucket is not None:
                exists = object_exists(output_bucket, stac_path)
            else:
                exists = blob_exists(stac_path)
            if not exists:
                valid_tasks.append(task)
            if len(valid_tasks) == limit:
                break
        # Switch to this list of tasks, which has been filtered
        tasks = valid_tasks
    else:
        # If we are overwriting, we just keep going
        pass

    if limit is not None:
        tasks = tasks[0:limit]

    json.dump(tasks, sys.stdout)


if __name__ == "__main__":
    typer.run(main)
