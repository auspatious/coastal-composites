import boto3
import geopandas as gpd
import pandas as pd
import typer
from coastlines.grids import VIETNAM_10
from dask.distributed import Client
from dea_tools.coastal import pixel_tides
from dep_tools.aws import object_exists
from dep_tools.stac_utils import set_stac_properties
from dep_tools.azure import blob_exists
from dep_tools.exceptions import EmptyCollectionError
from dep_tools.loaders import OdcLoader
from dep_tools.namers import DepItemPath
from dep_tools.processors import S2Processor
from dep_tools.searchers import PystacSearcher
from dep_tools.utils import get_logger

from datetime import datetime, timedelta

# from dep_tools.task import SimpleLoggingAreaTask
from dep_tools.writers import AwsDsCogWriter, AzureDsWriter
from odc.algo import geomedian_with_mads
from odc.geo.gridspec import GridSpec
from planetary_computer import sign_url
from typing_extensions import Annotated
from xarray import DataArray, Dataset

GRIDS = {
    "VIETNAM_10": VIETNAM_10,
}

TILE_FILE = "https://raw.githubusercontent.com/auspatious/dea-coastlines/stable/data/raw/vietnam_tile_ids.txt"


def get_tile_list() -> pd.DataFrame:
    return pd.read_csv(TILE_FILE)


def get_datetime_string(year: int, extra_months: int) -> str:
    extra_months_datetime = timedelta(days=30 * extra_months)

    start_date = datetime(year=year, month=1, day=1) - extra_months_datetime
    end_date = datetime(year=year, month=12, day=31) + extra_months_datetime

    datetime_string = f"{start_date:%Y-%m-%d}/{end_date:%Y-%m-%d}"

    return datetime_string

def get_geometry(tile_id: str, grid: GridSpec) -> gpd.GeoDataFrame:
    tile_tuple = tuple(int(i) for i in tile_id.split(","))
    tile = grid.tile_geobox(tile_tuple)
    geom = gpd.GeoDataFrame(
        {"geometry": [tile.extent.to_crs("EPSG:4326")]}, crs="EPSG:4326"
    )

    return geom


class CoastalCompositesProcessor(S2Processor):
    def __init__(
        self,
        send_area_to_processor: bool = False,
        load_data: bool = False,
        mask_clouds: bool = True,
        mask_clouds_kwargs: dict = dict(
            filters=[("closing", 5), ("opening", 5)], keep_ints=True
        ),
        tide_data_location: str = "~/Data/tide_models_clipped",
        low_or_high: str = "low",
    ) -> None:
        super().__init__(
            send_area_to_processor,
            scale_and_offset=False,
            harmonize_to_old=True,
            mask_clouds=mask_clouds,
            mask_clouds_kwargs=mask_clouds_kwargs,
        )
        self.load_data = load_data
        self.tide_data_location = tide_data_location
        if low_or_high not in ["low", "high"]:
            raise ValueError("low_or_high must be 'low' or 'high'")
        self.low_or_high = low_or_high

    def process(self, input_data: DataArray) -> Dataset:
        data = super().process(input_data)

        # Remove SCL
        data = data.drop_vars(["scl", "SCL"], errors="ignore")

        # Get low resolution tides for the study area
        tides_lowres = pixel_tides(
            data, resample=False, directory=self.tide_data_location
        )

        # Calculate the lowest and highest 10% of tides
        lowest_20, highest_20 = tides_lowres.quantile([0.2, 0.8]).values

        # Filter our data to low and high tide observations
        if self.low_or_high == "low":
            filtered = tides_lowres.where(tides_lowres <= lowest_20, drop=True)
        else:
            filtered = tides_lowres.where(tides_lowres >= highest_20, drop=True)

        # Filter out unwanted scenes
        data = data.sel(time=filtered.time)

        # TODO: Maybe filter out remaining data per-pixel

        # Process the geomad on the remaining data
        output = geomedian_with_mads(data, num_threads=4, work_chunks=(1000, 1000))

        if self.load_data:
            output = output.compute()

        return output


def main(
    tile_id: Annotated[str, typer.Option()],
    year: Annotated[int, typer.Option()],
    version: Annotated[str, typer.Option()],
    low_or_high: Annotated[str, typer.Option()] = "low",
    tide_data_location: str = "~/tide_models",
    extra_months: int = 12,
    output_bucket: str = None,
    output_resolution: int = 10,
    grid_definition: str = "VIETNAM_10",
    overwrite: Annotated[bool, typer.Option()] = False,
    memory_limit_per_worker: str = "120GB",
    n_workers: int = 2,
    threads_per_worker: int = 32,
    xy_chunk_size: int = 2501,
) -> None:
    log = get_logger(tile_id, "CoastalCompositesProcessor")
    log.info(f"Starting processing version {version} for {year} with {extra_months} either side")

    geom = get_geometry(tile_id, GRIDS[grid_definition])

    itempath = DepItemPath(
        "s2",
        f"{low_or_high}_tide_comp_20p",
        version,
        year,
        zero_pad_numbers=True,
        prefix="ausp",
    )
    stac_document = itempath.stac_path(
        tile_id,
    )

    # If we don't want to overwrite, and the destination file already exists, skip it
    if not overwrite:
        already_done = False
        if output_bucket is None:
            # The Azure case
            already_done = blob_exists(stac_document)
        else:
            # The AWS case
            already_done = object_exists(output_bucket, stac_document)

        if already_done:
            log.info(f"Item already exists at {stac_document}")
            # This is an exit with success
            raise typer.Exit()

    datetime_string = get_datetime_string(year, extra_months)

    # A searcher to find the data
    searcher = PystacSearcher(
        catalog="https://earth-search.aws.element84.com/v1/",
        collections=["sentinel-2-l2a"],
        datetime=datetime_string,
    )

    # A loader to load them
    loader = OdcLoader(
        crs=3832,
        resolution=output_resolution,
        bands=[
            "red",
            "green",
            "blue",
            "nir",
            "nir08",
            "rededge1",
            "rededge2",
            "rededge3",
            "swir16",
            "swir22",
            "scl",
        ],
        groupby="solar_day",
        chunks=dict(time=1, x=xy_chunk_size, y=xy_chunk_size),
        fail_on_error=False,
        patch_url=sign_url,
        overwrite=overwrite,
    )

    # A processor to process them
    processor = CoastalCompositesProcessor(
        low_or_high=low_or_high,
        tide_data_location=tide_data_location,
    )

    # And a writer to bind them
    if output_bucket is None:
        log.info("Writing with Azure writer")
        writer = AzureDsWriter(
            itempath=itempath,
            overwrite=overwrite,
            convert_to_int16=False,
            extra_attrs=dict(dep_version=version),
            write_multithreaded=True,
            load_before_write=True,
        )
    else:
        log.info("Writing with AWS writer")
        client = boto3.client("s3")
        writer = AwsDsCogWriter(
            itempath=itempath,
            overwrite=overwrite,
            convert_to_int16=False,
            extra_attrs=dict(dep_version=version),
            write_multithreaded=True,
            bucket=output_bucket,
            client=client,
        )

    with Client(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit_per_worker,
    ):
        try:
            # Run the task
            items = searcher.search(geom)
            log.info(f"Found {len(items)} items")

            data = loader.load(items, geom)
            log.info(f"Found {len(data.time)} timesteps to load")

            output_data = processor.process(data)
            log.info(
                f"Processed data to shape {[output_data.sizes[d] for d in ['x', 'y']]}"
            )

            # Hack for now... need to get it in the standard tools
            output_data = set_stac_properties(data, output_data)

            paths = writer.write(output_data, tile_id)
            if paths is not None:
                log.info(f"Completed writing to {paths[-1]}")
            else:
                log.warning("No paths returned from writer")

        except EmptyCollectionError:
            log.warning("No data found for this tile.")
        except Exception as e:
            log.exception(f"Failed to process with error: {e}")
            raise typer.Exit(code=1)


if __name__ == "__main__":
    typer.run(main)
