import boto3
import geopandas as gpd
import pandas as pd
import typer
from coastlines.grids import VIETNAM_10
from dask.distributed import Client
from dea_tools.coastal import pixel_tides
from dep_tools.aws import object_exists
from dep_tools.azure import blob_exists
from dep_tools.exceptions import EmptyCollectionError
from dep_tools.loaders import OdcLoader
from dep_tools.namers import DepItemPath
from dep_tools.processors import S2Processor
from dep_tools.searchers import PystacSearcher
from dep_tools.utils import get_logger

from odc.algo import mask_cleanup, erase_bad

from coastlines.combined import mask_pixels_by_tide

from odc.geo.geobox import scaled_down_geobox

from datetime import datetime, timezone

from dateutil.relativedelta import relativedelta

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
    return set(pd.read_csv(TILE_FILE).values.flatten())


def get_datetime_string(year: int, extra_months: int) -> str:
    months = relativedelta(months=extra_months)
    start_date = datetime(year=year, month=1, day=1) - months
    end_date = datetime(year=year, month=12, day=31) + months

    datetime_string = f"{start_date:%Y-%m-%d}/{end_date:%Y-%m-%d}"

    return datetime_string


def set_stac_properties(
    datetime_string: str, output_xr: DataArray | Dataset
) -> Dataset | DataArray:
    # Convert the xarray numpy datetime to a python datetime
    start, end = datetime_string.split("/")
    start_datetime = datetime.strptime(
        start,
        "%Y-%m-%d",
    )
    end_datetime = datetime.strptime(end, "%Y-%m-%d")

    center_time = start_datetime + (end_datetime - start_datetime) / 2

    utc = timezone.utc
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    date_format_00 = "%Y-%m-%dT00:00:00Z"
    date_format_24 = "%Y-%m-%dT23:59:59Z"

    output_xr.attrs["stac_properties"] = dict(
        start_datetime=start_datetime.astimezone(utc).strftime(date_format_00),
        datetime=center_time.astimezone(utc).strftime(date_format),
        end_datetime=end_datetime.astimezone(utc).strftime(date_format_24),
        created=datetime.now().astimezone(utc).strftime(date_format),
    )

    return output_xr


def get_tile_geometry(
    tile_id: str, grid: GridSpec, decimated: bool = False
) -> gpd.GeoDataFrame:
    tile_tuple = tuple(int(i) for i in tile_id.split(","))
    tile = grid.tile_geobox(tile_tuple)

    if decimated:
        tile = scaled_down_geobox(tile, 10)

    geom = gpd.GeoDataFrame(
        {"geometry": [tile.extent.to_crs("EPSG:4326")]}, crs="EPSG:4326"
    )

    return tile, geom


def get_item_path(
    base_product: str, low_or_high: str, version: str, year: int, prefix: str
) -> DepItemPath:
    return DepItemPath(
        base_product,
        f"{low_or_high}_tide_comp_20p",
        version,
        year,
        zero_pad_numbers=True,
        prefix=prefix,
    )


class CoastalCompositesProcessor(S2Processor):
    def __init__(
        self,
        send_area_to_processor: bool = False,
        load_data: bool = False,
        mask_clouds: bool = False,
        mask_clouds_kwargs: dict = dict(
            filters=[("closing", 5), ("opening", 5)], keep_ints=True
        ),
        tide_data_location: str = "~/Data/tide_models_clipped",
        mask_pixels_by_tide: bool = False,
        low_or_high: str = "low",
    ) -> None:
        super().__init__(
            send_area_to_processor,
            scale_and_offset=False,
            harmonize_to_old=False,
            mask_clouds=mask_clouds,
            mask_clouds_kwargs=mask_clouds_kwargs,
        )
        self.load_data = load_data
        self.tide_data_location = tide_data_location
        if low_or_high not in ["low", "high"]:
            raise ValueError("low_or_high must be 'low' or 'high'")
        self.low_or_high = low_or_high
        self.mask_pixels_by_tide = mask_pixels_by_tide

    def process(self, input_data: DataArray) -> Dataset:
        data = super().process(input_data)

        mask = data.cloud > 50
        mask = mask_cleanup(mask, [("dilation", 5)])
        data = erase_bad(input_data, mask)

        # Remove SCL
        data = data.drop_vars(["scl", "SCL", "cloud"], errors="ignore")

        # Get low resolution tides for the study area
        tides_lowres = pixel_tides(
            data, resample=False, directory=self.tide_data_location
        )

        # Calculate the lowest and highest % of tides
        lowest_20, highest_20 = tides_lowres.quantile([0.30, 0.70]).values

        # Filter our data to low and high tide observations
        if self.low_or_high == "low":
            filtered = tides_lowres.where(tides_lowres <= lowest_20, drop=True)
        else:
            filtered = tides_lowres.where(tides_lowres >= highest_20, drop=True)

        # Filter out unwanted scenes
        data = data.sel(time=filtered.time)

        print(f"Filtered to {len(data.time)} scenes")

        if self.mask_pixels_by_tide:
            data = mask_pixels_by_tide(data, self.tide_data_location, 0.0)

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
    decimated: bool = False,
    grid_definition: str = "VIETNAM_10",
    overwrite: Annotated[bool, typer.Option()] = False,
    memory_limit_per_worker: str = "160GB",
    n_workers: int = 2,
    threads_per_worker: int = 32,
    xy_chunk_size: int = 2501,
) -> None:
    log = get_logger(tile_id, "CoastalCompositesProcessor")
    log.info(
        f"Starting processing version {version} for {year} with {extra_months} months either side"
    )

    tile, geom = get_tile_geometry(tile_id, GRIDS[grid_definition], decimated)

    itempath = get_item_path("s2", low_or_high, version, year, "ausp")

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
        collections=["sentinel-2-c1-l2a"],
        datetime=datetime_string,
    )

    # A loader to load them
    loader = OdcLoader(
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
            "cloud",
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

            data = loader.load(items, tile)
            log.info(f"Found {len(data.time)} timesteps to load")

            output_data = processor.process(data)
            log.info(
                f"Processed data to shape {[output_data.sizes[d] for d in ['x', 'y']]}"
            )

            # Hack for now... need to get it in the standard tools
            output_data = set_stac_properties(datetime_string, output_data)

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
