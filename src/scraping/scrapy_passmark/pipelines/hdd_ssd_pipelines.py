# standard library imports
import os

# third party imports
import pandas as pd

# local imports
from ..constants import RAW_DATA_DIR
from ..items.hdd_ssd_items import HDDSSDItem, HDDSSDPricingHistoryItem


class HDDSSDItemPipeline:
    def __init__(self):
        self.hdds_ssds = []
        self.pricing_histories = []

    def process_item(self, item, spider):
        if isinstance(item, HDDSSDItem):
            self.hdds_ssds.append(item)
        elif isinstance(item, HDDSSDPricingHistoryItem):
            self.pricing_histories.append(item)
        return item

    def close_spider(self, spider):
        # Convert to dataframes
        hdds_ssds_df = (
            pd.DataFrame(self.hdds_ssds).sort_values(by="id").reset_index(drop=True)
        )
        pricing_histories_df = (
            pd.DataFrame(self.pricing_histories)
            .sort_values(by=["hdd_ssd_id", "timestamp"])
            .reset_index(drop=True)
        )

        # Reorder columns
        hdds_ssds_df = hdds_ssds_df[
            [
                "id",
                "name",
                "description",
                "size",
                "other_names",
                "first_benchmarked",
                "drive_rating_per_dollar_price",
                "overall_rank",
                "last_price_change",
                "drive_rating",
                "num_samples",
                "sequential_read",
                "sequential_write",
                "random_seek_read_write",
                "iops_4kqd1",
            ]
        ]
        pricing_histories_df = pricing_histories_df[
            ["hdd_ssd_id", "timestamp", "price"]
        ]

        # Save to CSV files
        hdds_ssds_df.to_csv(
            os.path.join(RAW_DATA_DIR, "hdd_ssd", "drives.csv"),
            index=False,
        )
        pricing_histories_df.to_csv(
            os.path.join(RAW_DATA_DIR, "hdd_ssd", "drive_pricing_histories.csv"),
            index=False,
        )
