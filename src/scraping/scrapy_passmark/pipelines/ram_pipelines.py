# standard library imports
import os

# third party imports
import pandas as pd

# local imports
from ..constants import RAW_DATA_DIR
from ..items.ram_items import RAMItem, RAMPricingHistoryItem


class RAMItemPipeline:
    def __init__(self):
        self.rams = []
        self.pricing_histories = []

    def process_item(self, item, spider):
        if isinstance(item, RAMItem):
            self.rams.append(item)
        elif isinstance(item, RAMPricingHistoryItem):
            self.pricing_histories.append(item)
        return item

    def close_spider(self, spider):
        # Convert to dataframes
        rams_df = pd.DataFrame(self.rams).sort_values(by="id").reset_index(drop=True)
        pricing_histories_df = (
            pd.DataFrame(self.pricing_histories)
            .sort_values(by=["ram_id", "timestamp"])
            .reset_index(drop=True)
        )

        # Reorder columns
        rams_df = rams_df[
            [
                "id",
                "generation",
                "name",
                "description",
                "other_names",
                "first_benchmarked",
                "last_price_change",
                "mark",
                "num_samples",
                "database_operations",
                "memory_read_cached",
                "memory_read_uncached",
                "memory_write",
                "latency",
                "memory_threaded",
            ]
        ]
        pricing_histories_df = pricing_histories_df[["ram_id", "timestamp", "price"]]

        # Save to CSV files
        rams_df.to_csv(
            os.path.join(RAW_DATA_DIR, "ram", "ram_modules.csv"),
            index=False,
        )
        pricing_histories_df.to_csv(
            os.path.join(RAW_DATA_DIR, "ram", "ram_pricing_histories.csv"),
            index=False,
        )
