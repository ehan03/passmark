# standard library imports
import os

# third party imports
import pandas as pd

# local imports
from ..constants import RAW_DATA_DIR
from ..items.gpu_items import G3DMarkDistributionItem, GPUItem, GPUPricingHistoryItem


class GPUItemPipeline:
    def __init__(self):
        self.gpus = []
        self.distributions = []
        self.pricing_histories = []

    def process_item(self, item, spider):
        if isinstance(item, GPUItem):
            self.gpus.append(item)
        elif isinstance(item, G3DMarkDistributionItem):
            self.distributions.append(item)
        elif isinstance(item, GPUPricingHistoryItem):
            self.pricing_histories.append(item)
        return item

    def close_spider(self, spider):
        # Convert to dataframes
        gpus_df = pd.DataFrame(self.gpus).sort_values(by="id").reset_index(drop=True)
        distributions_df = (
            pd.DataFrame(self.distributions)
            .sort_values(by=["gpu_id", "g3d_mark"])
            .reset_index(drop=True)
        )
        pricing_histories_df = (
            pd.DataFrame(self.pricing_histories)
            .sort_values(by=["gpu_id", "timestamp"])
            .reset_index(drop=True)
        )

        # Reorder columns
        gpus_df = gpus_df[
            [
                "id",
                "name",
                "bus_interface",
                "max_memory_size",
                "core_clock",
                "memory_clock",
                "directx_version",
                "opengl_version",
                "max_tdp",
                "category",
                "other_names",
                "first_benchmarked",
                "g3d_mark_per_dollar_price",
                "overall_rank",
                "last_price_change",
                "g3d_mark",
                "g2d_mark",
                "num_samples",
                "directx_9",
                "directx_10",
                "directx_11",
                "directx_12",
                "gpu_compute",
            ]
        ]
        distributions_df = distributions_df[["gpu_id", "g3d_mark", "num_records"]]
        pricing_histories_df = pricing_histories_df[["gpu_id", "timestamp", "price"]]

        # Save to CSV files
        gpus_df.to_csv(os.path.join(RAW_DATA_DIR, "gpu", "gpus.csv"), index=False)
        distributions_df.to_csv(
            os.path.join(RAW_DATA_DIR, "gpu", "g3d_mark_distributions.csv"), index=False
        )
        pricing_histories_df.to_csv(
            os.path.join(RAW_DATA_DIR, "gpu", "gpu_pricing_histories.csv"), index=False
        )
