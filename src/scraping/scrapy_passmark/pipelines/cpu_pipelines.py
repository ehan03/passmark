# standard library imports
import os

# third party imports
import pandas as pd

# local imports
from ..constants import RAW_DATA_DIR
from ..items.cpu_items import CPUItem, CPUMarkDistributionItem, CPUPricingHistoryItem


class CPUItemPipeline:
    def __init__(self):
        self.cpus = []
        self.distributions = []
        self.pricing_histories = []

    def process_item(self, item, spider):
        if isinstance(item, CPUItem):
            self.cpus.append(item)
        elif isinstance(item, CPUMarkDistributionItem):
            self.distributions.append(item)
        elif isinstance(item, CPUPricingHistoryItem):
            self.pricing_histories.append(item)
        return item

    def close_spider(self, spider):
        # Convert to dataframes
        cpus_df = pd.DataFrame(self.cpus).sort_values(by="id").reset_index(drop=True)
        distributions_df = (
            pd.DataFrame(self.distributions)
            .sort_values(by=["cpu_id", "cpu_mark"])
            .reset_index(drop=True)
        )
        pricing_histories_df = (
            pd.DataFrame(self.pricing_histories)
            .sort_values(by=["cpu_id", "timestamp"])
            .reset_index(drop=True)
        )

        # Reorder columns
        cpus_df = cpus_df[
            [
                "id",
                "name",
                "description",
                "cpu_class",
                "socket",
                "clock_speed",
                "turbo_speed",
                "cores",
                "threads",
                "total_cores",
                "primary_cores",
                "secondary_cores",
                "performance_cores",
                "efficient_cores",
                "typical_tdp",
                "tdp_down",
                "tdp_up",
                "cache_per_cpu_package",
                "cache_per_effective_cpu_package",
                "memory_support",
                "other_names",
                "first_seen_on_charts",
                "cpu_mark_per_dollar_price",
                "overall_rank",
                "last_price_change",
                "multi_thread_rating",
                "single_thread_rating",
                "num_samples",
                "margin_for_error",
                "integer_math",
                "floating_point_math",
                "find_prime_numbers",
                "random_string_sorting",
                "data_encryption",
                "data_compression",
                "physics",
                "extended_instructions",
                "relative_gaming_score",
            ]
        ]
        distributions_df = distributions_df[["cpu_id", "cpu_mark", "num_records"]]
        pricing_histories_df = pricing_histories_df[["cpu_id", "timestamp", "price"]]

        # Save to CSV files
        cpus_df.to_csv(os.path.join(RAW_DATA_DIR, "cpus.csv"), index=False)
        distributions_df.to_csv(
            os.path.join(RAW_DATA_DIR, "cpu_mark_distributions.csv"), index=False
        )
        pricing_histories_df.to_csv(
            os.path.join(RAW_DATA_DIR, "cpu_pricing_histories.csv"), index=False
        )
