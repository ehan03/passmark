# standard library imports

# third party imports
from scrapy import Field

# local imports


class GPUItem:
    id = Field()
    name = Field()


class G3DMarkDistributionItem:
    gpu_id = Field()
    g3d_mark = Field()
    num_records = Field()


class GPUPricingHistoryItem:
    gpu_id = Field()
    timestamp = Field()
    price = Field()
