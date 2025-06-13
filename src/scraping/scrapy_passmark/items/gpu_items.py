# standard library imports

# third party imports
from scrapy import Field, Item

# local imports


class GPUItem(Item):
    id = Field()
    name = Field()
    bus_interface = Field()
    max_memory_size = Field()
    core_clock = Field()
    memory_clock = Field()
    directx_version = Field()
    opengl_version = Field()
    max_tdp = Field()
    category = Field()
    other_names = Field()
    first_benchmarked = Field()
    g3d_mark_per_dollar_price = Field()
    overall_rank = Field()
    last_price_change = Field()
    g3d_mark = Field()
    g2d_mark = Field()
    num_samples = Field()
    directx_9 = Field()
    directx_10 = Field()
    directx_11 = Field()
    directx_12 = Field()
    gpu_compute = Field()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields:
            self[field] = None


class G3DMarkDistributionItem(Item):
    gpu_id = Field()
    g3d_mark = Field()
    num_records = Field()


class GPUPricingHistoryItem(Item):
    gpu_id = Field()
    timestamp = Field()
    price = Field()
