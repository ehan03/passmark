# standard library imports

# third party imports
from scrapy import Field, Item

# local imports


class HDDSSDItem(Item):
    id = Field()
    name = Field()
    description = Field()
    size = Field()
    other_names = Field()
    first_benchmarked = Field()
    drive_rating_per_dollar_price = Field()
    overall_rank = Field()
    last_price_change = Field()
    drive_rating = Field()
    num_samples = Field()
    sequential_read = Field()
    sequential_write = Field()
    random_seek_read_write = Field()
    iops_4kqd1 = Field()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields:
            self[field] = None


class HDDSSDPricingHistoryItem(Item):
    hdd_ssd_id = Field()
    timestamp = Field()
    price = Field()
