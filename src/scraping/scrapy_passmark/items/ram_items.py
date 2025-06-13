# standard library imports

# third party imports
from scrapy import Field, Item

# local imports


class RAMItem(Item):
    id = Field()
    generation = Field()
    name = Field()
    description = Field()
    other_names = Field()
    first_benchmarked = Field()
    last_price_change = Field()
    mark = Field()
    num_samples = Field()
    database_operations = Field()
    memory_read_cached = Field()
    memory_read_uncached = Field()
    memory_write = Field()
    latency = Field()
    memory_threaded = Field()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields:
            self[field] = None


class RAMPricingHistoryItem(Item):
    ram_id = Field()
    timestamp = Field()
    price = Field()
