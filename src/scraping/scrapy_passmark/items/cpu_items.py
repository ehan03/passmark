# standard library imports

# third party imports
from scrapy import Field, Item

# local imports


class CPUItem(Item):
    id = Field()
    name = Field()
    description = Field()
    cpu_class = Field()
    socket = Field()
    clock_speed = Field()
    turbo_speed = Field()
    cores = Field()
    threads = Field()
    total_cores = Field()
    primary_cores = Field()
    secondary_cores = Field()
    performance_cores = Field()
    efficient_cores = Field()
    typical_tdp = Field()
    tdp_down = Field()
    tdp_up = Field()
    cache_per_cpu_package = Field()
    cache_per_effective_cpu_package = Field()
    memory_support = Field()
    other_names = Field()
    first_seen_on_charts = Field()
    cpu_mark_per_dollar_price = Field()
    overall_rank = Field()
    last_price_change = Field()
    multi_thread_rating = Field()
    single_thread_rating = Field()
    num_samples = Field()
    margin_for_error = Field()
    integer_math = Field()
    floating_point_math = Field()
    find_prime_numbers = Field()
    random_string_sorting = Field()
    data_encryption = Field()
    data_compression = Field()
    physics = Field()
    extended_instructions = Field()
    relative_gaming_score = Field()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields:
            self[field] = None


class CPUMarkDistributionItem(Item):
    cpu_id = Field()
    cpu_mark = Field()
    num_records = Field()


class CPUPricingHistoryItem(Item):
    cpu_id = Field()
    timestamp = Field()
    price = Field()
