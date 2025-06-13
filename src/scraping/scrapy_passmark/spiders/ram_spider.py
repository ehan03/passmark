# standard library imports
import re
from urllib.parse import parse_qs

# third party imports
from scrapy.spiders import Spider
from w3lib.html import remove_tags

# local imports
from ..items.ram_items import RAMItem, RAMPricingHistoryItem


class RAMSpider(Spider):
    name = "ram_spider"
    allowed_domains = ["memorybenchmark.net"]
    start_urls = [
        "https://www.memorybenchmark.net/ram_list-ddr2.php",
        "https://www.memorybenchmark.net/ram_list-ddr3.php",
        "https://www.memorybenchmark.net/ram_list-ddr4.php",
        "https://www.memorybenchmark.net/ram_list.php",
    ]
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "CONCURRENT_REQUESTS": 4,
        "COOKIES_ENABLED": False,
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
            "scrapy_user_agents.middlewares.RandomUserAgentMiddleware": 400,
        },
        "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "FEED_EXPORT_ENCODING": "utf-8",
        "DEPTH_PRIORITY": 1,
        "SCHEDULER_DISK_QUEUE": "scrapy.squeues.PickleFifoDiskQueue",
        "SCHEDULER_MEMORY_QUEUE": "scrapy.squeues.FifoMemoryQueue",
        "RETRY_TIMES": 0,
        "LOG_LEVEL": "INFO",
        "ITEM_PIPELINES": {
            "scrapy_passmark.pipelines.ram_pipelines.RAMItemPipeline": 100,
        },
        "CLOSESPIDER_ERRORCOUNT": 1,
        "DOWNLOAD_TIMEOUT": 600,
    }

    def parse(self, response):
        match = re.search(r"ddr\d+", response.url, re.IGNORECASE)
        generation = "DDR5"
        if match:
            generation = match.group(0).upper()

        ram_table = response.css("table.cpulist")
        links = ram_table.css("tr > td > a::attr(href)").getall()
        ram_ids = [int(parse_qs(url)["id"][0]) for url in links if "#price" not in url]

        for ram_id in ram_ids:
            # Skip RAM with ID 12066, doesn't exist
            if ram_id == 12066:
                continue

            yield response.follow(
                url=f"https://www.memorybenchmark.net/ram.php?id={ram_id}",
                callback=self.parse_ram,
                cb_kwargs={"ram_id": ram_id, "generation": generation},
            )

    def parse_ram(self, response, ram_id, generation):
        # Main RAM info
        try:
            ram_item = RAMItem()
            ram_item["id"] = ram_id
            ram_item["generation"] = generation

            desc_body = response.css("div.desc > div.desc-body")
            ram_item["name"] = (
                desc_body.css("div.desc-header > span.cpuname::text").get().strip()
            )

            main_desc = desc_body.css("em.left-desc-cpu, div.desc-foot")
            main_desc_p_tags = main_desc.css("p")
            for p_tag in main_desc_p_tags:
                text = [
                    remove_tags(x.replace("<br>", "[BREAK]")).strip()
                    for x in p_tag.get().split("<strong>")
                ]
                text = [x.strip() for x in text if x]

                label_mapping = {
                    "Description:": "description",
                    "Other names:": "other_names",
                    "Memory First Benchmarked:": "first_benchmarked",
                    "Last Price Change:": "last_price_change",
                }

                for text_part in text:
                    for label, field in label_mapping.items():
                        if text_part.startswith(label):
                            value_semi_cleaned = [
                                x.strip()
                                for x in text_part.replace(label, "").split("[BREAK]")
                                if x.strip()
                            ]
                            ram_item[field] = "; ".join(value_semi_cleaned).strip()
                            break

            main_ratings = response.css("div.desc > div.right-desc")
            ratings_texts = [
                x.strip().replace("*", "")
                for x in main_ratings.css("::text").getall()
                if x.strip() and x.strip() not in [":", "*"]
            ]
            for i, text in enumerate(ratings_texts):
                if text == "Average Mark":
                    ram_item["mark"] = ratings_texts[i + 1]
                elif text == "Samples:":
                    ram_item["num_samples"] = ratings_texts[i + 1]

            test_suite_table = response.css("table[id='test-suite-results']")
            rows = test_suite_table.css("tr")
            for row in rows:
                th = row.css("th::text").get().strip()
                td = row.css("td::text").get().strip()

                th_mapping = {
                    "Database Operations": "database_operations",
                    "Memory Read Cached": "memory_read_cached",
                    "Memory Read Uncached": "memory_read_uncached",
                    "Memory Write": "memory_write",
                    "Latency": "latency",
                    "Memory Threaded": "memory_threaded",
                }

                if th in th_mapping:
                    ram_item[th_mapping[th]] = td

            yield ram_item

            # Pricing history
            script_tags = response.css("script")
            for script in script_tags:
                script_full = script.get()

                if "var chartLabel" in script_full and "dataArray.push" in script_full:
                    matches = re.findall(
                        r"dataArray\.push\(\{x:\s*(\d+),\s*y:\s*([\d.]+)\}\)",
                        script_full,
                    )
                    price_data = [{"x": int(x), "y": float(y)} for x, y in matches]

                    for data in price_data:
                        pricing_history_item = RAMPricingHistoryItem()
                        pricing_history_item["ram_id"] = ram_id
                        pricing_history_item["timestamp"] = data["x"]
                        pricing_history_item["price"] = data["y"]

                        yield pricing_history_item
        except:
            print(f"Error parsing RAM ID {ram_id} on {response.url}")
