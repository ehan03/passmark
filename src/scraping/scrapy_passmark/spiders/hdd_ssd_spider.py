# standard library imports
import re
from urllib.parse import parse_qs

# third party imports
from scrapy.spiders import Spider
from w3lib.html import remove_tags

# local imports
from ..items.hdd_ssd_items import HDDSSDItem, HDDSSDPricingHistoryItem


class HDDSSDSpider(Spider):
    name = "hdd_ssd_spider"
    allowed_domains = ["harddrivebenchmark.net"]
    start_urls = ["https://www.harddrivebenchmark.net/hdd_list.php"]
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
            "scrapy_passmark.pipelines.hdd_ssd_pipelines.HDDSSDItemPipeline": 100,
        },
        "CLOSESPIDER_ERRORCOUNT": 1,
        "DOWNLOAD_TIMEOUT": 600,
    }

    def parse(self, response):
        hdd_ssd_table = response.css("table.cpulist")
        links = hdd_ssd_table.css("tr > td > a::attr(href)").getall()
        hdd_ssd_ids = [
            int(parse_qs(url)["id"][0]) for url in links if "#price" not in url
        ]

        for hdd_ssd_id in hdd_ssd_ids:
            yield response.follow(
                url=f"https://www.harddrivebenchmark.net/hdd.php?id={hdd_ssd_id}",
                callback=self.parse_hdd_ssd,
                cb_kwargs={"hdd_ssd_id": hdd_ssd_id},
            )

    def parse_hdd_ssd(self, response, hdd_ssd_id):
        # Main HDD/SSD info
        hdd_ssd_item = HDDSSDItem()
        hdd_ssd_item["id"] = hdd_ssd_id

        desc_body = response.css("div.desc > div.desc-body")
        hdd_ssd_item["name"] = (
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
                "Drive Size:": "size",
                "Other names:": "other_names",
                "Drive First Benchmarked:": "first_benchmarked",
                "Drive Rating/$Price:": "drive_rating_per_dollar_price",
                "Overall Rank:": "overall_rank",
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
                        hdd_ssd_item[field] = "; ".join(value_semi_cleaned).strip()
                        break

        main_ratings = response.css("div.desc > div.right-desc")
        ratings_texts = [
            x.strip().replace("*", "")
            for x in main_ratings.css("::text").getall()
            if x.strip() and x.strip() not in [":", "*"]
        ]
        for i, text in enumerate(ratings_texts):
            if text == "Average Drive Rating":
                hdd_ssd_item["drive_rating"] = ratings_texts[i + 1]
            elif text == "Samples:":
                hdd_ssd_item["num_samples"] = ratings_texts[i + 1]

        test_suite_table = response.css("table[id='test-suite-results']")
        rows = test_suite_table.css("tr")
        for row in rows:
            th = row.css("th::text").get().strip()
            td = row.css("td::text").get().strip()

            th_mapping = {
                "Sequential Read": "sequential_read",
                "Sequential Write": "sequential_write",
                "Random Seek Read Write (IOPS 32KQD20)": "random_seek_read_write",
                "IOPS 4KQD1": "iops_4kqd1",
            }

            if th in th_mapping:
                hdd_ssd_item[th_mapping[th]] = td

        yield hdd_ssd_item

        # Pricing history
        script_tags = response.css("script")
        for script in script_tags:
            script_full = script.get()

            if "var chartLabel" in script_full and "dataArray.push" in script_full:
                matches = re.findall(
                    r"dataArray\.push\(\{x:\s*(\d+),\s*y:\s*([\d.]+)\}\)", script_full
                )
                price_data = [{"x": int(x), "y": float(y)} for x, y in matches]

                for data in price_data:
                    pricing_history_item = HDDSSDPricingHistoryItem()
                    pricing_history_item["hdd_ssd_id"] = hdd_ssd_id
                    pricing_history_item["timestamp"] = data["x"]
                    pricing_history_item["price"] = data["y"]

                    yield pricing_history_item
