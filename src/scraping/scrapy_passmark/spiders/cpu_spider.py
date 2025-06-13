# standard library imports
import re
from urllib.parse import parse_qs

# third party imports
from scrapy.spiders import Spider
from w3lib.html import remove_tags

# local imports
from ..items.cpu_items import CPUItem, CPUMarkDistributionItem, CPUPricingHistoryItem


class CPUSpider(Spider):
    name = "cpu_spider"
    allowed_domains = ["cpubenchmark.net"]
    start_urls = ["https://www.cpubenchmark.net/cpu_list.php"]
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
        # "LOG_LEVEL": "DEBUG",
        "ITEM_PIPELINES": {
            "scrapy_passmark.pipelines.cpu_pipelines.CPUItemPipeline": 100,
        },
        "CLOSESPIDER_ERRORCOUNT": 1,
        "DOWNLOAD_TIMEOUT": 600,
        "DOWNLOAD_DELAY": 0.25,
    }

    def parse(self, response):
        cpu_table = response.css("table.cpulist")
        links = cpu_table.css("tr > td > a::attr(href)").getall()
        cpu_ids = [int(parse_qs(url)["id"][0]) for url in links]

        for cpu_id in cpu_ids:
            yield response.follow(
                url=f"https://www.cpubenchmark.net/cpu.php?id={cpu_id}",
                callback=self.parse_cpu,
                cb_kwargs={"cpu_id": cpu_id},
            )

    def parse_cpu(self, response, cpu_id):
        # Main CPU info
        cpu_item = CPUItem()
        cpu_item["id"] = cpu_id

        desc_body = response.css("div.desc > div.desc-body")
        cpu_item["name"] = (
            desc_body.css("div.desc-header > span.cpuname::text").get().strip()
        )

        main_desc = desc_body.css("div.left-desc-cpu, div.desc-foot")
        main_desc_p_tags = main_desc.css("p")
        for p_tag in main_desc_p_tags:
            text = [
                remove_tags(x.replace("<br>", "[BREAK]")).strip()
                for x in p_tag.get().split("<strong>")
            ]
            text = [x.strip() for x in text if x]

            label_mapping = {
                "Description:": "description",
                "Class:": "cpu_class",
                "Socket:": "socket",
                "Clockspeed:": "clock_speed",
                "Turbo Speed:": "turbo_speed",
                "Cores:": "cores",
                "Threads:": "threads",
                "Total Cores:": "total_cores",
                "Primary Cores:": "primary_cores",
                "Secondary Cores:": "secondary_cores",
                "Performance Cores:": "performance_cores",
                "Efficient Cores:": "efficient_cores",
                "Typical TDP:": "typical_tdp",
                "TDP Down:": "tdp_down",
                "TDP Up:": "tdp_up",
                "Cache per CPU Package:": "cache_per_cpu_package",
                "Cache per Eff. CPU Package:": "cache_per_effective_cpu_package",
                "Memory Support:": "memory_support",
                "Other names:": "other_names",
                "CPU First Seen on Charts:": "first_seen_on_charts",
                "CPUmark/$Price:": "cpu_mark_per_dollar_price",
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
                        cpu_item[field] = "; ".join(value_semi_cleaned).strip()
                        break

        main_ratings = response.css("div.desc > div.right-desc")
        ratings_texts = [
            x.strip().replace("*", "")
            for x in main_ratings.css("::text").getall()
            if x.strip() and x.strip() not in [":", "*"]
        ]
        for i, text in enumerate(ratings_texts):
            if text == "Multithread Rating":
                cpu_item["multi_thread_rating"] = ratings_texts[i + 1]
            elif text == "Single Thread Rating":
                cpu_item["single_thread_rating"] = ratings_texts[i + 1]
            elif text == "Samples:":
                cpu_item["num_samples"] = ratings_texts[i + 1]
            elif text == "Margin for error":
                cpu_item["margin_for_error"] = ratings_texts[i + 1]

        test_suite_table = response.css("table[id='test-suite-results']")
        rows = test_suite_table.css("tr")
        for row in rows:
            th = row.css("th::text").get().strip()
            td = row.css("td::text").get().strip()

            th_mapping = {
                "Integer Math": "integer_math",
                "Floating Point Math": "floating_point_math",
                "Find Prime Numbers": "find_prime_numbers",
                "Random String Sorting": "random_string_sorting",
                "Data Encryption": "data_encryption",
                "Data Compression": "data_compression",
                "Physics": "physics",
                "Extended Instructions": "extended_instructions",
            }

            if th in th_mapping:
                cpu_item[th_mapping[th]] = td

        gaming_score_table = response.css("table[id='gamescoreChart']")
        if gaming_score_table:
            cpu_item["relative_gaming_score"] = (
                gaming_score_table.css(
                    "td.value-cifre[style='background: #E2EDF4;']::text"
                )
                .get()
                .strip()
            )

        yield cpu_item

        # CPU mark distribution and pricing history
        script_tags = response.css("script")
        for script in script_tags:
            script_full = script.get()

            if "var chartLabel" in script_full and "dataArray.push" in script_full:
                matches = re.findall(
                    r"dataArray\.push\(\{x:\s*(\d+),\s*y:\s*([\d.]+)\}\)", script_full
                )
                price_data = [{"x": int(x), "y": float(y)} for x, y in matches]

                for data in price_data:
                    pricing_history_item = CPUPricingHistoryItem()
                    pricing_history_item["cpu_id"] = cpu_id
                    pricing_history_item["timestamp"] = data["x"]
                    pricing_history_item["price"] = data["y"]

                    yield pricing_history_item

            elif "var distributionData" in script_full:
                data_points_match = re.search(
                    r"dataPoints\s*:\s*\[(.*?)\]", script_full, re.DOTALL
                )
                if data_points_match:
                    raw_points = data_points_match.group(1)
                    points = re.findall(
                        r"\{\s*x\s*:\s*(\d+)\s*,\s*y\s*:\s*(\d+)\s*\}", raw_points
                    )
                    distribution_data = [{"x": int(x), "y": int(y)} for x, y in points]

                    for data in distribution_data:
                        distribution_item = CPUMarkDistributionItem()
                        distribution_item["cpu_id"] = cpu_id
                        distribution_item["cpu_mark"] = data["x"]
                        distribution_item["num_records"] = data["y"]

                        yield distribution_item
