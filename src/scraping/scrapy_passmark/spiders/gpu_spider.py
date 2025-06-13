# standard library imports
import re
from urllib.parse import parse_qs

# third party imports
from scrapy.spiders import Spider
from w3lib.html import remove_tags

# local imports
from ..items.gpu_items import G3DMarkDistributionItem, GPUItem, GPUPricingHistoryItem


class GPUSpider(Spider):
    name = "gpu_spider"
    allowed_domains = ["videocardbenchmark.net"]
    start_urls = ["https://www.videocardbenchmark.net/gpu_list.php"]
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
            "scrapy_passmark.pipelines.gpu_pipelines.GPUItemPipeline": 100,
        },
        "CLOSESPIDER_ERRORCOUNT": 1,
        "DOWNLOAD_TIMEOUT": 600,
    }

    def parse(self, response):
        gpu_table = response.css("table.cpulist")
        links = gpu_table.css("tr > td > a::attr(href)").getall()
        gpu_ids = [int(parse_qs(url)["id"][0]) for url in links if "#price" not in url]

        for gpu_id in gpu_ids:
            yield response.follow(
                url=f"https://www.videocardbenchmark.net/gpu.php?id={gpu_id}",
                callback=self.parse_gpu,
                cb_kwargs={"gpu_id": gpu_id},
            )

    def parse_gpu(self, response, gpu_id):
        # Main GPU info
        gpu_item = GPUItem()
        gpu_item["id"] = gpu_id

        desc_body = response.css("div.desc > div.desc-body")
        gpu_item["name"] = (
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
                "Bus Interface:": "bus_interface",
                "Max Memory Size:": "max_memory_size",
                "Core Clock(s):": "core_clock",
                "Memory Clock(s):": "memory_clock",
                "DirectX:": "directx_version",
                "OpenGL:": "opengl_version",
                "Max TDP:": "max_tdp",
                "Videocard Category:": "category",
                "Other names:": "other_names",
                "Videocard First Benchmarked:": "first_benchmarked",
                "G3DMark/Price:": "g3d_mark_per_dollar_price",
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
                        gpu_item[field] = "; ".join(value_semi_cleaned).strip()
                        break

        main_ratings = response.css("div.desc > div.right-desc")
        ratings_texts = [
            x.strip().replace("*", "")
            for x in main_ratings.css("::text").getall()
            if x.strip() and x.strip() not in [":", "*"]
        ]
        for i, text in enumerate(ratings_texts):
            if text == "Average G3D Mark":
                gpu_item["g3d_mark"] = ratings_texts[i + 1]
            elif text == "Average G2D Mark:":
                gpu_item["g2d_mark"] = ratings_texts[i + 1]
            elif text == "Samples:":
                gpu_item["num_samples"] = ratings_texts[i + 1]

        test_suite_table = response.css("table[id='test-suite-results']")
        rows = test_suite_table.css("tr")
        for row in rows:
            th = row.css("th::text").get().strip()
            td = row.css("td::text").get().strip()

            th_mapping = {
                "DirectX 9": "directx_9",
                "DirectX 10": "directx_10",
                "DirectX 11": "directx_11",
                "DirectX 12": "directx_12",
                "GPU Compute": "gpu_compute",
            }

            if th in th_mapping:
                gpu_item[th_mapping[th]] = td

        yield gpu_item

        # G3D mark distribution and pricing history
        script_tags = response.css("script")
        for script in script_tags:
            script_full = script.get()

            if "var chartLabel" in script_full and "dataArray.push" in script_full:
                matches = re.findall(
                    r"dataArray\.push\(\{x:\s*(\d+),\s*y:\s*([\d.]+)\}\)", script_full
                )
                price_data = [{"x": int(x), "y": float(y)} for x, y in matches]

                for data in price_data:
                    pricing_history_item = GPUPricingHistoryItem()
                    pricing_history_item["gpu_id"] = gpu_id
                    pricing_history_item["timestamp"] = data["x"]
                    pricing_history_item["price"] = data["y"]

                    yield pricing_history_item

            elif "var distributionData" in script_full:
                script_full = script_full.replace(', color: "blue"', "")
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
                        distribution_item = G3DMarkDistributionItem()
                        distribution_item["gpu_id"] = gpu_id
                        distribution_item["g3d_mark"] = data["x"]
                        distribution_item["num_records"] = data["y"]

                        yield distribution_item
