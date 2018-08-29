import scrapy
import re


class YelpItem(scrapy.Item):
    business_name = scrapy.Field()
    industry_category = scrapy.Field()
    city = scrapy.Field()
    state = scrapy.Field()
    phone_number = scrapy.Field()
    street_address = scrapy.Field()
    website = scrapy.Field()
    email = scrapy.Field()
    url = scrapy.Field()
    count = scrapy.Field()


class GoogleApp (scrapy.Spider):

    name = "yelp_products"
    allowed_domains = ['www.yelp.com']
    start_urls = ['https://www.yelp.com/search?find_desc=Pets&find_loc=Las+Vegas,+NV']
    PAGE_URL = 'https://www.yelp.com/search?find_desc=Pets&find_loc=Las+Vegas,+NV&start={page_num}&cflt={category}'

    def start_requests(self):
        self.count = 0
        self.href_array = []

        yield scrapy.Request(url=self.start_urls[0], callback=self.parse)

    def parse(self, response):
        categories = response.xpath('//div[@class="filter-set category-filters"]/div[contains(@class, "more category-more")]'
                                    '/ul[@class="arrange_unit"]/li/label/input/@value').extract()
        categories.pop(0)
        for category in categories:
            yield scrapy.Request(url=self.PAGE_URL.format(page_num=0, category=category), callback=self._parse_categories)

    def _parse_categories(self, response):
        page_count = response.xpath('//div[@class="pagination-block"]//div[contains(@class, "page-of-pages")]/text()').extract()

        if page_count:
            page_count = int(re.search('of(.*)', page_count[0], re.DOTALL).group(1).strip())

        category = re.search('cflt=(.*)', response.url, re.DOTALL).group(1)

        count_per_page = response.xpath('//span[@class="pagination-results-window"]/text()').extract()
        count_per_page = int(re.search('-(.*?)of', count_per_page[0], re.DOTALL).group(1).strip())

        for i in range(page_count):
            yield scrapy.Request(url=self.PAGE_URL.format(page_num=i*count_per_page, category=category),
                                 callback=self._parse_links, dont_filter=True)

    def _parse_links(self, response):
        links = response.xpath('//h3[@class="search-result-title"]/span/a/@href').extract()
        href_links = []
        for link in links:
            href_links.append('https://www.yelp.com' + link)
        for link in href_links:
            if not self.href_array:
                self.href_array.append(link)
                yield scrapy.Request(url=link, callback=self._parse_data, dont_filter=True)
            else:
                flag = 0
                for href in self.href_array:
                    if link == href:
                        flag = 1
                        break
                if flag == 0:
                    self.href_array.append(link)
                    yield scrapy.Request(url=link, callback=self._parse_data, dont_filter=True)

    def _parse_data(self, response):
        self.count += 1
        if response.xpath('//span[contains(@class, "claim-status_icon--unclaimed")]'):
            item = YelpItem()
            item['business_name'] = response.xpath('//h1[contains(@class, "biz-page-title")]/text()')[0].extract().strip()
            item['industry_category'] = 'Pets'
            item['city'] = 'Las Vegas'
            item['state'] = 'Nevada'
            item['phone_number'] = response.xpath('//span[@itemprop="telephone"]/text()')[0].extract().strip()
            address = ''
            address_array = response.xpath('//strong[@class="street-address"]//text()').extract()
            for add in address_array:
                address += add + ' '
            item['street_address'] = address.strip()
            website_link = response.xpath('//span[@class="biz-website js-add-url-tagging"]/a/text()').extract()
            item['website'] = website_link[0] if website_link else ''
            item['url'] = response.url
            item['count'] = self.count
            yield item