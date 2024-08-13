"""
An async web crawler for site news.ycombinator.com
"""
import aiofiles
import aiohttp
import argparse
import asyncio
import logging

from aiofiles import os as aios
from aiofiles import ospath as aiospath
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
from hashlib import sha256
from pathlib import Path

ENCODING = 'utf-8'
SITE = 'https://news.ycombinator.com'


def make_safe_filename(text: str) -> str:
    """convert forbidden chars to underscores"""
    filename = ''.join(c if (c.isalnum() or c.isspace()) else '_' for c in text).strip('_ ')
    return filename + '.html'


async def write_file(fp: Path, content, mode='wb'):
    """ Writing content to file """
    if not await aiospath.exists(fp.parent):
        await aios.makedirs(fp.parent, exist_ok=True)
    async with aiofiles.open(fp, mode=mode) as f:
        await f.write(content)


class YCrawler:
    """ Web crawler """
    def __init__(self, sleep=60, top_news_count=30, max_tasks=10, root='web', run_once=True) -> None:
        self.site = SITE
        self.sleep = sleep
        self.top_news_count = top_news_count
        self.root = root
        self.run_once = run_once
        self.semaphore = asyncio.Semaphore(max_tasks)  # limit requests to the site

    async def read_url(self, url, session, proxy=None):
        """ Load web page from given url with concurrency control """
        response = content_type = status = None
        try:
            async with self.semaphore:
                async with session.get(url, proxy=proxy) as r:
                    response = await r.read()
                    content_type = r.content_type
                    status = r.status
            logging.debug('Read URL %s, status %s, received %s bytes', url, status, len(response))
        except TimeoutError:
            logging.error('Timed out connecting to %s', url)
            status = 408
        return response, content_type, status


    async def parse_comments(self, response, item, session):
        """ Parsing comments and downloading comment links """
        bs = BeautifulSoup(response, features="html.parser")
        st = bs.table.find(attrs={'class': 'comment-tree'})
        comment_items = st.find_all('tr', attrs={'class': 'athing comtr'}) if st else []
        if not comment_items:  
            return

        comm_index_dir = Path(self.root) / item['id'] / 'comments'

        for comment in comment_items:
            comm_id = comment['id']
            comm_text = comment.find(attrs={'class': 'commtext c00'})
            comm_urls = [a['href'] for a in comm_text.find_all('a') if a] if comm_text else []
            link_num = 0
            for comm_url in comm_urls:
                # download comment link
                response, content_type, status = await self.read_url(comm_url, session)
                if not response or status != 200:
                    logging.error('Error %s downloading comment link %s', status, comm_url)
                    continue
                
                if content_type == 'text/html':
                    fname = f'{comm_id}_{link_num}.html'
                else:
                    fname = f'{comm_id}_{link_num}_{Path(comm_url).name.split("?")[0]}'
                comment_file_path = comm_index_dir / fname
                await write_file(comment_file_path, response)
                link_num += 1


    async def parse_item(self, item, session):
        """Parse page and get comment links"""
        url = f'{self.site}/item?id={item['id']}'
        logging.info('Parsing item %s', url)
        response, _, status = await self.read_url(url, session)
        if not response:
            logging.info(f'No response from {url} status code - {status}')
            return

        a = item.find_all(attrs={'class': 'titleline'})[0].a
        name = a.text.strip()
        href = a['href']
        logging.info(f'News: {item['id']}\t{name}\t{href}')
        
        # write news item to file
        filepath = Path(self.root) / item['id'] / make_safe_filename(name)
        await write_file(filepath, response)
        
        # parse comments for news item
        await self.parse_comments(response, item, session)
        return name, url

    async def parse_main_page(self, session):
        """Parsing main news page"""
        response, _, status = await self.read_url(self.site, session)
        if not response or status != 200:
            logging.error('Error %s', status)
            return None
        bs = BeautifulSoup(response, features="html.parser")
        return bs.table.find_all(attrs={'class': 'athing'})

    async def run(self):
        """Main crawling algorithm"""
        logging.info("Starting web crawler at %s, check period %s seconds. Downloading %s top news. Run_once=%s",
            self.site, self.sleep, self.top_news_count, self.run_once)
        parsed_news = {}
        logging.info('Start crawling site: %s', self.site)
        while True:
            async with aiohttp.ClientSession(timeout=ClientTimeout(total=5), trust_env=True) as session:
                news = await self.parse_main_page(session)
                if not news:
                    logging.error('No news found')
                    break
                for news_item in news[:self.top_news_count]:
                    try:
                        if news_item['id'] not in parsed_news: 
                            name_url_tpl: tuple = await self.parse_item(news_item, session)
                            if not name_url_tpl:
                                continue
                            parsed_news[news_item['id']] = name_url_tpl
                        else:
                            logging.debug('News item %s already parsed', news_item['id'])
                    except KeyboardInterrupt:
                        break
                logging.info('End parsing news cycle')
            if self.run_once:
                break
            logging.info('Waiting for %s seconds...', self.sleep)
            await asyncio.sleep(self.sleep)
            logging.info('Repeat parsing news cycle')


def main():
    """
    Читает параметры и вызывает дальнейшие действия в программе
    @return:
    """
    parser = argparse.ArgumentParser(description='YCrawler')
    parser.add_argument("--count", "-c", dest="count", default=5, type=int, 
                        help='Count of top news to download')
    parser.add_argument("--period", "-p", dest="period", default=60, type=int, 
                        help='Period in seconds between checks')
    parser.add_argument("--root", "-r", dest="root", default='web', type=str, 
                        help='Folder to save downloaded pages')
    parser.add_argument("--log", "-l", dest="log", default=None, type=str,
                        help='Log file name')
    parser.add_argument("--once", "-o", action='store_true',
                        help='Run once')
    args = parser.parse_args()

    logging.basicConfig(
        filename=args.log, level=logging.INFO,
        format='[%(asctime)s] %(levelname).1s (%(filename)s:%(funcName)s) %(message)s',
        datefmt='%Y.%m.%d %H:%M:%S')

    ycrawler = YCrawler(sleep=args.period, top_news_count=args.count, root=args.root, run_once=args.once)
    try:
        print('Starting YCrawler app, press Ctrl+C to exit')
        #asyncio.run(ycrawler.run(), debug=True)
        asyncio.run(ycrawler.run())
    except (KeyboardInterrupt, asyncio.exceptions.CancelledError):
        logging.info("YCrawler was stopped by user")
    except:  # pylint: disable=bare-except
        logging.exception("Unexpected error")


if __name__ == '__main__':
    main()