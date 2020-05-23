import aiohttp
import asyncio
from urllib.parse import quote_plus as uriquote
from sys import stdout
from bs4 import BeautifulSoup as DOM
from dataclasses import dataclass
import csv
import itertools as it
from tqdm import tqdm


@dataclass
class Play(object):
    title: str
    themes: set
    period: set
    genres: set

    @classmethod
    def empty(cls):
        return cls('', {}, {}, {})

    @staticmethod
    def header():
        return ['title', 'author', 'themes', 'periods', 'places', 'genres']

    def record(self):
        return (self.title,
                self.author,
                ', '.join(self.themes),
                ', '.join(self.period),
                ', '.join(self.places),
                ', '.join(self.genres))

    async def retrieve(self, http, title, uri):
        self.title = title
        async with http.get(uri) as rsp:
            dom = DOM(await rsp.text(), 'lxml')
        tags = iter(dom.select('div.play-related-lists ul li'))
        try:
            self.author = dom.select('span.authorRole a')[0].text
        except IndexError:
            self.author = ''
        target_keys = {k: i for i, k in
                       enumerate(('theme', 'period', 'place', 'genre'))}
        targets = [set(()) for i in range(len(target_keys))]
        target = 0
        for tag in tags:
            t = tag.text.strip()
            if t.lower() in target_keys:
                target = target_keys[t.lower()]
                continue
            targets[target].add(t)
        self.themes, self.period, self.places, self.genres = targets
        return self


async def search(http, *filters):
    qry = '&'.join('='.join(map(uriquote, f)) for f in filters)
    uri = (f'https://www.dramaonlinelibrary.com/search?{qry}'
           f'&rows={2**31 - 1}')
    async with http.get(uri) as rsp:
        dom = DOM(await rsp.text(), 'lxml')
    return ((a.text.strip(), a['href'])
            for a in dom.select('div.search-article-text a[href]'))


async def main():
    steno = csv.writer(stdout, dialect='unix')
    steno.writerow(Play.header())

    filter_sets = (
            (('s2_type', 'monologue'),
             ('filter', 'and-wordcount-more-800'),
             ('filter', 'and-roles_male-more-0')),
            (('s2_type', 'play'),
             ('filter', 'and-wordcount-more-800'),
             ('filter', 'and-roles_male-equal-1'),
             ('filter', 'and-roles-equal-1')))

    async def get_play(http, title, uri, steno, bar):
        play = await Play.empty().retrieve(http, title, uri)
        steno.writerow(play.record())
        bar.update(1)

    async with aiohttp.ClientSession(raise_for_status=True) as http:
        tasks = asyncio.gather(*(search(http, *filters)
                                 for filters in filter_sets))
        plays = tuple(dict(it.chain(*(await tasks))).items())
        bar = tqdm(total=len(plays))
        tasks = [get_play(http, title, uri, steno, bar)
                 for title, uri in plays]
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
    stdout.flush()
