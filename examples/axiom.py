#!python
# coding=utf-8
import threading
from collections import namedtuple

from thredds_crawler.crawl import Crawl
from thredds_iso_harvester.harvest import ThreddsIsoHarvester

SAVE_DIR = "/srv/iso"


job = namedtuple('HarvestJob', 'catalog_url out_dir select skip')

harvests = [
    # AOOS
    job(
        catalog_url="http://thredds.aoos.org/thredds/catalog.xml",
        out_dir=SAVE_DIR + '/aoos',
        select=None,
        skip=None
    ),
    # CeNCOOS
    job(
        catalog_url="http://thredds.cencoos.org/thredds/catalog.xml",
        out_dir=SAVE_DIR + '/cencoos',
        select=None,
        skip=None
    ),
    # SECOORA
    job(
        catalog_url="http://thredds.secoora.org/thredds/catalog.xml",
        out_dir=SAVE_DIR + '/secoora',
        select=None,
        skip=Crawl.SKIPS  + ["Grab Bag"]
    ),
    # UNIDATA
    job(
        catalog_url="http://thredds.ucar.edu/thredds/catalog.html",
        out_dir=SAVE_DIR + '/unidata',
        select=[".*Best.*"],
        skip=Crawl.SKIPS  + [
            ".*grib2", ".*grib1", ".*GrbF.*", ".*ncx2",
            "Radar Data", "Station Data",
            "Point Feature Collections", "Satellite Data",
            "Unidata NEXRAD Composites \(GINI\)",
            "Unidata case studies",
            ".*Reflectivity-[0-9]{8}"
        ]
    ),
    # NANOOS OSU
    job(
        catalog_url="http://ona.coas.oregonstate.edu:8080/thredds/catalog.xml",
        out_dir=SAVE_DIR + '/nanoos_osu',
        select=None,
        skip=None
    ),
    # OceanNOMADS
    job(
        catalog_url="https://ecowatch.ncddc.noaa.gov/thredds/oceanNomads/catalog_aggs.xml",
        out_dir=SAVE_DIR + '/oceannomads',
        select=[".*\_best"],
        skip=None
    )
]

threads = []
for h in harvests:
    t = threading.Thread(
        target=ThreddsIsoHarvester,
        name=h.catalog_url,
        kwargs=h._asdict()
    )
    threads.append(t)
    t.start()

for t in threads:
    t.join()
