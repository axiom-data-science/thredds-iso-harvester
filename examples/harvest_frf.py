# Harvest all the .ncml files in the FRF folder 
# on the USACE CHL THREDDS Server
#
# docker run --rm -v $(pwd)/harvest_frf.py:/srv/harvest.py -v $(pwd)/iso:/srv/iso axiom/thredds_iso_harvester

from thredds_iso_harvester.harvest import ThreddsIsoHarvester
from thredds_crawler.crawl import Crawl

#skip = Crawl.SKIPS + ['.*MATLAB.*']
skip = Crawl.SKIPS
select = ['.*\.ncml']

ThreddsIsoHarvester(catalog_url="http://chlthredds.erdc.dren.mil/thredds/catalog/frf/catalog.xml",
    skip=skip, select=select,
    out_dir="/srv/iso/frf")
