# sci-paper-miner
Generate datasets of academic research papers (including full-text) based on [Core](https://core.ac.uk/services)[1] research paper text mines. 

## Setup
 - Run `pip install -r requirements.txt`
 - Obtain an api key [here](https://core.ac.uk/api-keys/register) for Core. 
## Usage
 - Set the target query in `crawl_core.py`. This currently can include the repository number (set to arXiv by default), range of years and topics (arXiv specific). 
 - Default is set for all CS papers from arXiv between 2006-2018
 - Run `python crawl_core.py <your-api-key>`

## Future
 - [ ] Extract citation information (currently not extracted for arXiv, contacted CORE and apparently citation extraction is limited today due to computational constraints but will be completed over the next few months)

[1] Knoth, P. and Zdrahal, Z. (2012) CORE: Three Access Levels to Underpin Open Access, D-Lib Magazine, 18, 11/12, Corporation for National Research Initiatives.
