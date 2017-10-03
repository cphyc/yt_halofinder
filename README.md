# Rationale
This script is intended to be used to generate halo catalog and merger
from python.

It is using AdaptaHop (also named HaloFinder, because why not) from Aubert, Pichon & Colombi 2004 to build the halo catalog as well as HaloMaker (?).

The source of HaloFinder can be found at https://gitlab.com/cphyc/HaloFinder on request.
The source of TreeMaker can be found at https://gitlab.com/cphyc/TreeMaker on request.


## Installation

First you need to get the sources
```
$ git clone https://github.com/cphyc/yt_halofinder.git
```
You then have to customize the `CONFIG` variable in `yt_halofinder/control.py` to match your installation. The paths should be absolute to your own `HaloFinder` and `TreeMaker`. The `BR` is for the `BIG_RUN` option of these two softwares, see their doc for information about it.

Once you're done, fire up a terminal, go within the `yt_halofinder` root directory and execute
```
pip install .
```
That's it! To get an example, see the Example section.

## Example
For a complete example, see the `Example.ipynb` file.
