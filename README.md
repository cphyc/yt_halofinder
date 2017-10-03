This is a template extension package for yt.

To use this template, download an archive of the current state of it from:

https://bitbucket.org/yt_analysis/yt_extension_template/get/tip.zip

and initialize a repository.  You will need to update a few things:

 * Copyright information in COPYING.txt
 * Update setup.py, being sure to change:
    * `package_name`, changing the `extname` part to whatever your package is
      named
    * Your email address (in two places)
    * The URL to point to your repository
    * Version number
    * Description
 * Rename the `extname` part of the directory `yt_extname` to be the name of
   your package.

Once a package of the form `yt_extname` is installed, it will be importable as
`yt.extensions.extname`.
