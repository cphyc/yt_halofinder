import os
from distutils.core import setup

package_name = "yt_halofinder"
version = '0.1'
README = os.path.join(os.path.dirname(__file__), 'README.md')
long_description = open(README).read()
setup(name=package_name,
      version=version,
      description=("Wrapper for HaloFinder"),
      long_description=long_description,
      classifiers=[
          "Programming Language :: Python",
          ("Topic :: Software Development :: Libraries :: Python Modules"),
      ],
      keywords='data',
      author='Corentin Cadiou <corentin.cadiou@iap.fr>',
      license='BSD',
      package_dir={package_name: package_name},
      packages=[package_name],
      install_requires=["yt"],
      author_email="corentin.cadiou@iap.fr",
)
