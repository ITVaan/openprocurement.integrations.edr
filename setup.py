from setuptools import setup, find_packages

version = '1.0.0'

requires = [
    'setuptools',
    'PyYAML',
    'chaussette',
    'gevent',
    'pyramid_exclog',
    'requests',
    'redis',
    'setuptools',
    'pyramid',
    'pytz',
    'simplejson'
]

test_requires = requires + [
    'webtest',
    'python-coveralls',
    'bottle'
]

entry_points = {
    'paste.app_factory': [
        'main = openprocurement.integrations.edr:main'
    ]
}

docs_requires = requires + [
    'sphinxcontrib-httpdomain',
]


setup(name='openprocurement.integrations.edr',
      version=version,
      description="openprocurement.integrations.edr",
      long_description=open("README.rst").read(),
      classifiers=[
        "Framework :: Pylons",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application"
        ],
      keywords="web services",
      author='Quintagroup, Ltd.',
      author_email='info@quintagroup.com',
      license='Apache License 2.0',
      url='https://github.com/Krokop/openprocurement.integrations.edr',
      packages=find_packages(exclude=['ez_setup']),
      namespace_packages=['openprocurement', 'openprocurement.integrations'],
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      tests_require=test_requires,
      extras_require={'test': test_requires,
                      'docs': docs_requires},
      entry_points=entry_points)
