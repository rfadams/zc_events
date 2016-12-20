import os

from setuptools import setup


def get_packages(package):
    """
    Return root package and all sub-packages.
    """
    return [dirpath
            for dirpath, dirnames, filenames in os.walk(package)
            if os.path.exists(os.path.join(dirpath, '__init__.py'))]


setup(
    name='zc_events',
    version='0.1.4',
    description="Shared code for ZeroCater microservices events",
    long_description='',
    keywords='zerocater python util',
    author='ZeroCater',
    author_email='tech@zerocater.com',
    url='https://github.com/ZeroCater/zc_events',
    download_url='https://github.com/ZeroCater/zc_events/tarball/0.1.4',
    license='MIT',
    packages=get_packages('zc_events'),
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Internet :: WWW/HTTP",
    ],
    install_requires=[
        'boto==2.43.0',
        'celery>=3.1.10,<4.0.0',
        'inflection>=0.3.1,<0.4',
        'pika>=0.10.0,<0.11.0',
        'pika_pool>=0.1.3,<0.1.4',
        'redis>=2.10.5,<2.11.0',
        'ujson>=1.35,<1.36',
        'zc_common>=0.3.0',
        'pyjwt>=1.4.0,<2.0.0',
    ]
)
