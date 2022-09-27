import setuptools

setuptools.setup (
    name = 'renpy_analyzer',
    description = '',
    # version = VERSION,
    packages = setuptools.find_packages(),
    include_package_data=True,
    # install_requires = all_reqs,
    python_requires = '>=3',
    author = "Jay Kaiser",
    keyword = "data, transformation",
    # long_description = README,
    # long_description_content_type = "text/markdown",
    license = 'Apache 2.0',
    url = 'https://github.com/jayckaiser/renpy_analyzer',
    download_url = '',
    # dependency_links = all_reqs,
    author_email = 'jayckaiser@gmail.com',
    classifiers = [
    ]
)