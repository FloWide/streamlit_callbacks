import setuptools

setuptools.setup(
    name="streamlit_callbacks",
    version="0.0.1",
    author="",
    author_email="",
    description="",
    long_description="",
    long_description_content_type="text/plain",
    url="",
    packages=setuptools.find_namespace_packages(include=['streamlit.*']),
    include_package_data=True,
    setup_requires=['wheel'],
    classifiers=[],
    python_requires=">=3.6",
    install_requires=[
        "streamlit >= 0.76.0",
    ],
)

