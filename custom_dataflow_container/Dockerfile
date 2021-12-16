# https://cloud.google.com/dataflow/docs/guides/using-custom-containers#python


FROM apache/beam_python3.8_sdk:2.34.0

RUN apt-get update && apt-get install -y \
    autoconf \
    automake \
    gcc \ 
    libbz2-dev \
    libcurl4-openssl-dev \
    liblzma-dev \
    libssl-dev \
    make\
    perl \
    zlib1g-dev \
    python3-pysam

ADD /requirements.txt /requirements.txt

RUN pip install -r /requirements.txt
